"""
Joker Bingo — Flask Backend + Telegram Bot (merged, single process)
======================================================================
Running both Flask and the aiogram bot in one process means you only
need ONE free Render Web Service — no Background Worker required.

How it works:
  • Gunicorn starts with --workers 1 (critical — see Procfile)
  • At module load time, start_bot_thread() spawns a daemon thread
  • That thread owns its own asyncio event loop and runs the bot forever
  • Flask handles HTTP on the main thread pool as normal

Database: auto-detects from DATABASE_URL environment variable.
  • If DATABASE_URL starts with postgres → uses psycopg2 (PostgreSQL)
  • Otherwise → uses sqlite3 (local dev)

Environment variables:
  DATABASE_URL          — PostgreSQL connection string (Render sets this automatically)
  TELEGRAM_BOT_TOKEN    — from @BotFather
  ADMIN_TG_IDS          — comma-separated Telegram user IDs
  WELCOME_BONUS         — ETB given to new users (default: 100)
  INTERNAL_SECRET       — shared secret for /internal/notify endpoint
  PORT                  — HTTP port (default: 5000)

Run locally:   python app.py
Run on Render: gunicorn --bind 0.0.0.0:$PORT --workers 1 --threads 8 --timeout 120 app:app
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ workers=1 is REQUIRED
"""

import asyncio
import json
import hashlib
import hmac
import logging
import os
import random
import threading
import time
import uuid
from datetime import datetime

import aiohttp
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    WebAppInfo,
)
from flask import Flask, request, jsonify, g, send_from_directory
from flask_cors import CORS

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger('joker_bingo')

# ─── CONFIG ───────────────────────────────────────────────────────────────────
app = Flask(__name__, static_folder='static')
CORS(app, resources={r"/api/*": {"origins": "*"}})
app.config['COMPRESS_MIMETYPES'] = ['application/json', 'text/html', 'text/css', 'application/javascript']
app.config['COMPRESS_LEVEL'] = 6
app.config['COMPRESS_MIN_SIZE'] = 256
try:
    from flask_compress import Compress
    Compress(app)
except ImportError:
    pass  # flask-compress not installed — no gzip

BOT_TOKEN        = os.environ.get('TELEGRAM_BOT_TOKEN', os.environ.get('BOT_TOKEN', ''))
ADMIN_TG_IDS_STR = os.environ.get('ADMIN_TG_IDS', '')
ADMIN_TG_IDS     = set(ADMIN_TG_IDS_STR.split(',')) if ADMIN_TG_IDS_STR else set()
ADMIN_TG_IDS_INT = {int(x) for x in ADMIN_TG_IDS if x.strip().isdigit()}
WELCOME_BONUS       = int(os.environ.get('WELCOME_BONUS', 10))
INTERNAL_SECRET     = os.environ.get('INTERNAL_SECRET', 'changeme-please')
HOUSE_CUT           = 0.10
COUNTDOWN_SECS      = 30
CALL_INTERVAL       = 3
BET_AMOUNTS         = [10, 20, 50, 100]
REFERRAL_BONUS      = 20         # ETB per batch
REFERRALS_PER_BONUS = 10         # new users per batch
MIN_DEPOSIT      = 50
MAX_DEPOSIT      = 5_000
MIN_WITHDRAW     = 200
PAYMENT_ACCOUNTS = (
    "🏦 <b>CBE:</b>     <code>1000532982565</code>  (Tewodros Ephrem)\n"
    "📱 <b>Telebirr:</b> <code>0913252238</code>  (Tewodros Ephrem)"
)

# Owner = first admin (or set OWNER_TG_ID env var separately)
try:
    OWNER_TG_ID = int(os.environ.get('OWNER_TG_ID',
        str(min(ADMIN_TG_IDS_INT)) if ADMIN_TG_IDS_INT else '0'))
except (ValueError, TypeError):
    OWNER_TG_ID = 0

# Set at bot startup, used in referral links
_bot_username: str = ''

# ══════════════════════════════════════════════════════════════════════════════
#  DATABASE LAYER — auto-switches between SQLite (local) and PostgreSQL (prod)
# ══════════════════════════════════════════════════════════════════════════════
_DATABASE_URL = os.environ.get('DATABASE_URL', '')

# Render gives postgres:// URLs; psycopg2 requires postgresql://
if _DATABASE_URL.startswith('postgres://'):
    _DATABASE_URL = _DATABASE_URL.replace('postgres://', 'postgresql://', 1)

DB_TYPE   = 'postgres' if _DATABASE_URL.startswith('postgresql://') else 'sqlite'
SQLITE_DB = os.environ.get('DB_PATH', 'bingo.db')
PH        = '%s' if DB_TYPE == 'postgres' else '?'   # placeholder character

print(f'[DB] Using {"PostgreSQL" if DB_TYPE == "postgres" else "SQLite"} '
      f'({"prod" if DB_TYPE == "postgres" else "local dev"})')

# ── PostgreSQL driver imports ─────────────────────────────────────────────────
if DB_TYPE == 'postgres':
    import psycopg2
    import psycopg2.extras
    import psycopg2.pool
    import psycopg2.extensions
else:
    import sqlite3 as _sqlite3

# Pool is created lazily on first DB call — keeps Gunicorn from blocking at import
_pg_pool = None


_pg_pool_lock = __import__('threading').Lock()

def _ensure_pool():
    global _pg_pool
    if _pg_pool is not None:
        return
    with _pg_pool_lock:
        if _pg_pool is None:
            _pg_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=2, maxconn=12, dsn=_DATABASE_URL
            )

# ── Connection helpers ────────────────────────────────────────────────────────
def _new_pg_conn():
    """Create a brand-new PostgreSQL connection (bypasses pool)."""
    conn = psycopg2.connect(_DATABASE_URL)
    conn.autocommit = False
    return conn

def _conn_is_alive(conn) -> bool:
    """
    Check if a psycopg2 connection is usable without executing a query.
    Using STATUS_READY avoids starting a transaction, which would make
    the subsequent conn.autocommit = False raise ProgrammingError.
    """
    if conn.closed:
        return False
    # STATUS_READY (1) = idle, not in a transaction — safe to use
    return conn.status == psycopg2.extensions.STATUS_READY

def _get_conn():
    """Get a connection from the pool; replace it silently if stale."""
    if DB_TYPE == 'postgres':
        _ensure_pool()
        conn = _pg_pool.getconn()
        if not _conn_is_alive(conn):
            logger.warning('[DB] Stale connection — replacing.')
            try:
                _pg_pool.putconn(conn, close=True)
            except Exception:
                pass
            conn = _new_pg_conn()
        else:
            # Only set autocommit when connection is STATUS_READY (no active tx)
            conn.autocommit = False
        return conn
    else:
        conn = _sqlite3.connect(SQLITE_DB, check_same_thread=False)
        conn.row_factory = _sqlite3.Row
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA foreign_keys=ON')
        return conn

def _release_conn(conn, close=False):
    """Return a connection to the pool (or close it if it is broken)."""
    if DB_TYPE == 'postgres':
        try:
            _pg_pool.putconn(conn, close=close)
        except Exception:
            pass
    else:
        conn.close()

# ── Flask per-request connection ──────────────────────────────────────────────
def get_db():
    """Return the per-request DB connection, stored in Flask g."""
    if '_db' not in g:
        g._db = _get_conn()
    return g._db

@app.teardown_appcontext
def close_db(exc):
    conn = g.pop('_db', None)
    if conn is None:
        return
    if DB_TYPE == 'postgres':
        broken = getattr(conn, 'closed', False)
        try:
            if exc:
                conn.rollback()
            else:
                conn.commit()
        except Exception:
            broken = True
        _release_conn(conn, close=broken)
    else:
        conn.close()

def _cursor(conn):
    """Return a dict-aware cursor for the current DB type."""
    if DB_TYPE == 'postgres':
        return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    else:
        conn.row_factory = _sqlite3.Row
        return conn.cursor()

# ── Public query helpers ──────────────────────────────────────────────────────
def _pg_sql(sql: str) -> str:
    """Convert SQLite-style ? placeholders to psycopg2 %s."""
    return sql.replace('?', '%s') if DB_TYPE == 'postgres' else sql

def query_db(sql, args=(), one=False):
    """SELECT → list of dicts (or one dict when one=True)."""
    conn = get_db()
    cur  = _cursor(conn)
    cur.execute(_pg_sql(sql), args)
    rows = [dict(r) for r in cur.fetchall()]
    return (rows[0] if rows else None) if one else rows

def execute_db(sql, args=()):
    """INSERT / UPDATE / DELETE. Returns inserted row id for INSERTs."""
    sql = _pg_sql(sql)
    if DB_TYPE == 'postgres' and sql.strip().upper().startswith('INSERT')             and 'RETURNING' not in sql.upper():
        sql = sql + ' RETURNING id'
    conn = get_db()
    cur  = _cursor(conn)
    cur.execute(sql, args)
    conn.commit()
    if DB_TYPE == 'postgres' and sql.strip().upper().startswith('INSERT'):
        row = cur.fetchone()
        return dict(row)['id'] if row else None
    return cur.lastrowid if DB_TYPE != 'postgres' else None

def execute_many(statements):
    """Run multiple statements in one transaction."""
    conn = get_db()
    cur  = _cursor(conn)
    for sql, args in statements:
        cur.execute(_pg_sql(sql) if DB_TYPE == 'postgres' else sql, args)
    conn.commit()
    return cur

# ── Standalone connection — GameEngine thread (no Flask context) ───────────────
class StandaloneDB:
    """
    Context manager for DB operations that run outside Flask request context
    (e.g. the GameEngine background thread).
    Opens its own connection; does NOT use Flask g or the request-scoped pool.
    """
    def __init__(self):
        self.conn = None

    def __enter__(self):
        self.conn = _get_conn()
        return self

    def __exit__(self, exc_type, *_):
        if self.conn is None:
            return
        broken = getattr(self.conn, 'closed', False)
        try:
            if exc_type:
                self.conn.rollback()
            else:
                self.conn.commit()
        except Exception:
            broken = True
        _release_conn(self.conn, close=broken)
        self.conn = None

    def query(self, sql, args=()):
        cur = _cursor(self.conn)
        cur.execute(_pg_sql(sql), args)
        return [dict(r) for r in cur.fetchall()]

    def one(self, sql, args=()):
        rows = self.query(sql, args)
        return rows[0] if rows else None

    def run(self, sql, args=()):
        sql = _pg_sql(sql)
        if DB_TYPE == 'postgres' and sql.strip().upper().startswith('INSERT')                 and 'RETURNING' not in sql.upper():
            sql = sql + ' RETURNING id'
        cur = _cursor(self.conn)
        cur.execute(sql, args)
        if DB_TYPE == 'postgres' and sql.strip().upper().startswith('INSERT'):
            row = cur.fetchone()
            return dict(row)['id'] if row else None
        return None

# ── Schema init ───────────────────────────────────────────────────────────────
def init_db():
    """Create tables if they don't exist. Runs once at startup."""
    if DB_TYPE == 'postgres':
        ddl = """
            CREATE TABLE IF NOT EXISTS users (
                id              SERIAL PRIMARY KEY,
                tg_id           TEXT    UNIQUE NOT NULL,
                username        TEXT    UNIQUE NOT NULL,
                balance         INTEGER NOT NULL DEFAULT 10,
                wins            INTEGER NOT NULL DEFAULT 0,
                games_played    INTEGER NOT NULL DEFAULT 0,
                total_won       INTEGER NOT NULL DEFAULT 0,
                total_bet       INTEGER NOT NULL DEFAULT 0,
                is_admin        INTEGER NOT NULL DEFAULT 0,
                is_banned       INTEGER NOT NULL DEFAULT 0,
                referral_count  INTEGER NOT NULL DEFAULT 0,
                referred_by     INTEGER,
                deposit_count   INTEGER NOT NULL DEFAULT 0,
                phone           TEXT,
                created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS bingo_games (
                id                   TEXT    PRIMARY KEY,
                bet_amount           INTEGER NOT NULL,
                status               TEXT    NOT NULL DEFAULT 'waiting',
                called_numbers       TEXT    NOT NULL DEFAULT '[]',
                number_pool          TEXT    NOT NULL DEFAULT '[]',
                countdown_started_at DOUBLE PRECISION,
                started_at           DOUBLE PRECISION,
                finished_at          DOUBLE PRECISION,
                winner_id            INTEGER REFERENCES users(id),
                created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS bingo_players (
                id            SERIAL PRIMARY KEY,
                game_id       TEXT    NOT NULL REFERENCES bingo_games(id),
                user_id       INTEGER NOT NULL REFERENCES users(id),
                picked_number INTEGER,
                picked_number2 INTEGER,
                card          TEXT,
                card2         TEXT,
                marked_cells  TEXT    NOT NULL DEFAULT '[[2,2]]',
                marked2       TEXT    NOT NULL DEFAULT '[[2,2]]',
                bet_paid      INTEGER NOT NULL DEFAULT 1,
                has_bingo     INTEGER NOT NULL DEFAULT 0,
                kicked        INTEGER NOT NULL DEFAULT 0,
                joined_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(game_id, user_id)
            );
            CREATE TABLE IF NOT EXISTS game_history (
                id          SERIAL PRIMARY KEY,
                user_id     INTEGER NOT NULL REFERENCES users(id),
                game_id     TEXT    NOT NULL,
                bet_amount  INTEGER,
                prize_won   INTEGER NOT NULL DEFAULT 0,
                won         INTEGER NOT NULL DEFAULT 0,
                result      TEXT    NOT NULL DEFAULT 'loss',
                played_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS transactions (
                id          SERIAL PRIMARY KEY,
                tg_id       TEXT    NOT NULL,
                type        TEXT    NOT NULL,
                amount      INTEGER NOT NULL,
                ref         TEXT    UNIQUE NOT NULL,
                note        TEXT,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_tx_tgid ON transactions(tg_id);
            CREATE INDEX IF NOT EXISTS idx_users_tg_id      ON users(tg_id);
            CREATE INDEX IF NOT EXISTS idx_games_status     ON bingo_games(status);
            CREATE INDEX IF NOT EXISTS idx_games_bet_status ON bingo_games(bet_amount, status);
            CREATE INDEX IF NOT EXISTS idx_players_game     ON bingo_players(game_id);
            CREATE INDEX IF NOT EXISTS idx_players_user     ON bingo_players(user_id);
            CREATE INDEX IF NOT EXISTS idx_history_user     ON game_history(user_id);
        """
        conn = _get_conn()
        try:
            cur = conn.cursor()
            cur.execute(ddl)
            conn.commit()
            print('[DB] PostgreSQL schema ready.')
            # Migrate existing DBs — add columns if missing
            migrations = [
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS referral_count INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS referred_by INTEGER",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS deposit_count INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS phone TEXT",
                "ALTER TABLE bingo_players ADD COLUMN IF NOT EXISTS card2 TEXT",
                "ALTER TABLE bingo_players ADD COLUMN IF NOT EXISTS marked2 TEXT NOT NULL DEFAULT '[[2,2]]'",
                "ALTER TABLE bingo_players ADD COLUMN IF NOT EXISTS bet_paid INTEGER NOT NULL DEFAULT 1",
                "ALTER TABLE bingo_players ADD COLUMN IF NOT EXISTS picked_number2 INTEGER",
                "ALTER TABLE bingo_players ADD COLUMN IF NOT EXISTS kicked INTEGER NOT NULL DEFAULT 0",
                "CREATE INDEX IF NOT EXISTS idx_games_bet_status ON bingo_games(bet_amount, status)",
                "CREATE INDEX IF NOT EXISTS idx_players_user ON bingo_players(user_id)",
                """CREATE TABLE IF NOT EXISTS transactions (
                    id SERIAL PRIMARY KEY, tg_id TEXT NOT NULL,
                    type TEXT NOT NULL, amount INTEGER NOT NULL,
                    ref TEXT UNIQUE NOT NULL, note TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )""",
                "CREATE INDEX IF NOT EXISTS idx_tx_tgid ON transactions(tg_id)",
                "ALTER TABLE game_history ADD COLUMN IF NOT EXISTS result TEXT NOT NULL DEFAULT 'loss'",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS is_banned INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE bingo_players ADD COLUMN IF NOT EXISTS picked_number2 INTEGER",
            ]
            conn2 = _get_conn()
            try:
                cur2 = conn2.cursor()
                for m in migrations:
                    try: cur2.execute(m)
                    except Exception: pass
                conn2.commit()
            finally:
                _release_conn(conn2)
        finally:
            _release_conn(conn)
    else:
        import sqlite3
        with sqlite3.connect(SQLITE_DB) as conn:
            conn.execute('PRAGMA journal_mode=WAL')
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS users (
                    id             INTEGER PRIMARY KEY AUTOINCREMENT,
                    tg_id          TEXT    UNIQUE NOT NULL,
                    username       TEXT    UNIQUE NOT NULL,
                    balance        INTEGER DEFAULT 10,
                    wins           INTEGER DEFAULT 0,
                    games_played   INTEGER DEFAULT 0,
                    total_won      INTEGER DEFAULT 0,
                    total_bet      INTEGER DEFAULT 0,
                    is_admin       INTEGER DEFAULT 0,
                    referral_count INTEGER DEFAULT 0,
                    referred_by    INTEGER,
                    deposit_count  INTEGER DEFAULT 0,
                    phone          TEXT,
                    created_at     TEXT DEFAULT (datetime('now'))
                );
                CREATE TABLE IF NOT EXISTS bingo_games (
                    id                   TEXT PRIMARY KEY,
                    bet_amount           INTEGER NOT NULL,
                    status               TEXT    DEFAULT 'waiting',
                    called_numbers       TEXT    DEFAULT '[]',
                    number_pool          TEXT    DEFAULT '[]',
                    countdown_started_at REAL,
                    started_at           REAL,
                    finished_at          REAL,
                    winner_id            INTEGER,
                    created_at           TEXT DEFAULT (datetime('now')),
                    FOREIGN KEY (winner_id) REFERENCES users(id)
                );
                CREATE TABLE IF NOT EXISTS bingo_players (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    game_id       TEXT    NOT NULL,
                    user_id       INTEGER NOT NULL,
                    picked_number INTEGER,
                picked_number2 INTEGER,
                    card          TEXT,
                    card2         TEXT,
                    marked_cells  TEXT    DEFAULT '[[2,2]]',
                    marked2       TEXT    DEFAULT '[[2,2]]',
                    bet_paid      INTEGER DEFAULT 1,
                    has_bingo     INTEGER DEFAULT 0,
                    kicked        INTEGER DEFAULT 0,
                    joined_at     TEXT    DEFAULT (datetime('now')),
                    UNIQUE(game_id, user_id),
                    FOREIGN KEY (game_id) REFERENCES bingo_games(id),
                    FOREIGN KEY (user_id) REFERENCES users(id)
                );
                CREATE TABLE IF NOT EXISTS game_history (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id    INTEGER NOT NULL,
                    game_id    TEXT    NOT NULL,
                    bet_amount INTEGER,
                    prize_won  INTEGER DEFAULT 0,
                    won        INTEGER DEFAULT 0,
                    played_at  TEXT    DEFAULT (datetime('now')),
                    FOREIGN KEY (user_id) REFERENCES users(id)
                );
                CREATE INDEX IF NOT EXISTS idx_users_tg_id  ON users(tg_id);
                CREATE INDEX IF NOT EXISTS idx_games_status ON bingo_games(status);
                CREATE INDEX IF NOT EXISTS idx_players_game ON bingo_players(game_id);
                CREATE INDEX IF NOT EXISTS idx_history_user ON game_history(user_id);
            """)
            conn.commit()
            print('[DB] SQLite schema ready.')

# ══════════════════════════════════════════════════════════════════════════════
#  AUTH HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def verify_telegram_init_data(init_data: str):
    try:
        parts = {}
        for seg in init_data.split('&'):
            if '=' in seg:
                k, v = seg.split('=', 1)
                parts[k] = v
        recv_hash = parts.pop('hash', '')
        data_str  = '\n'.join(f'{k}={v}' for k, v in sorted(parts.items()))
        secret    = hmac.new(b'WebAppData', BOT_TOKEN.encode(), hashlib.sha256).digest()
        calc_hash = hmac.new(secret, data_str.encode(), hashlib.sha256).hexdigest()
        if hmac.compare_digest(calc_hash, recv_hash):
            return json.loads(parts.get('user', '{}'))
    except Exception:
        pass
    return None

def get_tg_user_from_request():
    init_data = request.headers.get('X-Telegram-Init-Data', '')
    if init_data and BOT_TOKEN:
        user = verify_telegram_init_data(init_data)
        if user:
            return user
    dev_hdr = request.headers.get('X-Tg-User', '')
    if dev_hdr:
        try:
            return json.loads(dev_hdr)
        except Exception:
            pass
    return None

def get_current_user():
    tg = get_tg_user_from_request()
    if not tg:
        return None, None
    tg_id = str(tg.get('id', ''))
    if not tg_id:
        return None, None
    row = query_db('SELECT * FROM users WHERE tg_id=?', (tg_id,), one=True)
    return row, tg

def require_auth():
    user, tg = get_current_user()
    if not user:
        return None, None, (jsonify({'error': 'Unauthorized'}), 401)
    if user.get('is_banned'):
        return None, None, (jsonify({'error': 'Account banned'}), 403)
    return user, tg, None

def require_admin():
    user, tg, err = require_auth()
    if err:
        return None, err
    if not user.get('is_admin'):
        return None, (jsonify({'error': 'Admin access required'}), 403)
    return user, None

# ══════════════════════════════════════════════════════════════════════════════
#  BINGO LOGIC
# ══════════════════════════════════════════════════════════════════════════════
def generate_bingo_card(seed: int) -> list:
    """Standard BINGO card: B=1-15, I=16-30, N=31-45, G=46-60, O=61-75. 5 per col, FREE centre."""
    rng    = random.Random(seed)  # purely deterministic — same number → same card always
    ranges = [(1,15),(16,30),(31,45),(46,60),(61,75)]
    cols   = []
    for lo, hi in ranges:
        pool = list(range(lo, hi + 1))
        rng.shuffle(pool)
        cols.append(pool[:5])
    card = [[cols[c][r] for c in range(5)] for r in range(5)]
    card[2][2] = 'FREE'
    return card

BINGO_COLS = {0:'B',1:'I',2:'N',3:'G',4:'O'}
COL_RANGES = {0:(1,15),1:(16,30),2:(31,45),3:(46,60),4:(61,75)}

def _calc_pot(gid: str) -> int:
    row = query_db('SELECT COALESCE(SUM(bet_paid),0) AS s FROM bingo_players WHERE game_id=?', (gid,), one=True)
    return int(row['s']) if row else 0

def check_bingo(card: list, marked: list, called: list) -> bool:
    """
    True only if the marked cells form a line AND every non-FREE marked cell
    has its number in the correct BINGO column AND that number was called.
    Rules: B=col0 (1-15), I=col1 (16-30), N=col2 (31-45), G=col3 (46-60), O=col4 (61-75)
    """
    called_set = set(called)
    s = set()
    for r, c in marked:
        val = card[r][c]
        if val == 'FREE':
            s.add((r, c))
            continue
        lo, hi = COL_RANGES[c]
        if val not in called_set:
            return False          # number not yet called
        if not (lo <= val <= hi):
            return False          # wrong column
        s.add((r, c))
    # Check lines
    for r in range(5):
        if all((r, c) in s for c in range(5)): return True
    for c in range(5):
        if all((r, c) in s for r in range(5)): return True
    if all((i, i) in s for i in range(5)):     return True
    if all((i, 4-i) in s for i in range(5)):   return True
    if all((r,c) in s for r,c in [(0,0),(0,4),(4,0),(4,4)]): return True  # 4 corners
    return False

# ══════════════════════════════════════════════════════════════════════════════
#  GAME ENGINE  (background thread — no Flask context)
# ══════════════════════════════════════════════════════════════════════════════
_db_lock = threading.Lock()

class GameEngine:
    def __init__(self):
        t = threading.Thread(target=self._loop, daemon=True, name='GameEngine')
        t.start()

    def _loop(self):
        while True:
            try:
                self._tick()
            except Exception as exc:
                print(f'[Engine] ERROR: {exc}')
            time.sleep(1)

    def _tick(self):
        now = time.time()
        with _db_lock, StandaloneDB() as db:
            games = db.query(
                "SELECT * FROM bingo_games WHERE status IN ('waiting','countdown','active')"
            )
            for g in games:
                gid    = g['id']
                status = g['status']
                pc_row = db.one(
                    'SELECT COUNT(*) AS c FROM bingo_players WHERE game_id=? AND kicked=0', (gid,)
                )
                pcount = pc_row['c'] if pc_row else 0

                # waiting → countdown
                if status == 'waiting' and pcount >= 2:
                    db.run(
                        "UPDATE bingo_games SET status='countdown', countdown_started_at=? WHERE id=?",
                        (now, gid)
                    )
                    print(f'[Engine] {gid}: COUNTDOWN ({pcount} players)')

                # countdown — resets if players drop below 2
                elif status == 'countdown':
                    if pcount < 2:
                        db.run(
                            "UPDATE bingo_games SET status='waiting', countdown_started_at=NULL WHERE id=?",
                            (gid,)
                        )
                    elif g['countdown_started_at'] and (now - g['countdown_started_at']) >= COUNTDOWN_SECS:
                        # Build pool 1-75 as "LETTER-NUMBER" strings
                        pool = []
                        for num in range(1, 76):
                            if   num <= 15: col = 'B'
                            elif num <= 30: col = 'I'
                            elif num <= 45: col = 'N'
                            elif num <= 60: col = 'G'
                            else:           col = 'O'
                            pool.append(f'{col}-{num}')
                        random.shuffle(pool)
                        db.run(
                            "UPDATE bingo_games SET status='active', started_at=?, number_pool=? WHERE id=?",
                            (now, json.dumps(pool), gid)
                        )
                        print(f'[Engine] {gid}: ACTIVE')

                # active — call next number every CALL_INTERVAL seconds
                elif status == 'active':
                    called = json.loads(g['called_numbers'] or '[]')
                    pool   = json.loads(g['number_pool']   or '[]')
                    if not pool:
                        remaining = db.query(
                            'SELECT * FROM bingo_players WHERE game_id=? AND kicked=0', (gid,)
                        )
                        if len(remaining) == 1:
                            sole  = remaining[0]
                            units = db.query('SELECT COALESCE(SUM(bet_paid),0) AS s FROM bingo_players WHERE game_id=?',(gid,),one=True)['s'] or 0
                            prize = int(int(units) * g['bet_amount'] * (1 - HOUSE_CUT))
                            db.run('UPDATE users SET balance=balance+?, wins=wins+1, games_played=games_played+1 WHERE id=?',
                                   (prize, sole['user_id']))
                        elif len(remaining) > 1:
                            for p in remaining:
                                db.run('UPDATE users SET balance=balance+?, games_played=games_played+1 WHERE id=?',
                                       (g['bet_amount'], p['user_id']))
                        db.run("UPDATE bingo_games SET status='finished', finished_at=? WHERE id=?", (now, gid))
                        print(f'[Engine] {gid}: FINISHED (pool exhausted)')
                        continue

                    started  = g['started_at'] or now
                    expected = int((now - started) // CALL_INTERVAL) + 1
                    while len(called) < expected and pool:
                        nxt = pool.pop(0)
                        called.append(nxt)
                        print(f'[Engine] {gid}: called {nxt} ({len(called)}/75)')

                    db.run(
                        'UPDATE bingo_games SET called_numbers=?, number_pool=? WHERE id=?',
                        (json.dumps(called), json.dumps(pool), gid)
                    )

# ══════════════════════════════════════════════════════════════════════════════
#  API ROUTES
# ══════════════════════════════════════════════════════════════════════════════

@app.route('/')
def serve_index():
    resp = send_from_directory('static', 'index.html')
    resp.headers['Cache-Control'] = 'no-cache'
    return resp

@app.route('/api/debug-auth', methods=['POST'])
def debug_auth():
    """Temporary debug endpoint — shows exactly what tg_id the server sees."""
    body    = request.json or {}
    tg_user = body.get('tg_user') or get_tg_user_from_request()
    tg_id   = str(tg_user.get('id', '')) if tg_user else 'NO_USER'
    row     = query_db('SELECT id, username, tg_id FROM users WHERE tg_id=?', (tg_id,), one=True)
    all_ids = [u['tg_id'] for u in query_db('SELECT tg_id FROM users ORDER BY id DESC LIMIT 5')]
    return jsonify({
        'seen_tg_id': tg_id,
        'found_user': row['username'] if row else None,
        'last_5_tg_ids': all_ids,
        'tg_user_raw': tg_user,
    })

@app.route('/api/auth', methods=['POST'])
def auth():
    body    = request.json or {}
    tg_user = body.get('tg_user') or get_tg_user_from_request()
    if not tg_user:
        return jsonify({'error': 'No Telegram user data'}), 400
    tg_id = str(tg_user.get('id', ''))
    if not tg_id:
        return jsonify({'error': 'Missing user id'}), 400
    row = query_db('SELECT * FROM users WHERE tg_id=?', (tg_id,), one=True)
    if row:
        return jsonify({'status': 'existing', 'user': row})
    return jsonify({
        'status': 'new',
        'suggested_username': tg_user.get('username') or tg_user.get('first_name', 'Player'),
        'tg_id': tg_id
    })

@app.route('/api/register', methods=['POST'])
def register():
    body        = request.json or {}
    tg_id       = str(body.get('tg_id', '')).strip()
    username    = body.get('username', '').strip()
    phone       = str(body.get('phone', '')).strip()
    referred_by = body.get('referred_by')   # tg_id of referrer
    if not tg_id or not username:
        return jsonify({'error': 'tg_id and username are required'}), 400
    if len(username) < 2 or len(username) > 20:
        return jsonify({'error': 'Username must be 2-20 characters'}), 400
    if query_db('SELECT id FROM users WHERE username=?', (username,), one=True):
        return jsonify({'error': 'Username already taken'}), 409
    if query_db('SELECT id FROM users WHERE tg_id=?', (tg_id,), one=True):
        return jsonify({'error': 'Telegram account already registered'}), 409
    is_admin = 1 if tg_id in ADMIN_TG_IDS else 0
    ref_user = None
    if referred_by:
        ref_user = query_db('SELECT * FROM users WHERE tg_id=?', (str(referred_by),), one=True)
    ref_id = ref_user['id'] if ref_user else None
    execute_db(
        'INSERT INTO users (tg_id, username, balance, is_admin, referred_by, phone) VALUES (?,?,?,?,?,?)',
        (tg_id, username, WELCOME_BONUS, is_admin, ref_id, phone or None)
    )
    user = query_db('SELECT * FROM users WHERE tg_id=?', (tg_id,), one=True)
    # Credit referrer: +5 ETB per 10 successful referrals
    if ref_user:
        execute_db(
            'UPDATE users SET referral_count=referral_count+1 WHERE id=?', (ref_user['id'],)
        )
        updated_ref = query_db('SELECT referral_count FROM users WHERE id=?', (ref_user['id'],), one=True)
        rc = updated_ref['referral_count']
        if rc % REFERRALS_PER_BONUS == 0:
            execute_db(
                'UPDATE users SET balance=balance+? WHERE id=?', (REFERRAL_BONUS, ref_user['id'])
            )
    return jsonify({'status': 'registered', 'user': user}), 201

@app.route('/api/user', methods=['GET'])
def get_user():
    user, _, err = require_auth()
    if err: return err
    fresh = query_db('SELECT * FROM users WHERE id=?', (user['id'],), one=True)
    d = dict(fresh)
    try:
        d['is_owner'] = (int(d.get('tg_id', 0)) == OWNER_TG_ID)
    except (ValueError, TypeError):
        d['is_owner'] = False
    return jsonify(d)

@app.route('/api/referral-stats', methods=['GET'])
def referral_stats():
    user, _, err = require_auth()
    if err: return err
    fresh = query_db('SELECT referral_count FROM users WHERE id=?', (user['id'],), one=True)
    rc    = fresh['referral_count'] if fresh else 0
    friends = query_db(
        'SELECT username, created_at FROM users WHERE referred_by=? ORDER BY created_at DESC',
        (user['id'],)
    )
    return jsonify({
        'referral_count': rc,
        'next_bonus_at':  ((rc // REFERRALS_PER_BONUS) + 1) * REFERRALS_PER_BONUS,
        'friends':        friends,
        'bonus_amount':   REFERRAL_BONUS,
        'per_bonus':      REFERRALS_PER_BONUS,
    })

@app.route('/api/games', methods=['GET'])
def list_games():
    bet    = request.args.get('bet', type=int)
    sql    = """
        SELECT g.*, COUNT(p.id) as player_count
        FROM bingo_games g
        LEFT JOIN bingo_players p ON g.id=p.game_id
        WHERE g.status IN ('waiting','countdown','active')
    """
    params = []
    if bet:
        sql += ' AND g.bet_amount=?'
        params.append(bet)
    sql += ' GROUP BY g.id ORDER BY g.created_at DESC'
    games = query_db(sql, params)
    if not games:
        return jsonify([])
    now = time.time()
    # Batch-fetch all players for all active games in ONE query (eliminates N+1)
    gids = [g['id'] for g in games]
    ph_list = ','.join(['?' ] * len(gids))
    all_bp = query_db(
        f'SELECT bp.game_id, u.username, bp.picked_number, bp.bet_paid '
        f'FROM bingo_players bp JOIN users u ON bp.user_id=u.id '
        f'WHERE bp.game_id IN ({ph_list})', gids
    )
    bp_by_game: dict = {}
    for p in all_bp:
        bp_by_game.setdefault(p['game_id'], []).append(p)
    for gm in games:
        players   = bp_by_game.get(gm['id'], [])
        pot_units = sum(p.get('bet_paid', 1) for p in players)
        gm['pot']          = pot_units * gm['bet_amount']
        gm['prize']        = int(gm['pot'] * (1 - HOUSE_CUT))
        gm['player_count'] = len(players)
        gm['players']      = [{'username': p['username'], 'picked_number': p['picked_number']} for p in players]
        csat = gm.get('countdown_started_at')
        gm['countdown_remaining'] = (
            max(0, COUNTDOWN_SECS - int(now - csat))
            if gm['status'] == 'countdown' and csat else COUNTDOWN_SECS
        )
    return jsonify(games)

@app.route('/api/lobby', methods=['GET'])
def get_lobby():
    """Single endpoint returns user + all active games — replaces 3 round-trips with 1."""
    user, _, err = require_auth()
    if err: return err
    # Fresh user
    fresh = query_db('SELECT * FROM users WHERE id=?', (user['id'],), one=True)
    d = dict(fresh)
    try:
        d['is_owner'] = (int(d.get('tg_id', 0)) == OWNER_TG_ID)
    except (ValueError, TypeError):
        d['is_owner'] = False
    # All active games
    games = query_db("""
        SELECT g.*, COUNT(p.id) as player_count
        FROM bingo_games g
        LEFT JOIN bingo_players p ON g.id=p.game_id
        WHERE g.status IN ('waiting','countdown','active')
        GROUP BY g.id ORDER BY g.created_at DESC
    """)
    if games:
        now = time.time()
        gids = [g['id'] for g in games]
        ph_list = ','.join(['?'] * len(gids))
        all_bp = query_db(
            f'SELECT bp.game_id, u.username, bp.picked_number, bp.bet_paid '
            f'FROM bingo_players bp JOIN users u ON bp.user_id=u.id '
            f'WHERE bp.game_id IN ({ph_list})', gids
        )
        bp_by_game: dict = {}
        for p in all_bp:
            bp_by_game.setdefault(p['game_id'], []).append(p)
        for gm in games:
            players   = bp_by_game.get(gm['id'], [])
            pot_units = sum(p.get('bet_paid', 1) for p in players)
            gm['pot']          = pot_units * gm['bet_amount']
            gm['prize']        = int(gm['pot'] * (1 - HOUSE_CUT))
            gm['player_count'] = len(players)
            gm['players']      = [{'username': p['username'], 'picked_number': p['picked_number']} for p in players]
            csat = gm.get('countdown_started_at')
            gm['countdown_remaining'] = (
                max(0, COUNTDOWN_SECS - int(now - csat))
                if gm['status'] == 'countdown' and csat else COUNTDOWN_SECS
            )
    return jsonify({'user': d, 'games': games or []})

@app.route('/api/games', methods=['POST'])
def create_game():
    user, _, err = require_auth()
    if err: return err
    body = request.json or {}
    bet  = body.get('bet_amount')
    if bet not in BET_AMOUNTS:
        return jsonify({'error': f'bet_amount must be one of {BET_AMOUNTS}'}), 400
    # Return open room: countdown first (players already gathered), then waiting
    existing = query_db(
        "SELECT * FROM bingo_games WHERE bet_amount=? AND status IN ('waiting','countdown') "
        "ORDER BY CASE status WHEN 'countdown' THEN 0 ELSE 1 END, created_at ASC LIMIT 1",
        (bet,), one=True
    )
    if existing:
        return jsonify(existing), 200
    if user['balance'] < bet:
        return jsonify({'error': 'Insufficient balance'}), 400
    gid = 'G' + uuid.uuid4().hex[:8].upper()
    execute_db('INSERT INTO bingo_games (id, bet_amount) VALUES (?,?)', (gid, bet))
    game = query_db('SELECT * FROM bingo_games WHERE id=?', (gid,), one=True)
    return jsonify(game), 201

@app.route('/api/games/<gid>', methods=['GET'])
def get_game(gid):
    user, _, err = require_auth()
    if err: return err
    gm = query_db('SELECT * FROM bingo_games WHERE id=?', (gid,), one=True)
    if not gm:
        return jsonify({'error': 'Game not found'}), 404
    gm['called_numbers'] = json.loads(gm['called_numbers'] or '[]')
    now  = time.time()
    csat = gm.get('countdown_started_at')
    gm['countdown_remaining'] = (
        max(0, COUNTDOWN_SECS - int(now - csat))
        if gm['status'] == 'countdown' and csat else COUNTDOWN_SECS
    )
    # Client uses server_time + started_at to derive call index locally
    gm['server_time']    = now
    gm['call_interval']  = CALL_INTERVAL
    # Single query gets players + pot units + my_player in one round-trip
    all_players = query_db(
        'SELECT bp.*, u.username FROM bingo_players bp '
        'JOIN users u ON bp.user_id=u.id WHERE bp.game_id=?', (gid,)
    )
    active_players = [p for p in all_players if not p.get('kicked')]
    pot_units = sum(p.get('bet_paid', 1) for p in all_players)
    gm['players']      = [{'username': p['username'], 'picked_number': p['picked_number'],
                            'picked_number2': p.get('picked_number2'),
                            'has_bingo': p['has_bingo']} for p in active_players]
    gm['player_count'] = len(active_players)
    gm['pot']          = pot_units * gm['bet_amount']
    gm['prize']        = int(gm['pot'] * (1 - HOUSE_CUT))
    my_row = next((p for p in all_players if p['user_id'] == user['id']), None)
    if my_row:
        gm['my_player'] = {
            'card':         json.loads(my_row['card']  or '[]'),
            'card2':        json.loads(my_row['card2'] or 'null') if my_row.get('card2') else None,
            'marked_cells': json.loads(my_row['marked_cells'] or '[[2,2]]'),
            'marked2':      json.loads(my_row.get('marked2') or '[[2,2]]'),
            'has_bingo':    my_row['has_bingo'],
            'kicked':       my_row.get('kicked', 0),
            'bet_paid':     my_row.get('bet_paid', 1),
        }
    return jsonify(gm)

@app.route('/api/games/<gid>/join', methods=['POST'])
def join_game(gid):
    user, _, err = require_auth()
    if err: return err
    body       = request.json or {}
    picked     = body.get('picked_number')
    picked2    = body.get('picked_number2')    # seed for card 2
    dual_card  = bool(body.get('dual_card'))
    gm = query_db('SELECT * FROM bingo_games WHERE id=?', (gid,), one=True)
    if not gm:
        return jsonify({'error': 'Game not found'}), 404
    if gm['status'] not in ('waiting', 'countdown'):
        return jsonify({'error': 'Game already started or finished'}), 400
    existing = query_db(
        'SELECT * FROM bingo_players WHERE game_id=? AND user_id=?', (gid, user['id']), one=True
    )
    if existing and not existing.get('kicked'):
        # Already in game and not kicked — return existing card
        return jsonify({
            'status':      'already_joined',
            'card':        json.loads(existing['card'] or '[]'),
            'card2':       json.loads(existing['card2'] or 'null') if existing.get('card2') else None,
            'marked_cells':json.loads(existing['marked_cells'] or '[[2,2]]'),
            'marked2':     json.loads(existing.get('marked2') or '[[2,2]]'),
        }), 200
    if existing and existing.get('kicked'):
        # Player left earlier — only allow rejoin during waiting/countdown, not active
        if gm['status'] == 'active':
            return jsonify({'error': 'Cannot rejoin a game already in progress'}), 400
        fresh = query_db('SELECT * FROM users WHERE id=?', (user['id'],), one=True)
        cost  = gm['bet_amount'] * (2 if dual_card else 1)
        try:
            is_owner_play = (int(fresh['tg_id']) == OWNER_TG_ID)
        except (ValueError, TypeError):
            is_owner_play = False
        if not is_owner_play and fresh['balance'] < cost:
            return jsonify({'error': 'Insufficient balance'}), 400
        new_card  = generate_bingo_card(picked or random.randint(1, 100))
        new_card2 = generate_bingo_card(picked2 or random.randint(1000, 9999)) if dual_card else None
        with _db_lock:
            if not is_owner_play:
                execute_db('UPDATE users SET balance=balance-?, total_bet=total_bet+? WHERE id=?',
                           (cost, cost, user['id']))
            execute_db(
                'UPDATE bingo_players SET kicked=0, card=?, card2=?, '
                'picked_number=?, picked_number2=?, marked_cells=?, marked2=?, bet_paid=? WHERE id=?',
                (json.dumps(new_card), json.dumps(new_card2) if new_card2 else None,
                 picked, picked2 if dual_card else None,
                 '[[2,2]]', '[[2,2]]', 2 if dual_card else 1, existing['id'])
            )
        return jsonify({
            'status':       'rejoined',
            'card':         new_card,
            'card2':        new_card2,
            'marked_cells': [[2, 2]],
            'marked2':      [[2, 2]],
        })
    if picked:
        taken = query_db(
            'SELECT id FROM bingo_players WHERE game_id=? AND (picked_number=? OR picked_number2=?)',
            (gid, picked, picked), one=True
        )
        if taken:
            return jsonify({'error': 'That number is already taken'}), 409
    if picked2:
        taken2 = query_db(
            'SELECT id FROM bingo_players WHERE game_id=? AND (picked_number=? OR picked_number2=?)',
            (gid, picked2, picked2), one=True
        )
        if taken2:
            return jsonify({'error': 'Card 2 number is already taken by another player'}), 409
    fresh = query_db('SELECT * FROM users WHERE id=?', (user['id'],), one=True)
    cost = gm['bet_amount'] * (2 if dual_card else 1)
    try:
        is_owner_play = (int(fresh['tg_id']) == OWNER_TG_ID)
    except (ValueError, TypeError):
        is_owner_play = False
    if not is_owner_play and fresh['balance'] < cost:
        return jsonify({'error': 'Insufficient balance'}), 400
    card  = generate_bingo_card(picked or random.randint(1, 100))
    card2 = generate_bingo_card(picked2 or random.randint(1000, 9999)) if dual_card else None
    with _db_lock:
        if not is_owner_play:
            execute_db(
                'UPDATE users SET balance=balance-?, total_bet=total_bet+? WHERE id=?',
                (cost, cost, user['id'])
            )
        execute_db(
            'INSERT INTO bingo_players (game_id, user_id, picked_number, picked_number2, card, card2, bet_paid) VALUES (?,?,?,?,?,?,?)',
            (gid, user['id'], picked, picked2 if dual_card else None, json.dumps(card),
             json.dumps(card2) if card2 else None, 2 if dual_card else 1)
        )
    return jsonify({
        'status':       'joined',
        'card':         card,
        'card2':        card2,
        'marked_cells': [[2, 2]],
        'marked2':      [[2, 2]],
    })

@app.route('/api/games/<gid>/leave', methods=['POST'])
def leave_game(gid):
    user, _, err = require_auth()
    if err: return err
    with _db_lock:
        gm = query_db('SELECT * FROM bingo_games WHERE id=?', (gid,), one=True)
        if not gm:
            return jsonify({'ok': True})
        prow = query_db(
            'SELECT * FROM bingo_players WHERE game_id=? AND user_id=?', (gid, user['id']), one=True
        )
        if not prow or prow.get('kicked'):
            return jsonify({'ok': True})
        cost   = gm['bet_amount'] * prow.get('bet_paid', 1)
        status = gm['status']
        execute_db('UPDATE bingo_players SET kicked=1 WHERE id=?', (prow['id'],))
        if status in ('waiting', 'countdown'):
            # Refunded — game never started for them, no history entry
            execute_db('UPDATE users SET balance=balance+? WHERE id=?', (cost, user['id']))
            remaining_all = query_db('SELECT id FROM bingo_players WHERE game_id=? AND kicked=0', (gid,))
            if not remaining_all:
                # Last player left — close the room
                execute_db("UPDATE bingo_games SET status='finished', finished_at=? WHERE id=?",
                           (time.time(), gid))
            elif status == 'countdown' and len(remaining_all) == 1:
                # Only one player left during countdown — reset to waiting
                execute_db(
                    "UPDATE bingo_games SET status='waiting', countdown_started_at=NULL WHERE id=?",
                    (gid,)
                )
            return jsonify({'ok': True, 'refunded': True})
        # Active forfeit — record as 'left' (not a loss)
        execute_db('UPDATE users SET games_played=games_played+1 WHERE id=?', (user['id'],))
        execute_db(
            "INSERT INTO game_history (user_id,game_id,bet_amount,prize_won,won,result) VALUES (?,?,?,0,0,'left')",
            (user['id'], gid, cost)
        )
        remaining = query_db(
            'SELECT * FROM bingo_players WHERE game_id=? AND kicked=0', (gid,)
        )
        if len(remaining) == 1:
            sole   = remaining[0]
            prize2 = int(_calc_pot(gid) * gm['bet_amount'] * (1 - HOUSE_CUT))
            now2   = time.time()
            execute_many([
                ("UPDATE bingo_games SET status='finished', winner_id=?, finished_at=? WHERE id=?",
                 (sole['user_id'], now2, gid)),
                ('UPDATE bingo_players SET has_bingo=1 WHERE id=?', (sole['id'],)),
                ('UPDATE users SET balance=balance+?, wins=wins+1, games_played=games_played+1, total_won=total_won+? WHERE id=?',
                 (prize2, prize2, sole['user_id'])),
                ("INSERT INTO game_history (user_id,game_id,bet_amount,prize_won,won,result) VALUES (?,?,?,?,1,'win')",
                 (sole['user_id'], gid, gm['bet_amount'], prize2)),
            ])
            sole_user = query_db('SELECT tg_id FROM users WHERE id=?', (sole['user_id'],), one=True)
            if sole_user:
                _notify_queue.put({'tg_id': int(sole_user['tg_id']), 'event': '_last_standing', 'data': {'prize': prize2}})
            return jsonify({'ok': True, 'forfeited': cost, 'last_standing': True})
        # Notify remaining players that someone forfeited
        for p in remaining:
            urow = query_db('SELECT tg_id FROM users WHERE id=?', (p['user_id'],), one=True)
            if urow:
                _notify_queue.put({'tg_id': urow['tg_id'], 'event': 'player_forfeited',
                                   'data': {'remaining': len(remaining)}})
    return jsonify({'ok': True, 'forfeited': cost})

@app.route('/api/games/<gid>/mark', methods=['POST'])
def mark_cell(gid):
    """Toggle a cell on card1 or card2. No restriction on which cells can be marked — 
    bingo validation happens at claim time."""
    user, _, err = require_auth()
    if err: return err
    body    = request.json or {}
    r, c    = body.get('row'), body.get('col')
    card_id = body.get('card_id', 1)   # 1 = primary card, 2 = second card
    if r is None or c is None:
        return jsonify({'error': 'row and col required'}), 400
    gm = query_db('SELECT * FROM bingo_games WHERE id=?', (gid,), one=True)
    if not gm or gm['status'] != 'active':
        return jsonify({'error': 'Game not active'}), 400
    prow = query_db(
        'SELECT * FROM bingo_players WHERE game_id=? AND user_id=?', (gid, user['id']), one=True
    )
    if not prow:
        return jsonify({'error': 'You are not in this game'}), 403
    if card_id == 2 and not prow.get('card2'):
        return jsonify({'error': 'No second card'}), 400
    col_key  = 'marked2' if card_id == 2 else 'marked_cells'
    marked   = json.loads(prow.get(col_key) or '[[2,2]]')
    cell     = [r, c]
    if cell in marked:
        marked.remove(cell)    # toggle off
    else:
        marked.append(cell)    # toggle on
    with _db_lock:
        execute_db(
            f'UPDATE bingo_players SET {col_key}=? WHERE id=?',
            (json.dumps(marked), prow['id'])
        )
    return jsonify({'marked_cells': marked, 'card_id': card_id})

@app.route('/api/games/<gid>/claim', methods=['POST'])
def claim_bingo(gid):
    """
    Validate BINGO. If the claim is wrong → kick player (no refund for their bet).
    If correct → pay prize and finish game.
    """
    user, _, err = require_auth()
    if err: return err
    body    = request.json or {}
    card_id = body.get('card_id', 1)   # which card the player is claiming on
    with _db_lock:
        gm = query_db('SELECT * FROM bingo_games WHERE id=?', (gid,), one=True)
        if not gm:
            return jsonify({'error': 'Game not found'}), 404
        if gm['status'] != 'active':
            return jsonify({'error': 'Game not active'}), 400
        if gm['winner_id']:
            return jsonify({'error': 'A winner has already been declared'}), 400
        prow = query_db(
            'SELECT * FROM bingo_players WHERE game_id=? AND user_id=?', (gid, user['id']), one=True
        )
        if not prow:
            return jsonify({'error': 'You are not in this game'}), 403
        if prow.get('kicked'):
            return jsonify({'error': 'You have been removed from this game'}), 403
        called  = json.loads(gm['called_numbers'] or '[]')
        # Parse called numbers — stored as "B-7" strings, extract integer part
        called_ints = []
        for c in called:
            try: called_ints.append(int(c.split('-')[1]))
            except Exception: pass
        if card_id == 2 and prow.get('card2'):
            card   = json.loads(prow['card2'])
            marked = json.loads(prow.get('marked2') or '[[2,2]]')
        else:
            card   = json.loads(prow['card'])
            marked = json.loads(prow['marked_cells'])
        # Admins bypass BINGO validation (testing only — remove before production)
        is_admin_player = bool(user.get('is_admin') or user.get('is_owner') or
                               str(user.get('tg_id','')) in ADMIN_TG_IDS)
        valid = True if is_admin_player else check_bingo(card, marked, called_ints)
        if not valid:
            # False claim → kick and forfeit their bet (already deducted at join)
            execute_db(
                'UPDATE bingo_players SET kicked=1 WHERE id=?', (prow['id'],)
            )
            execute_db(
                'UPDATE users SET games_played=games_played+1 WHERE id=?', (user['id'],)
            )
            execute_db(
                "INSERT INTO game_history (user_id,game_id,bet_amount,prize_won,won,result) VALUES (?,?,?,0,0,'loss')",
                (user['id'], gid, gm['bet_amount'] * prow.get('bet_paid', 1))
            )
            # Check remaining active players after kick
            remaining = query_db(
                'SELECT * FROM bingo_players WHERE game_id=? AND kicked=0', (gid,)
            )
            if len(remaining) == 1:
                sole   = remaining[0]
                prize2 = int(_calc_pot(gid) * gm['bet_amount'] * (1 - HOUSE_CUT))
                now2  = time.time()
                execute_many([
                    ("UPDATE bingo_games SET status='finished', winner_id=?, finished_at=? WHERE id=?",
                     (sole['user_id'], now2, gid)),
                    ('UPDATE bingo_players SET has_bingo=1 WHERE id=?', (sole['id'],)),
                    ('UPDATE users SET balance=balance+?, wins=wins+1, games_played=games_played+1, '
                     'total_won=total_won+? WHERE id=?', (prize2, prize2, sole['user_id'])),
                    ("INSERT INTO game_history (user_id,game_id,bet_amount,prize_won,won,result) VALUES (?,?,?,?,1,'win')",
                     (sole['user_id'], gid, gm['bet_amount'], prize2)),
                ])
                sole_tg = query_db('SELECT tg_id FROM users WHERE id=?', (sole['user_id'],), one=True)
                if sole_tg:
                    _notify_queue.put({'tg_id': int(sole_tg['tg_id']), 'event': '_last_standing',
                                       'data': {'prize': prize2}})
            return jsonify({'kicked': True, 'error': 'Wrong BINGO — you have been removed from the game!'}), 200
        # Pot = sum of all actual payments (dual-card pays 2x)
        total_units = _calc_pot(gid)
        pot   = total_units * gm['bet_amount']
        prize = int(pot * (1 - HOUSE_CUT))
        now   = time.time()
        execute_many([
            ("UPDATE bingo_games SET status='finished', winner_id=?, finished_at=? WHERE id=?",
             (user['id'], now, gid)),
            ('UPDATE bingo_players SET has_bingo=1 WHERE id=?', (prow['id'],)),
            ('UPDATE users SET balance=balance+?, wins=wins+1, games_played=games_played+1, '
             'total_won=total_won+? WHERE id=?', (prize, prize, user['id'])),
            ("INSERT INTO game_history (user_id,game_id,bet_amount,prize_won,won,result) VALUES (?,?,?,?,1,'win')",
             (user['id'], gid, gm['bet_amount'], prize)),
        ])
        losers = query_db(
            'SELECT user_id FROM bingo_players WHERE game_id=? AND user_id!=? AND kicked=0',
            (gid, user['id'])
        )
        for lo in losers:
            execute_many([
                ('UPDATE users SET games_played=games_played+1 WHERE id=?', (lo['user_id'],)),
                ("INSERT INTO game_history (user_id,game_id,bet_amount,prize_won,won,result) VALUES (?,?,?,0,0,'loss')",
                 (lo['user_id'], gid, gm['bet_amount'] * lo.get('bet_paid', 1))),
            ])
        fresh = query_db('SELECT * FROM users WHERE id=?', (user['id'],), one=True)
    return jsonify({'winner': True, 'prize': prize, 'user': fresh})

@app.route('/api/history', methods=['GET'])
def get_history():
    user, _, err = require_auth()
    if err: return err
    rows = query_db(
        'SELECT * FROM game_history WHERE user_id=? ORDER BY played_at DESC LIMIT 15',
        (user['id'],)
    )
    return jsonify(rows)

@app.route('/api/transactions', methods=['GET'])
def get_transactions():
    user, _, err = require_auth()
    if err: return err
    rows = query_db(
        'SELECT type, amount, ref, note, created_at FROM transactions '
        'WHERE tg_id=? ORDER BY created_at DESC LIMIT 30',
        (user['tg_id'],)
    )
    return jsonify(rows)

@app.route('/api/leaderboard', methods=['GET'])
def leaderboard():
    rows = query_db(
        'SELECT username,wins,games_played,total_won,balance FROM users '
        'ORDER BY wins DESC, total_won DESC LIMIT 20'
    )
    return jsonify(rows)

# ── Preview card without joining ────────────────────────────────────────────
@app.route('/api/preview-card/<int:seed>', methods=['GET'])
def preview_card(seed):
    user, _, err = require_auth()
    if err: return err
    card = generate_bingo_card(seed)
    return jsonify({'card': card})

# ── Admin: user lookup by tg_id (for bot autocomplete) ───────────────────────
@app.route('/api/admin/user-by-tgid/<tgid>', methods=['GET'])
def admin_user_by_tgid(tgid):
    user, err = require_admin()
    if err: return err
    row = query_db('SELECT username,tg_id,balance FROM users WHERE tg_id=?', (str(tgid),), one=True)
    if not row: return jsonify({'error': 'Not found'}), 404
    return jsonify(row)

# ── First-deposit bonus endpoint ─────────────────────────────────────────────
@app.route('/api/deposit-complete', methods=['POST'])
def deposit_complete():
    admin, err = require_admin()
    if err: return err
    body   = request.json or {}
    tg_id  = str(body.get('tg_id', ''))
    amount = int(body.get('amount', 0))
    target = query_db('SELECT * FROM users WHERE tg_id=?', (tg_id,), one=True)
    if not target:
        return jsonify({'error': 'User not found'}), 404
    bonus = 0
    if target.get('deposit_count', 0) == 0 and amount >= MIN_DEPOSIT:
        bonus = int(amount * 0.10)
        execute_db('UPDATE users SET balance=balance+?, deposit_count=deposit_count+1 WHERE tg_id=?',
                   (bonus, tg_id))
    else:
        execute_db('UPDATE users SET deposit_count=deposit_count+1 WHERE tg_id=?', (tg_id,))
    fresh = query_db('SELECT * FROM users WHERE tg_id=?', (tg_id,), one=True)
    return jsonify({'bonus': bonus, 'new_balance': fresh['balance']})

# ── Admin routes ──────────────────────────────────────────────────────────────
@app.route('/api/admin/stats', methods=['GET'])
def admin_stats():
    user, err = require_admin()
    if err: return err
    _tp = query_db('SELECT COALESCE(SUM(prize_won),0) AS s FROM game_history', one=True)
    _tb = query_db('SELECT COALESCE(SUM(bet_amount),0) AS s FROM game_history', one=True)
    house_cut_etb = int((_tb or {}).get('s', 0) * HOUSE_CUT)
    stats = {
        'total_users':    query_db('SELECT COUNT(*) AS c FROM users', one=True)['c'],
        'total_games':    query_db('SELECT COUNT(*) AS c FROM bingo_games', one=True)['c'],
        'active_games':   query_db(
            "SELECT COUNT(*) AS c FROM bingo_games WHERE status IN ('waiting','countdown','active')",
            one=True)['c'],
        'finished_games': query_db(
            "SELECT COUNT(*) AS c FROM bingo_games WHERE status='finished'", one=True)['c'],
        'total_prizes':   (_tp or {}).get('s', 0),
        'house_cut_etb':  house_cut_etb,
    }
    return jsonify(stats)

@app.route('/api/admin/users', methods=['GET'])
def admin_users():
    user, err = require_admin()
    if err: return err
    q = request.args.get('q', '').strip()
    total = query_db('SELECT COUNT(*) AS c FROM users', one=True)['c']
    if q:
        rows = query_db(
            "SELECT * FROM users WHERE username LIKE ? OR tg_id=? ORDER BY created_at DESC LIMIT 10",
            (f'%{q}%', q)
        )
    else:
        rows = query_db('SELECT * FROM users ORDER BY created_at DESC LIMIT 10')
    return jsonify({'users': rows, 'total': total})

@app.route('/api/admin/games', methods=['GET'])
def admin_games():
    user, err = require_admin()
    if err: return err
    rows = query_db("""
        SELECT g.*, COUNT(p.id) as player_count
        FROM bingo_games g LEFT JOIN bingo_players p ON g.id=p.game_id
        GROUP BY g.id ORDER BY g.created_at DESC LIMIT 100
    """)
    return jsonify(rows)

@app.route('/api/admin/funds', methods=['POST'])
def admin_funds():
    admin, err = require_admin()
    if err: return err
    body     = request.json or {}
    username = body.get('username', '').strip()
    amount   = body.get('amount', 0)
    target   = query_db('SELECT * FROM users WHERE username=?', (username,), one=True)
    if not target:
        return jsonify({'error': 'User not found'}), 404
    new_bal = max(0, target['balance'] + int(amount))
    execute_db('UPDATE users SET balance=? WHERE username=?', (new_bal, username))
    # Record transaction and notify user
    amt_abs = abs(int(amount))
    if amt_abs > 0:
        import hashlib as _hlf
        tx_type = 'deposit' if int(amount) > 0 else 'withdraw'
        tx_ref  = _hlf.md5(f"{tx_type}:{target['tg_id']}:{amt_abs}:{int(time.time())//300}".encode()).hexdigest()[:16]
        try:
            execute_db(
                'INSERT INTO transactions (tg_id,type,amount,ref,note) VALUES (?,?,?,?,?)',
                (target['tg_id'], tx_type, amt_abs, tx_ref, f'admin:{admin["id"]}')
            )
        except Exception:
            pass  # duplicate ref — ignore
        event = 'deposit_approved' if tx_type == 'deposit' else 'withdraw_approved'
        _notify_queue.put({'tg_id': int(target['tg_id']), 'event': event,
                           'data': {'amount': amt_abs, 'new_balance': new_bal}})
    return jsonify({'success': True, 'username': username, 'new_balance': new_bal})

@app.route('/api/admin/games/<gid>/finish', methods=['POST'])
def admin_force_finish(gid):
    admin, err = require_admin()
    if err: return err
    gm = query_db('SELECT * FROM bingo_games WHERE id=?', (gid,), one=True)
    if not gm:
        return jsonify({'error': 'Game not found'}), 404
    if gm['status'] == 'finished':
        return jsonify({'error': 'Already finished'}), 400
    with _db_lock:
        players = query_db('SELECT * FROM bingo_players WHERE game_id=?', (gid,))
        for p in players:
            execute_db('UPDATE users SET balance=balance+? WHERE id=?', (gm['bet_amount'], p['user_id']))
        execute_db(
            "UPDATE bingo_games SET status='finished', finished_at=? WHERE id=?",
            (time.time(), gid)
        )
    return jsonify({'success': True, 'refunded': len(players)})

@app.route('/api/admin/users/<tgid>/ban', methods=['POST'])
def admin_ban_user(tgid):
    admin, err = require_admin()
    if err: return err
    body   = request.json or {}
    banned = 1 if body.get('ban') else 0
    target = query_db('SELECT * FROM users WHERE tg_id=?', (str(tgid),), one=True)
    if not target: return jsonify({'error': 'User not found'}), 404
    execute_db('UPDATE users SET is_banned=? WHERE tg_id=?', (banned, str(tgid)))
    return jsonify({'success': True, 'username': target['username'], 'banned': banned})

@app.route('/api/admin/users/<int:uid>/admin', methods=['POST'])
def toggle_admin(uid):
    admin, err = require_admin()
    if err: return err
    if int(admin['tg_id']) != OWNER_TG_ID:
        return jsonify({'error': 'Only the owner can manage admins'}), 403
    target = query_db('SELECT * FROM users WHERE id=?', (uid,), one=True)
    if target and int(target['tg_id']) == OWNER_TG_ID:
        return jsonify({'error': 'Cannot change owner status'}), 403
    is_adm = 1 if (request.json or {}).get('is_admin') else 0
    execute_db('UPDATE users SET is_admin=? WHERE id=?', (is_adm, uid))
    return jsonify({'success': True})

# ── Health check (for UptimeRobot / Render) ───────────────────────────────────
@app.route('/api/deposit-request', methods=['POST'])
def webapp_deposit_request():
    user, tg_user, err = require_auth()
    if err: return err
    body          = request.json or {}
    amt           = float(body.get('amount', 0))
    ref           = str(body.get('reference', '')).strip()
    sender_name   = str(body.get('sender_name', '')).strip()  # name as on payment
    if amt < MIN_DEPOSIT:
        return jsonify({'error': f'Min deposit {MIN_DEPOSIT} ETB'}), 400
    if not ref:
        return jsonify({'error': 'Reference required'}), 400
    if not sender_name:
        return jsonify({'error': 'Sender name required'}), 400
    fresh = query_db('SELECT * FROM users WHERE id=?', (user['id'],), one=True)
    admin_txt = (
        f"📥 <b>Deposit Request (WebApp)</b>\n\n"
        f"JB Username: <b>{fresh['username']}</b>\n"
        f"TG ID: <code>{user['tg_id']}</code>\n"
        f"Sender Name: {sender_name}\n"
        f"Amount: <b>{amt:,.0f} ETB</b>\nReference: <code>{ref}</code>\n"
        f"Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC\n\n"
        f"✅ Approve: <code>/admindeposit {user['tg_id']} {amt:.0f}</code>"
    )
    if not ADMIN_TG_IDS_INT:
        logger.warning('[deposit-request] ADMIN_TG_IDS_INT is empty — set ADMIN_TG_IDS env var')
    for aid in ADMIN_TG_IDS_INT:
        sent = _tg_notify_direct(aid, admin_txt)
        if not sent:
            _notify_queue.put({'tg_id': aid, 'event': 'custom', 'data': {'message': admin_txt}})
    _notify_queue.put({'tg_id': int(user['tg_id']), 'event': 'custom', 'data': {'message':
        f"⏳ <b>Deposit Request Received</b>\n\n"
        f"Amount: <b>{amt:,.0f} ETB</b> · Ref: <code>{ref}</code>\n"
        f"Pending admin approval. You'll be notified once confirmed. ✅"}})
    return jsonify({'status': 'submitted'})

def _check_withdrawal_eligibility(user, withdrawal_amount=None):
    """
    Returns (ok:bool, error:str|None).
    Rules (all use StandaloneDB for bot thread safety):
      - Must have at least one deposit of 50+ ETB.
      - Must have played 15+ total games (result != 'left').
      - Must have played 5+ games today.
      - Max 1 withdrawal request per calendar day.
      - After withdrawal, balance must be >= 50 ETB.
    """
    tg_id = user['tg_id']
    uid   = user['id']
    balance = user['balance']

    with StandaloneDB() as sdb:
        # 1. At least one deposit of 50+ ETB
        deposit_row = sdb.one(
            "SELECT amount FROM transactions WHERE tg_id=? AND type='deposit' AND amount>=50 LIMIT 1",
            (tg_id,)
        )
        if not deposit_row:
            return False, "You must make at least one deposit of 50 ETB before withdrawing."

        # 2. Total games (excluding left)
        total_games = sdb.one(
            "SELECT COUNT(*) AS c FROM game_history WHERE user_id=? AND result != 'left'",
            (uid,)
        )['c']
        if total_games < 15:
            remaining = 15 - total_games
            return False, f"You need to play {remaining} more game(s) (total 15). You have played {total_games}."

        # 3. Games today
        today_games = sdb.one(
            "SELECT COUNT(*) AS c FROM game_history WHERE user_id=? AND result != 'left' AND DATE(played_at) = DATE('now')",
            (uid,)
        )['c']
        if today_games < 5:
            remaining_today = 5 - today_games
            return False, f"You need to play {remaining_today} more game(s) today. Played today: {today_games}."

        # 4. Daily withdrawal limit (1 per day)
        wd_today = sdb.one(
            "SELECT COUNT(*) AS c FROM transactions WHERE tg_id=? AND type='withdraw_request' AND DATE(created_at) = DATE('now')",
            (tg_id,)
        )['c']
        if wd_today >= 1:
            return False, "You have already submitted a withdrawal request today. Try again tomorrow."

    # 5. After withdrawal balance check
    if withdrawal_amount is not None:
        if balance - withdrawal_amount < 50:
            return False, "After withdrawal, your balance must be at least 50 ETB."

    return True, None

@app.route('/api/withdraw-request', methods=['POST'])
def webapp_withdraw_request():
    user, tg_user, err = require_auth()
    if err: return err
    body         = request.json or {}
    amt          = float(body.get('amount', 0))
    phone        = str(body.get('phone', '')).strip()
    bank         = str(body.get('bank', '')).strip()         # 'CBE' or 'Telebirr'
    account_name = str(body.get('account_name', '')).strip()
    if amt < MIN_WITHDRAW:
        return jsonify({'error': f'Minimum withdrawal is {MIN_WITHDRAW} ETB'}), 400
    if not phone:
        return jsonify({'error': 'Phone/account number required'}), 400
    if not bank:
        return jsonify({'error': 'Select payment method'}), 400
    if not account_name:
        return jsonify({'error': 'Account name required'}), 400
    fresh = query_db('SELECT * FROM users WHERE id=?', (user['id'],), one=True)
    if fresh['balance'] < amt:
        return jsonify({'error': 'Insufficient balance'}), 400
    # Check eligibility (including post-withdrawal balance)
    ok_wd, err_wd = _check_withdrawal_eligibility(fresh, amt)
    if not ok_wd:
        return jsonify({'error': err_wd}), 400
    # Record the withdrawal request in transactions for daily limit tracking
    import hashlib as _hwd
    wd_req_ref = _hwd.md5(f"wdreq:{fresh['tg_id']}:{int(amt)}:{int(time.time())//86400}".encode()).hexdigest()[:16]
    try:
        execute_db('INSERT INTO transactions (tg_id,type,amount,ref,note) VALUES (?,?,?,?,?)',
                   (fresh['tg_id'], 'withdraw_request', int(amt), wd_req_ref, 'webapp'))
    except Exception: pass  # duplicate ref in same day — blocked above anyway
    admin_txt = (
        f"💸 <b>Withdrawal Request (WebApp)</b>\n\n"
        f"JB Username: <b>{fresh['username']}</b>\n"
        f"TG ID: <code>{user['tg_id']}</code>\n"
        f"Amount: <b>{amt:,.0f} ETB</b>\n"
        f"Method: <b>{bank}</b>\n"
        f"Account: <code>{phone}</code> ({account_name})\n"
        f"Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC\n\n"
        f"✅ Approve: <code>/adminwithdraw {user['tg_id']} {amt:.0f}</code>"
    )
    if not ADMIN_TG_IDS_INT:
        logger.warning('[withdraw-request] ADMIN_TG_IDS_INT is empty — set ADMIN_TG_IDS env var')
    for aid in ADMIN_TG_IDS_INT:
        sent = _tg_notify_direct(aid, admin_txt)
        if not sent:
            _notify_queue.put({'tg_id': aid, 'event': 'custom', 'data': {'message': admin_txt}})
    _notify_queue.put({'tg_id': int(user['tg_id']), 'event': 'custom', 'data': {'message':
        f"⏳ <b>Withdrawal Request Received</b>\n\n"
        f"Amount: <b>{amt:,.0f} ETB</b> via {bank}\n"
        f"Account: {phone} ({account_name})\n"
        f"Pending processing. You'll be notified once sent. ✅"}})
    return jsonify({'status': 'submitted'})

@app.route('/api/games-today', methods=['GET'])
def games_today():
    user, _, err = require_auth()
    if err: return err
    with StandaloneDB() as sdb:
        count = sdb.one(
            "SELECT COUNT(*) AS c FROM game_history WHERE user_id=? AND result != 'left' AND DATE(played_at) = DATE('now')",
            (user['id'],)
        )['c']
    return jsonify({'count': count})

@app.route('/api/record-withdrawal-date', methods=['POST'])
def record_withdrawal_date():
    user, _, err = require_auth()
    if err: return err
    execute_db(
        'UPDATE users SET last_withdrawal_date=? WHERE id=?',
        (datetime.utcnow().date().isoformat(), user['id'])
    )
    return jsonify({'ok': True})

@app.route('/health')
def health():
    return jsonify({'status': 'ok', 'db': DB_TYPE})

# ── Direct Telegram notification (bypasses queue — works before bot starts) ──
def _tg_notify_direct(chat_id: int, text: str) -> bool:
    if not BOT_TOKEN or not chat_id:
        return False
    import urllib.request as _urlreq
    try:
        payload = json.dumps({'chat_id': chat_id, 'text': text, 'parse_mode': 'HTML'}).encode()
        req = _urlreq.Request(
            f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage',
            data=payload, headers={'Content-Type': 'application/json'}, method='POST'
        )
        _urlreq.urlopen(req, timeout=8)
        return True
    except Exception as exc:
        logger.warning(f'[tg_direct] {chat_id}: {exc}')
        return False

# ── Internal notify endpoint (bot calls this to push Telegram messages) ────────
@app.route('/internal/notify', methods=['POST'])
def internal_notify():
    if request.headers.get('X-Internal-Secret') != INTERNAL_SECRET:
        return jsonify({'error': 'Unauthorized'}), 401
    # Notification is handled by the bot thread; we just queue it
    data   = request.json or {}
    tg_id  = data.get('tg_id')
    event  = data.get('event', '')
    if not tg_id:
        return jsonify({'error': 'tg_id required'}), 400
    _notify_queue.put({'tg_id': int(tg_id), 'event': event, 'data': data})
    return jsonify({'ok': True})

# ══════════════════════════════════════════════════════════════════════════════
#  TELEGRAM BOT — runs in a background daemon thread inside this same process
#
#  IMPORTANT: Gunicorn must use --workers 1
#             Multiple workers = multiple bot polling instances = Telegram error
# ══════════════════════════════════════════════════════════════════════════════

import queue as _queue_module
_notify_queue: _queue_module.Queue = _queue_module.Queue()

# ── FSM States ────────────────────────────────────────────────────────────────
class Registration(StatesGroup):
    waiting_for_username = State()
    waiting_for_phone    = State()

class Deposit(StatesGroup):
    waiting_for_amount  = State()
    waiting_for_sms_ref = State()

class Withdrawal(StatesGroup):
    waiting_for_amount       = State()
    waiting_for_bank         = State()
    waiting_for_phone        = State()
    waiting_for_account_name = State()

# ── Bot API helper (calls Flask REST API — same host, loopback) ───────────────
async def bot_api(method: str, path: str, tg_id: int | None = None,
                  body: dict | None = None,
                  session: aiohttp.ClientSession | None = None) -> dict:
    """
    Call the local Flask API from inside the bot.
    Uses localhost so traffic never leaves the machine — zero latency.
    """
    port    = int(os.environ.get('PORT', 5000))
    base    = f'http://127.0.0.1:{port}'
    headers = {'Content-Type': 'application/json'}
    if tg_id:
        headers['X-Tg-User'] = json.dumps({'id': tg_id})

    own_session = session is None
    if own_session:
        session = aiohttp.ClientSession()
    try:
        async with session.request(method, base + path, json=body, headers=headers) as resp:
            data = await resp.json(content_type=None)
            if not resp.ok:
                raise ValueError(data.get('error', f'API {resp.status}'))
            return data
    finally:
        if own_session:
            await session.close()

# ── Notification helper ───────────────────────────────────────────────────────
async def bot_notify(bot: Bot, tg_id: int, text: str) -> bool:
    try:
        await bot.send_message(chat_id=tg_id, text=text, parse_mode=ParseMode.HTML)
        return True
    except Exception as exc:
        logger.warning(f'[notify] failed for {tg_id}: {exc}')
        return False

# ── Keyboards ─────────────────────────────────────────────────────────────────
def kb_main() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text='🎮 Play Bingo')],
            [KeyboardButton(text='💰 Deposit'),  KeyboardButton(text='💳 Withdraw')],
            [KeyboardButton(text='📊 My Stats'), KeyboardButton(text='🏆 Leaderboard')],
            [KeyboardButton(text='📨 Invite Friends'), KeyboardButton(text='🆘 Support')],
        ],
        resize_keyboard=True,
        input_field_placeholder='Choose an option…',
    )

def kb_cancel() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text='❌ Cancel')]],
        resize_keyboard=True, one_time_keyboard=True,
    )

def kb_phone() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text='📱 Share Phone Number', request_contact=True)],
            [KeyboardButton(text='⏭ Skip')],
        ],
        resize_keyboard=True, one_time_keyboard=True,
    )

def kb_webapp(label: str = '🎱 Open Joker Bingo', bet: int | None = None) -> InlineKeyboardMarkup:
    port = int(os.environ.get('PORT', 5000))
    base = os.environ.get('WEBAPP_URL', f'http://localhost:{port}').rstrip('/')
    url  = f'{base}/?bet={bet}' if bet else f'{base}/'
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=label, web_app=WebAppInfo(url=url))
    ]])

def kb_bets(balance: int) -> InlineKeyboardMarkup:
    rows = []
    pair = []
    for a in BET_AMOUNTS:
        label = f'🪙 {a} ETB' + (' ✓' if balance >= a else ' 🔒')
        pair.append(InlineKeyboardButton(text=label, callback_data=f'bet:{a}'))
        if len(pair) == 2:
            rows.append(pair); pair = []
    if pair:
        rows.append(pair)
    rows.append([InlineKeyboardButton(text='❌ Cancel', callback_data='bet:cancel')])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# ── Router ────────────────────────────────────────────────────────────────────
bot_router = Router(name='joker_bingo')

def admin_guard(handler):
    import inspect as _inspect
    async def _w(message: Message, *a, **kw):
        if message.from_user.id not in ADMIN_TG_IDS_INT:
            await message.answer('⛔ Admin access required.')
            return
        sig      = _inspect.signature(handler)
        accepted = {k: v for k, v in kw.items() if k in sig.parameters}
        return await handler(message, *a, **accepted)
    _w.__name__ = handler.__name__
    return _w

# /start ──────────────────────────────────────────────────────────────────────
@bot_router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext, session: aiohttp.ClientSession):
    await state.clear()
    tg   = message.from_user
    args = message.text.split()[1:]
    ref  = int(args[0]) if args and args[0].isdigit() and int(args[0]) != tg.id else None
    if ref:
        await state.update_data(referrer_id=ref)
    try:
        res = await bot_api('POST', '/api/auth', tg_id=tg.id,
                            body={'tg_user': {'id': tg.id, 'username': tg.username,
                                              'first_name': tg.first_name}},
                            session=session)
    except Exception as exc:
        await message.answer(f'⚠️ Server error: {exc}'); return

    LOGO_URL = os.environ.get('LOGO_URL', '')
    if res.get('status') == 'existing':
        u = res['user']
        welcome_text = (
            f"👋 Welcome back, <b>{u['username']}</b>!\n\n"
            f"💰 Balance: <b>{u['balance']:,} ETB</b>\n"
            f"🏆 Wins: {u['wins']}  ·  🎮 Games: {u['games_played']}"
        )
        if LOGO_URL:
            try:
                await message.answer_photo(photo=LOGO_URL, caption=welcome_text,
                                           reply_markup=kb_main())
            except Exception:
                await message.answer(welcome_text, reply_markup=kb_main())
        else:
            await message.answer(welcome_text, reply_markup=kb_main())
    else:
        suggested = res.get('suggested_username') or tg.first_name or 'Player'
        await state.set_state(Registration.waiting_for_username)
        await state.update_data(tg_id=str(tg.id))
        new_text = (
            '🎱 <b>Welcome to Joker Bingo!</b>\n\n'
            'Pick numbers · Mark your card · Shout <b>BINGO!</b>\n'
            'Win up to <b>90% of the pot</b> every round 💰\n\n'
            f'Choose a username (2–20 chars, no spaces):\n'
            f'<i>Suggestion:</i> <code>{suggested}</code>'
        )
        if LOGO_URL:
            try:
                await message.answer_photo(photo=LOGO_URL, caption=new_text,
                                           reply_markup=kb_cancel())
            except Exception:
                await message.answer(new_text, reply_markup=kb_cancel())
        else:
            await message.answer(new_text, reply_markup=kb_cancel())

# Registration FSM ────────────────────────────────────────────────────────────
@bot_router.message(Registration.waiting_for_username, F.text)
async def reg_username(message: Message, state: FSMContext, session: aiohttp.ClientSession):
    if message.text in ('❌ Cancel', '/cancel'):
        await state.clear()
        await message.answer('Registration cancelled.', reply_markup=ReplyKeyboardRemove()); return
    u = message.text.strip()
    if len(u) < 2 or len(u) > 20 or ' ' in u:
        await message.answer('⚠️ 2–20 chars, no spaces. Try again:'); return
    data = await state.get_data()
    try:
        res  = await bot_api('POST', '/api/register', tg_id=int(data['tg_id']),
                             body={'tg_id': data['tg_id'], 'username': u,
                                   'referred_by': data.get('referrer_id')}, session=session)
    except ValueError as exc:
        await message.answer(f'⚠️ {exc}\nTry a different username:'); return
    user = res['user']

    # Referral — pass referrer tg_id to /api/register (backend handles bonus logic)
    # (Already handled server-side via referred_by field in register endpoint)

    await state.set_state(Registration.waiting_for_phone)
    await message.answer(
        f"✅ <b>Account created!</b>\n\n"
        f"Username: <b>{user['username']}</b>\n"
        f"Welcome bonus: <b>{WELCOME_BONUS} ETB</b> 🎁\n\n"
        "📱 <b>Share your phone number</b> to complete registration.\n"
        "Required for deposits & withdrawals.",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text='📱 Share Phone', request_contact=True)]],
            resize_keyboard=True, one_time_keyboard=True,
        )
    )

@bot_router.message(Registration.waiting_for_phone, F.contact)
async def reg_phone(message: Message, state: FSMContext):
    if message.contact.user_id != message.from_user.id:
        await message.answer('⚠️ Share <b>your own</b> contact.'); return
    await state.clear()
    link = f'https://t.me/{_bot_username}?start={message.from_user.id}'
    await message.answer(
        f'✅ <b>Phone verified!</b>\n\n'
        f'🔗 Referral link:\n<code>{link}</code>\n\n'
        f'Earn <b>{REFERRAL_BONUS} ETB</b> per friend who joins! 🎉',
        reply_markup=kb_main(),
    )

@bot_router.message(Registration.waiting_for_phone, F.text)
async def reg_phone_text(message: Message, state: FSMContext):
    await message.answer('⚠️ Please tap the <b>📱 Share Phone</b> button to continue.')

# /cancel  ────────────────────────────────────────────────────────────────────
@bot_router.message(Command('cancel'))
@bot_router.message(F.text == '❌ Cancel')
async def cmd_cancel(message: Message, state: FSMContext):
    cur = await state.get_state()
    await state.clear()
    txt = '✅ Cancelled.' if cur else 'Nothing to cancel.'
    await message.answer(txt, reply_markup=kb_main())

# /help ───────────────────────────────────────────────────────────────────────
@bot_router.message(Command('help'))
async def cmd_help(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        '🎱 <b>Joker Bingo — How to Play</b>\n\n'
        '1. Tap 🎮 Play Bingo → choose bet (10/20/50/100 ETB)\n'
        '2. Open the game and pick your lucky number\n'
        '3. Optionally choose a 2nd card (double bet, double chance!)\n'
        '4. 2+ players join → 30-second countdown → GO!\n'
        '5. Numbers called every 2 seconds (e.g. B-7, O-65)\n'
        '6. Mark numbers on your card in the correct column (B I N G O)\n'
        '7. Complete row / column / diagonal → tap 🎉 BINGO!\n'
        '8. Wrong BINGO claim = removed from game!\n'
        '9. Winner takes <b>90% of the pot</b>\n\n'
        '<b>Commands:</b> /start /balance /stats /history /referral /cancel /help\n'
        '<b>Support:</b> @Tewodros1888',
    )

# /balance ────────────────────────────────────────────────────────────────────
@bot_router.message(Command('balance'))
async def cmd_balance(message: Message, session: aiohttp.ClientSession):
    try:
        u = await bot_api('GET', '/api/user', tg_id=message.from_user.id, session=session)
    except Exception as exc:
        await message.answer(f'⚠️ {exc}'); return
    await message.answer(
        f"💰 <b>Wallet — {u['username']}</b>\n\n"
        f"Balance:   <b>{u['balance']:,} ETB</b>\n"
        f"Total won: {u['total_won']:,} ETB\n"
        f"Total bet: {u['total_bet']:,} ETB",
    )

# 📊 My Stats ─────────────────────────────────────────────────────────────────
@bot_router.message(Command('stats'))
@bot_router.message(F.text == '📊 My Stats')
async def cmd_stats(message: Message, session: aiohttp.ClientSession):
    try:
        u   = await bot_api('GET', '/api/user',    tg_id=message.from_user.id, session=session)
        his = await bot_api('GET', '/api/history', tg_id=message.from_user.id, session=session)
    except Exception as exc:
        await message.answer(f'⚠️ {exc}'); return
    gp  = u['games_played'] or 1
    wr  = round(u['wins'] / gp * 100, 1)
    rec = ''.join(
        f"{'✅' if h['won'] else '❌'} {'+' if h['won'] else '−'}"
        f"{h['prize_won'] if h['won'] else h['bet_amount']:,} ETB · {str(h['played_at'])[:10]}\n"
        for h in his[:5]
    ) or '<i>No games yet</i>'
    await message.answer(
        f"📊 <b>Stats — {u['username']}</b>\n\n"
        f"💰 Balance:      <b>{u['balance']:,} ETB</b>\n"
        f"🎮 Games played: {u['games_played']}\n"
        f"🏆 Wins:         {u['wins']}\n"
        f"📈 Win rate:     {wr}%\n"
        f"💵 Total won:    {u['total_won']:,} ETB\n\n"
        f"<b>Last 5 games:</b>\n{rec}",
    )

# /history ────────────────────────────────────────────────────────────────────
@bot_router.message(Command('history'))
async def cmd_history(message: Message, session: aiohttp.ClientSession):
    try:
        his = await bot_api('GET', '/api/history', tg_id=message.from_user.id, session=session)
    except Exception as exc:
        await message.answer(f'⚠️ {exc}'); return
    if not his:
        await message.answer('📜 No games yet — tap 🎮 Play Bingo to start!'); return
    lines = [
        f"{'🏆' if h['won'] else '💸'} <b>{'+' if h['won'] else '−'}"
        f"{h['prize_won'] if h['won'] else h['bet_amount']:,} ETB</b>"
        f" · {h['bet_amount']} bet · {str(h['played_at'])[:10]}"
        for h in his[:20]
    ]
    await message.answer('📜 <b>Game History (last 15)</b>\n\n' + '\n'.join(lines))

# /referral ───────────────────────────────────────────────────────────────────
@bot_router.message(Command('referral'))
async def cmd_referral(message: Message):
    link = f'https://t.me/{_bot_username}?start={message.from_user.id}'
    await message.answer(
        f'🔗 <b>Your Referral Link</b>\n\n<code>{link}</code>\n\n'
        f'Earn <b>{REFERRAL_BONUS} ETB</b> every {REFERRALS_PER_BONUS} friends! 🎁',
    )

# 🏆 Leaderboard ──────────────────────────────────────────────────────────────
@bot_router.message(F.text == '🏆 Leaderboard')
async def cmd_leaderboard(message: Message, session: aiohttp.ClientSession):
    try:
        rows = await bot_api('GET', '/api/leaderboard', tg_id=message.from_user.id, session=session)
    except Exception as exc:
        await message.answer(f'⚠️ {exc}'); return
    medals = ['🥇', '🥈', '🥉']
    lines  = [
        f"{medals[i] if i < 3 else f'{i+1}.'} <b>{u['username']}</b>"
        f" — {u['wins']}W · {u['total_won']:,} ETB"
        for i, u in enumerate(rows[:10])
    ]
    await message.answer('🏆 <b>Top 10 Players</b>\n\n' + '\n'.join(lines) if lines else 'No players yet.')

# 🎮 Play Bingo ───────────────────────────────────────────────────────────────
@bot_router.message(F.text == '🎮 Play Bingo')
async def cmd_play(message: Message, session: aiohttp.ClientSession):
    try:
        u     = await bot_api('GET', '/api/user',  tg_id=message.from_user.id, session=session)
        games = await bot_api('GET', '/api/games', tg_id=message.from_user.id, session=session)
    except Exception as exc:
        await message.answer(f'⚠️ {exc}'); return
    counts: dict[int, int] = {b: 0 for b in BET_AMOUNTS}
    for gm in games:
        b = gm.get('bet_amount')
        if b in counts and gm.get('status') in ('waiting', 'countdown'):
            counts[b] += gm.get('player_count', 0)
    lobby = '\n'.join(
        f"  {'🟢' if counts[b] else '⚪'} <b>{b} ETB</b>  ({counts[b]} waiting)"
        for b in BET_AMOUNTS
    )
    await message.answer(
        f"🎯 <b>Choose Your Bet</b>\n\n"
        f"💰 Balance: <b>{u['balance']:,} ETB</b>\n\n"
        f"<b>Active lobbies:</b>\n{lobby}\n\n"
        f"<i>Locked bets 🔒 need more balance.</i>",
        reply_markup=kb_bets(u['balance']),
    )

@bot_router.callback_query(F.data.startswith('bet:'))
async def cb_bet(call: CallbackQuery, session: aiohttp.ClientSession):
    await call.answer()
    raw = call.data.split(':', 1)[1]
    if raw == 'cancel':
        await call.message.edit_text('✅ Cancelled.'); return
    amount = int(raw)
    try:
        u = await bot_api('GET', '/api/user', tg_id=call.from_user.id, session=session)
    except Exception as exc:
        await call.message.edit_text(f'⚠️ {exc}'); return
    if u['balance'] < amount:
        await call.message.edit_text(
            f"⚠️ <b>Insufficient balance</b>\n\n"
            f"You have <b>{u['balance']:,} ETB</b> but need <b>{amount} ETB</b>.\n\n"
            "Use 💰 Deposit to top up.",
        ); return
    await call.message.edit_text(
        f"🎲 <b>{amount} ETB Lobby</b>\n\n"
        f"Prize: <b>90% of the pot</b>\n"
        f"Countdown starts when 2+ players join.\n\n"
        f"Tap below to pick your number and join!",
        reply_markup=kb_webapp(f'🎱 Enter {amount} ETB Game', bet=amount),
    )

# 💰 Deposit ──────────────────────────────────────────────────────────────────
@bot_router.message(F.text == '💰 Deposit')
async def cmd_deposit(message: Message, state: FSMContext, session: aiohttp.ClientSession):
    try:
        u = await bot_api('GET', '/api/user', tg_id=message.from_user.id, session=session)
    except Exception as exc:
        await message.answer(f'⚠️ {exc}'); return
    await state.update_data(bb_username=u.get('username', '—'))
    await state.set_state(Deposit.waiting_for_amount)
    first_dep = u.get('deposit_count', 0) == 0
    await message.answer(
        f"💰 <b>Deposit Funds</b>\n\n"
        f"Balance: <b>{u['balance']:,} ETB</b>\n"
        + ("🎁 <b>First deposit? You get 10% bonus!</b>\n" if first_dep else "")
        + f"\nEnter amount (min {MIN_DEPOSIT} · max {MAX_DEPOSIT:,} ETB):",
        reply_markup=kb_cancel(),
    )

@bot_router.message(Deposit.waiting_for_amount, F.text)
async def deposit_amount(message: Message, state: FSMContext):
    if message.text in ('❌ Cancel', '/cancel'):
        await state.clear()
        await message.answer('Deposit cancelled.', reply_markup=kb_main()); return
    try:
        amt = float(message.text.replace(',', '').strip())
    except ValueError:
        await message.answer('⚠️ Enter a valid number.'); return
    if amt < MIN_DEPOSIT:
        await message.answer(f'⚠️ Minimum deposit: {MIN_DEPOSIT} ETB.'); return
    if amt > MAX_DEPOSIT:
        await message.answer(f'⚠️ Maximum deposit: {MAX_DEPOSIT:,} ETB.'); return
    await state.update_data(deposit_amount=amt)
    await state.set_state(Deposit.waiting_for_sms_ref)
    await message.answer(
        f"✅ Amount: <b>{amt:,.0f} ETB</b>\n\n"
        f"📤 <b>Send payment to:</b>\n{PAYMENT_ACCOUNTS}\n\n"
        f"📌 <b>Reference:</b> <code>JB-{message.from_user.id}</code>\n"
        "<i>Include this reference when sending!</i>\n\n"
        "Reply with your <b>transaction reference / SMS code</b>:",
        reply_markup=kb_cancel(),
    )

@bot_router.message(Deposit.waiting_for_sms_ref, F.text)
async def deposit_ref(message: Message, state: FSMContext):
    if message.text in ('❌ Cancel', '/cancel'):
        await state.clear()
        await message.answer('Deposit cancelled.', reply_markup=kb_main()); return
    data = await state.get_data()
    amt         = data['deposit_amount']
    bb_username = data.get('bb_username', '—')
    ref  = message.text.strip()
    await state.clear()
    admin_txt = (
        f"📥 <b>Deposit Request</b>\n\n"
        f"JB Username: <b>{bb_username}</b>\n"
        f"TG: <code>{message.from_user.id}</code> (@{message.from_user.username or '—'})\n"
        f"Amount: <b>{amt:,.0f} ETB</b>\nReference: <code>{ref}</code>\n"
        f"Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC\n\n"
        f"✅ Approve: <code>/admindeposit {message.from_user.id} {amt:.0f}</code>"
    )
    for aid in ADMIN_TG_IDS_INT:
        _notify_queue.put({'tg_id': aid, 'event': 'custom', 'data': {'message': admin_txt}})
    await message.answer(
        f"✅ <b>Deposit request submitted!</b>\n\nAmount: <b>{amt:,.0f} ETB</b>\n"
        "Our team will credit your wallet within <b>~1 hour</b>. 🔔",
        reply_markup=kb_main(),
    )

# 💳 Withdraw ─────────────────────────────────────────────────────────────────
@bot_router.message(F.text == '💳 Withdraw')
async def cmd_withdraw(message: Message, state: FSMContext, session: aiohttp.ClientSession):
    try:
        u = await bot_api('GET', '/api/user', tg_id=message.from_user.id, session=session)
    except Exception as exc:
        await message.answer(f'⚠️ {exc}'); return
    # Check eligibility before even starting FSM
    ok_wd, err_wd = _check_withdrawal_eligibility(u, None)
    if not ok_wd:
        await message.answer(err_wd, reply_markup=kb_main()); return
    if u['balance'] < MIN_WITHDRAW:
        await message.answer(f"⚠️ Min withdrawal: {MIN_WITHDRAW} ETB.\nYour balance: {u['balance']:,} ETB", reply_markup=kb_main()); return
    await state.update_data(bb_username=u.get('username', '—'))
    await state.set_state(Withdrawal.waiting_for_amount)
    await message.answer(
        f"💳 <b>Withdraw Funds</b>\n\nAvailable: <b>{u['balance']:,} ETB</b>\n"
        f"Min: {MIN_WITHDRAW:,} ETB · Processing: ~<b>1 hour</b>\n\nEnter amount:",
        reply_markup=kb_cancel(),
    )

@bot_router.message(Withdrawal.waiting_for_amount, F.text)
async def withdraw_amount(message: Message, state: FSMContext, session: aiohttp.ClientSession):
    if message.text in ('❌ Cancel', '/cancel'):
        await state.clear()
        await message.answer('Withdrawal cancelled.', reply_markup=kb_main()); return
    try:
        amt = float(message.text.replace(',', '').strip())
    except ValueError:
        await message.answer('⚠️ Enter a valid number.'); return
    try:
        u = await bot_api('GET', '/api/user', tg_id=message.from_user.id, session=session)
    except Exception as exc:
        await message.answer(f'⚠️ {exc}'); return
    if amt < MIN_WITHDRAW:
        await message.answer(f'⚠️ Minimum withdrawal is {MIN_WITHDRAW} ETB.'); return
    if amt > u['balance']:
        await message.answer(f"⚠️ Insufficient balance. Available: {u['balance']:,} ETB."); return
    # Final eligibility check including post-withdrawal balance
    ok_wd, err_wd = _check_withdrawal_eligibility(u, amt)
    if not ok_wd:
        await message.answer(err_wd, reply_markup=kb_main()); return
    await state.update_data(withdraw_amount=amt)
    await state.set_state(Withdrawal.waiting_for_bank)
    await message.answer(
        f"✅ Amount: <b>{amt:,.0f} ETB</b>\n\nSelect payment method:",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text='🏦 CBE'), KeyboardButton(text='📱 Telebirr')],
                [KeyboardButton(text='❌ Cancel')],
            ],
            resize_keyboard=True, one_time_keyboard=True,
        )
    )

@bot_router.message(Withdrawal.waiting_for_bank, F.text)
async def withdraw_bank(message: Message, state: FSMContext):
    if message.text in ('❌ Cancel', '/cancel'):
        await state.clear()
        await message.answer('Withdrawal cancelled.', reply_markup=kb_main()); return
    if '🏦' not in message.text and '📱' not in message.text:
        await message.answer('⚠️ Please select CBE or Telebirr.'); return
    bank = 'CBE' if '🏦' in message.text else 'Telebirr'
    await state.update_data(withdraw_bank=bank)
    await state.set_state(Withdrawal.waiting_for_phone)
    await message.answer(
        f"✅ Method: <b>{bank}</b>\n\n"
        f"Enter your {'CBE account number' if bank=='CBE' else 'Telebirr phone number'}:",
        reply_markup=kb_cancel(),
    )

@bot_router.message(Withdrawal.waiting_for_phone, F.text)
async def withdraw_phone(message: Message, state: FSMContext):
    if message.text in ('❌ Cancel', '/cancel'):
        await state.clear()
        await message.answer('Withdrawal cancelled.', reply_markup=kb_main()); return
    await state.update_data(withdraw_phone=message.text.strip())
    await state.set_state(Withdrawal.waiting_for_account_name)
    await message.answer(
        "Enter the <b>account holder name</b> (full name as registered on the account):",
        reply_markup=kb_cancel(),
    )

@bot_router.message(Withdrawal.waiting_for_account_name, F.text)
async def withdraw_account_name(message: Message, state: FSMContext):
    if message.text in ('❌ Cancel', '/cancel'):
        await state.clear()
        await message.answer('Withdrawal cancelled.', reply_markup=kb_main()); return
    data    = await state.get_data()
    amt     = data['withdraw_amount']
    bank    = data.get('withdraw_bank', '—')
    phone   = data.get('withdraw_phone', '—')
    aname   = message.text.strip()
    bb_username = data.get('bb_username', '—')
    await state.clear()
    admin_txt = (
        f"💸 <b>Withdrawal Request</b>\n\n"
        f"JB Username: <b>{bb_username}</b>\n"
        f"TG: <code>{message.from_user.id}</code> (@{message.from_user.username or '—'})\n"
        f"Amount: <b>{amt:,.0f} ETB</b>\n"
        f"Method: <b>{bank}</b>\n"
        f"Account: <code>{phone}</code> ({aname})\n"
        f"Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC\n\n"
        f"✅ Approve: <code>/adminwithdraw {message.from_user.id} {amt:.0f}</code>"
    )
    # Record the request for daily limit tracking
    import hashlib as _hbotwd
    wd_ref = _hbotwd.md5(f"wdreq:{message.from_user.id}:{int(amt)}:{int(time.time())//86400}".encode()).hexdigest()[:16]
    with StandaloneDB() as sdb:
        try:
            sdb.run('INSERT INTO transactions (tg_id,type,amount,ref,note) VALUES (?,?,?,?,?)',
                    (str(message.from_user.id), 'withdraw_request', int(amt), wd_ref, 'bot'))
        except Exception: pass
    for aid in ADMIN_TG_IDS_INT:
        sent = _tg_notify_direct(aid, admin_txt)
        if not sent:
            _notify_queue.put({'tg_id': aid, 'event': 'custom', 'data': {'message': admin_txt}})
    await message.answer(
        f"⏳ <b>Withdrawal request received!</b>\n\nAmount: <b>{amt:,.0f} ETB</b>\n"
        f"Method: <b>{bank}</b> · {phone}\n"
        "You'll be notified once your request is approved. Please wait. 🔔",
        reply_markup=kb_main(),
    )

# 🆘 Support ──────────────────────────────────────────────────────────────────
@bot_router.message(Command('transactions'))
async def cmd_transactions(message: Message, session: aiohttp.ClientSession):
    """Show last 10 deposit/withdraw transactions for the user."""
    tg_id = str(message.from_user.id)
    with StandaloneDB() as sdb:
        rows = sdb.query(
            "SELECT type, amount, created_at FROM transactions WHERE tg_id=? ORDER BY created_at DESC LIMIT 10",
            (tg_id,)
        )
    if not rows:
        await message.answer('📋 No transactions yet.\n\nDeposit or withdraw to see your history.'); return
    lines = []
    for r in rows:
        icon  = '💰' if r['type'] == 'deposit' else '💸'
        sign  = '+' if r['type'] == 'deposit' else '−'
        date  = str(r['created_at'])[:10]
        lines.append(f"{icon} <b>{sign}{r['amount']:,} ETB</b> ({r['type'].title()}) · {date}")
    await message.answer('📋 <b>Transaction History (last 10)</b>\n\n' + '\n'.join(lines))

@bot_router.message(F.text == '🆘 Support')
async def cmd_support(message: Message):
    await message.answer(
        '🆘 <b>Contact Support</b>\n\n'
        'For help with deposits, withdrawals, or any issue:\n\n'
        '👉 @Tewodros1888\n\n'
        'We typically respond within 1–2 hours.',
        reply_markup=kb_main(),
    )

# 📨 Invite Friends ────────────────────────────────────────────────────────────
@bot_router.message(F.text == '📨 Invite Friends')
async def cmd_invite(message: Message, session: aiohttp.ClientSession):
    link = f'https://t.me/{_bot_username}?start={message.from_user.id}'
    try:
        stats = await bot_api('GET', '/api/referral-stats', tg_id=message.from_user.id, session=session)
    except Exception:
        stats = {'referral_count': 0, 'next_bonus_at': REFERRALS_PER_BONUS, 'friends': []}
    rc      = stats.get('referral_count', 0)
    nxt     = stats.get('next_bonus_at', REFERRALS_PER_BONUS)
    friends = stats.get('friends', [])
    recent  = '\n'.join(f"  👤 {f['username']}" for f in friends[:5]) or '  <i>None yet</i>'
    await message.answer(
        f'📨 <b>Invite Friends & Earn!</b>\n\n'
        f'🔗 Your referral link:\n<code>{link}</code>\n\n'
        f'👥 Friends joined: <b>{rc}</b>\n'
        f'🎁 Next bonus at: <b>{nxt} referrals</b> (+{REFERRAL_BONUS} ETB)\n\n'
        f'<b>Recent friends:</b>\n{recent}\n\n'
        f'Share your link → friends register → you earn <b>{REFERRAL_BONUS} ETB</b> every {REFERRALS_PER_BONUS} signups!\n\n'        '🎁 <b>New players get 10% bonus on first deposit!</b>\n\n'        '🇪🇹 <i>በ 10 ብር ቦነስ ጀምሩ! ለመጀመሪያ ዲፖዚት 10% ቦነስ! ጓደኛ ሲጋብዙ ሌላ ቦነስ! የጋበዙት ጓደኛ ዲፖዚት ሲያደርግ ለናንተ ቦነስ! ይጫወቱ! ያሸንፉ!</i>',
        reply_markup=kb_main(),
    )

# Admin commands ──────────────────────────────────────────────────────────────
@bot_router.message(Command('admin'))
@admin_guard
async def cmd_admin(message: Message, session: aiohttp.ClientSession):
    try:
        s = await bot_api('GET', '/api/admin/stats', tg_id=message.from_user.id, session=session)
    except Exception as exc:
        await message.answer(f'⚠️ {exc}'); return
    await message.answer(
        f"⚙️ <b>Admin Dashboard</b>\n\n"
        f"👥 Users: {s['total_users']}  🎮 Games: {s['total_games']}\n"
        f"🔴 Active: {s['active_games']}  ✅ Finished: {s['finished_games']}\n"
        f"💰 Prizes paid: {s['total_prizes']:,} ETB\n\n"
        f"<b>Commands:</b>\n"
        f"/adminstats — live stats\n"
        f"/admindeposit &lt;tg_id&gt; &lt;amount&gt;\n"
        f"/adminwithdraw &lt;tg_id&gt; &lt;amount&gt;\n"
        f"/adminban &lt;tg_id&gt; — ban user\n"
        f"/adminunban &lt;tg_id&gt; — unban user\n"
        f"/adminend &lt;game_id&gt;\n"
        f"/adminmsg &lt;tg_id&gt; &lt;text&gt;\n"
        f"/adminbroadcast &lt;text&gt;",
    )

@bot_router.message(Command('admindeposit'))
@admin_guard
async def cmd_adep(message: Message, session: aiohttp.ClientSession):
    parts = message.text.split(maxsplit=2)
    # Show usage hint with tg_id → username lookup
    if len(parts) == 2 and parts[1].isdigit():
        try:
            row = await bot_api('GET', f'/api/admin/user-by-tgid/{parts[1]}',
                                tg_id=message.from_user.id, session=session)
            await message.answer(
                f"👤 TG <code>{parts[1]}</code> = <b>{row['username']}</b> "
                f"(balance: {row['balance']:,} ETB)\n\n"
                f"Usage: <code>/admindeposit {parts[1]} &lt;amount&gt;</code>"
            )
        except Exception:
            await message.answer('Usage: /admindeposit &lt;tg_id&gt; &lt;amount&gt;')
        return
    if len(parts) < 3 or not parts[1].isdigit():
        await message.answer('Usage: /admindeposit &lt;tg_id&gt; &lt;amount&gt;\n'
                             'Tip: /admindeposit &lt;tg_id&gt; to look up username first'); return
    tid, amt = int(parts[1]), float(parts[2])
    try:
        target = query_db('SELECT * FROM users WHERE tg_id=?', (str(tid),), one=True)
        if not target: await message.answer('⚠️ User not found.'); return
        # Idempotency: build a ref from admin_tg + user_tg + amount + minute-bucket
        import hashlib as _hl
        ref_raw = f"dep:{tid}:{int(amt)}:{int(time.time())//300}"
        tx_ref  = _hl.md5(ref_raw.encode()).hexdigest()[:16]
        # Check if this deposit was already processed (StandaloneDB — no Flask context)
        with StandaloneDB() as sdb:
            existing_tx = sdb.one('SELECT id FROM transactions WHERE ref=?', (tx_ref,))
        if existing_tx:
            await message.answer('⚠️ This deposit appears to have already been processed (duplicate).'); return
        res = await bot_api('POST', '/api/admin/funds', tg_id=message.from_user.id,
                            body={'username': target['username'], 'amount': int(amt)}, session=session)
        # Record transaction (StandaloneDB — no Flask context in bot thread)
        with StandaloneDB() as sdb:
            sdb.run('INSERT INTO transactions (tg_id, type, amount, ref, note) VALUES (?,?,?,?,?)',
                    (str(tid), 'deposit', int(amt), tx_ref, f'admin:{message.from_user.id}'))
        bonus_res = await bot_api('POST', '/api/deposit-complete', tg_id=message.from_user.id,
                                  body={'tg_id': str(tid), 'amount': int(amt)}, session=session)
        bonus     = bonus_res.get('bonus', 0)
        final_bal = bonus_res.get('new_balance', res['new_balance'])
        bonus_note = f"\n🎁 First deposit bonus: +{bonus} ETB!" if bonus else ""
        await message.answer(f"✅ +{amt:,.0f} ETB → <b>{target['username']}</b>{bonus_note}\nBalance: {final_bal:,} ETB")
        notify_amt = amt + bonus
        _notify_queue.put({'tg_id': tid, 'event': 'deposit_approved', 'data': {'amount': notify_amt, 'new_balance': final_bal}})
    except Exception as exc:
        await message.answer(f'⚠️ {exc}')

@bot_router.message(Command('adminwithdraw'))
@admin_guard
async def cmd_awdraw(message: Message, session: aiohttp.ClientSession):
    parts = message.text.split(maxsplit=2)
    if len(parts) == 2 and parts[1].isdigit():
        try:
            row = await bot_api('GET', f'/api/admin/user-by-tgid/{parts[1]}',
                                tg_id=message.from_user.id, session=session)
            await message.answer(
                f"👤 TG <code>{parts[1]}</code> = <b>{row['username']}</b> "
                f"(balance: {row['balance']:,} ETB)\n\n"
                f"Usage: <code>/adminwithdraw {parts[1]} &lt;amount&gt;</code>"
            )
        except Exception:
            await message.answer('Usage: /adminwithdraw &lt;tg_id&gt; &lt;amount&gt;')
        return
    if len(parts) < 3 or not parts[1].isdigit():
        await message.answer('Usage: /adminwithdraw &lt;tg_id&gt; &lt;amount&gt;\n'
                             'Tip: /adminwithdraw &lt;tg_id&gt; to look up username first'); return
    tid, amt = int(parts[1]), float(parts[2])
    try:
        target = query_db('SELECT * FROM users WHERE tg_id=?', (str(tid),), one=True)
        if not target: await message.answer('⚠️ User not found.'); return
        res = await bot_api('POST', '/api/admin/funds', tg_id=message.from_user.id,
                            body={'username': target['username'], 'amount': -int(amt)}, session=session)
        # Record in transactions table (StandaloneDB — no Flask context in bot thread)
        import hashlib as _hl2
        wr_ref = _hl2.md5(f"wd:{tid}:{int(amt)}:{int(time.time())//300}".encode()).hexdigest()[:16]
        with StandaloneDB() as sdb:
            try:
                sdb.run('INSERT INTO transactions (tg_id,type,amount,ref,note) VALUES (?,?,?,?,?)',
                        (str(tid), 'withdraw', int(amt), wr_ref, f'admin:{message.from_user.id}'))
            except Exception: pass  # ignore duplicate
        await message.answer(f"✅ Withdrawal {amt:,.0f} ETB confirmed for <b>{target['username']}</b>.\nBalance: {res['new_balance']:,} ETB")
        _notify_queue.put({'tg_id': tid, 'event': 'withdraw_approved',
                          'data': {'amount': int(amt), 'new_balance': res['new_balance']}})
    except Exception as exc:
        await message.answer(f'⚠️ {exc}')

@bot_router.message(Command('adminstats'))
@admin_guard
async def cmd_astats(message: Message, session: aiohttp.ClientSession):
    try:
        s = await bot_api('GET', '/api/admin/stats', tg_id=message.from_user.id, session=session)
    except Exception as exc:
        await message.answer(f'⚠️ {exc}'); return
    await message.answer(
        f"📊 <b>Admin Stats</b>\n\n"
        f"👥 Total users:    {s['total_users']}\n"
        f"🎮 Total games:    {s['total_games']}\n"
        f"🔴 Active games:   {s['active_games']}\n"
        f"✅ Finished games: {s['finished_games']}\n"
        f"💰 Prizes paid:    {s['total_prizes']:,} ETB"
    )

@bot_router.message(Command('adminban'))
@admin_guard
async def cmd_aban(message: Message, session: aiohttp.ClientSession):
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip().isdigit():
        await message.answer('Usage: /adminban &lt;tg_id&gt;'); return
    tgid = parts[1].strip()
    try:
        res = await bot_api('POST', f'/api/admin/users/{tgid}/ban',
                            tg_id=message.from_user.id, body={'ban': True}, session=session)
        await message.answer(f"🚫 <b>{res['username']}</b> has been banned.")
    except Exception as exc:
        await message.answer(f'⚠️ {exc}')

@bot_router.message(Command('adminunban'))
@admin_guard
async def cmd_aunban(message: Message, session: aiohttp.ClientSession):
    parts = message.text.split(maxskip=1)
    if len(parts) < 2 or not parts[1].strip().isdigit():
        await message.answer('Usage: /adminunban &lt;tg_id&gt;'); return
    tgid = parts[1].strip()
    try:
        res = await bot_api('POST', f'/api/admin/users/{tgid}/ban',
                            tg_id=message.from_user.id, body={'ban': False}, session=session)
        await message.answer(f"✅ <b>{res['username']}</b> has been unbanned.")
    except Exception as exc:
        await message.answer(f'⚠️ {exc}')

@bot_router.message(Command('adminend'))
@admin_guard
async def cmd_aend(message: Message, session: aiohttp.ClientSession):
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2: await message.answer('Usage: /adminend &lt;game_id&gt;'); return
    try:
        res = await bot_api('POST', f'/api/admin/games/{parts[1].strip()}/finish',
                            tg_id=message.from_user.id, session=session)
        await message.answer(f"✅ Game ended. {res.get('refunded', 0)} player(s) refunded.")
    except Exception as exc:
        await message.answer(f'⚠️ {exc}')

@bot_router.message(Command('adminmsg'))
@admin_guard
async def cmd_amsg(message: Message):
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3 or not parts[1].isdigit():
        await message.answer('Usage: /adminmsg &lt;tg_id&gt; &lt;text&gt;'); return
    _notify_queue.put({'tg_id': int(parts[1]), 'event': 'custom',
                       'data': {'message': f"📢 <b>Admin message:</b>\n\n{parts[2]}"}})
    await message.answer('✅ Message queued.')

@bot_router.message(Command('adminbroadcast'))
@admin_guard
async def cmd_abcast(message: Message, session: aiohttp.ClientSession):
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer('Usage: /adminbroadcast &lt;text&gt;'); return
    # Use StandaloneDB to avoid app context issues
    with StandaloneDB() as sdb:
        all_users = sdb.query('SELECT tg_id FROM users WHERE is_banned=0 ORDER BY id')
    for u in all_users:
        _notify_queue.put({'tg_id': int(u['tg_id']), 'event': 'custom',
                           'data': {'message': f"📣 <b>Announcement</b>\n\n{parts[1]}"}})
    await message.answer(f"📣 Broadcast queued for {len(all_users)} users.")

# ── Notification drain task ───────────────────────────────────────────────────
async def _drain_notify_queue(bot: Bot):
    """
    Runs as an asyncio task inside the bot's event loop.
    Drains the thread-safe queue that Flask route handlers write to.
    """
    event_map = {
        'win':               lambda d: f"🏆 <b>BINGO! You won!</b>\n\n+{d.get('prize',0):,} ETB added to your wallet! 🎱",
        '_last_standing':    lambda d: f"🥇 <b>You're the last one standing!</b>\n\nAll other players left — the pot is yours!\n+{d.get('prize',0):,} ETB added to your wallet! 🎉",
        'player_forfeited':  lambda d: f"⚠️ <b>A player forfeited!</b>\n\nGame continues with {d.get('remaining',1)} player{'s' if d.get('remaining',1)!=1 else ''} remaining. Keep marking! 🎱",
        'game_start':        lambda d: '🔔 <b>Your game is starting!</b>\nOpen the app and mark your card!',
        'deposit_approved':  lambda d: f"✅ <b>Deposit Approved!</b>\n\n<b>{d.get('amount',0):,.0f} ETB</b> added to your wallet.\nNew balance: <b>{d.get('new_balance',0):,} ETB</b>",
        'withdraw_approved': lambda d: f"✅ <b>Withdrawal Processed!</b>\n\n<b>{d.get('amount',0):,.0f} ETB</b> sent to your account.\nNew balance: <b>{d.get('new_balance',0):,} ETB</b>",
        'custom':            lambda d: d.get('message', ''),
    }
    while True:
        await asyncio.sleep(0.25)
        while not _notify_queue.empty():
            try:
                item  = _notify_queue.get_nowait()
                tg_id = item['tg_id']
                event = item['event']
                data  = item['data']
                fn    = event_map.get(event)
                if fn:
                    text = fn(data)
                    if text:
                        await bot_notify(bot, tg_id, text)
            except Exception as exc:
                logger.warning(f'[notify_drain] {exc}')

# ── aiohttp middleware to inject shared session ───────────────────────────────
async def _session_middleware(handler, event, data):
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=10)
    ) as session:
        data['session'] = session
        return await handler(event, data)

# ── Bot main coroutine ────────────────────────────────────────────────────────
async def _bot_main():
    global _bot_username
    if not BOT_TOKEN:
        logger.warning('[Bot] No BOT_TOKEN set — bot will not start.')
        return

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp  = Dispatcher(storage=MemoryStorage())

    dp.update.middleware(_session_middleware)
    dp.include_router(bot_router)

    me = await bot.get_me()
    _bot_username = me.username
    logger.info(f'[Bot] @{_bot_username} started (polling)')

    # Delete any stale webhook before polling
    await bot.delete_webhook(drop_pending_updates=True)
    await asyncio.sleep(2)

    asyncio.create_task(_drain_notify_queue(bot))

    try:
        await dp.start_polling(
            bot,
            allowed_updates=dp.resolve_used_update_types(),
            close_bot_session=True,
            handle_signals=False,
        )
    finally:
        await bot.session.close()
        logger.info('[Bot] Polling stopped.')

# ── Thread launcher (called once at module load) ──────────────────────────────
_bot_thread_started = False

def start_bot_thread():
    """
    Starts the aiogram bot in a dedicated daemon thread with its own event loop.
    Called once at module load — Gunicorn --workers 1 ensures this happens exactly once.
    """
    global _bot_thread_started
    if _bot_thread_started:
        return
    _bot_thread_started = True

    def _run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        # Suppress the harmless "Task was destroyed but pending" warning
        # that appears when Gunicorn shuts down the daemon thread mid-poll.
        loop.set_exception_handler(lambda l, ctx: (
            None if 'Task was destroyed' in ctx.get('message', '')
            else l.default_exception_handler(ctx)
        ))
        try:
            loop.run_until_complete(_bot_main())
        except Exception as exc:
            logger.error(f'[Bot] Fatal: {exc}')
        finally:
            # Cancel remaining tasks cleanly before closing
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.close()

    t = threading.Thread(target=_run, daemon=True, name='TelegramBot')
    t.start()
    logger.info('[Bot] Daemon thread launched.')

# ── Runs on import by Gunicorn AND by direct `python app.py` ─────────────────
# IMPORTANT: init_db() can block for several seconds on a cold PostgreSQL
# connection. Running it in the main thread delays Gunicorn from binding to
# its port, causing Render's port scanner to time out ("No open HTTP ports").
# Solution: defer ALL startup into a background thread so Gunicorn binds
# immediately, Render detects the port, and DB + bot start in the background.
def _deferred_startup():
    # Pool is now lazy — no need to sleep. Gunicorn binds immediately at import.
    # Give it 2s anyway so the first HTTP request isn't racing with DB init.
    time.sleep(2)
    init_db()              # create/migrate tables
    GameEngine()           # start number-calling background thread
    start_bot_thread()     # start telegram bot daemon thread

threading.Thread(target=_deferred_startup, daemon=True, name='Startup').start()

# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT  (python app.py — local dev only)
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    print(f'[Joker Bingo] http://0.0.0.0:{port}  DB={DB_TYPE}')
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)
