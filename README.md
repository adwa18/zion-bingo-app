# 🎱 Benz Bingo — Telegram WebApp

Full-stack Telegram Bingo game with Flask backend, SQLite database, and a complete Telegram WebApp frontend.

---

## Project Structure

```
benz-bingo/
├── app.py              ← Flask backend (API + game engine)
├── bot.py              ← Telegram bot (inline WebApp launcher)
├── requirements.txt    ← Python dependencies
├── static/
│   └── index.html      ← Complete Telegram WebApp frontend
└── README.md
```

---

## Features

### Backend (app.py)
- **SQLite** database — `users`, `bingo_games`, `bingo_players`, `game_history`
- **Background GameEngine thread** — drives countdown → active → number calling lifecycle
- **Full game lifecycle**: `waiting → countdown → active → finished`
- **Wallet** deduction on join, prize award (90% pot) on BINGO claim
- **Telegram initData HMAC verification** for auth security
- **Admin routes**: stats, adjust funds, force-finish games, list users/games

### Frontend (static/index.html)
- **Welcome screen** — animated logo, Telegram greeting
- **Register** — username setup with 100 ETB welcome bonus
- **Lobby** — 4 bet options (10/50/100/200 ETB), live player counts, active game list with auto-refresh
- **Number picker** — 10×10 grid (1–100), taken numbers shown in red
- **Card preview** — 5×5 BINGO card (B/I/N/G/O columns, FREE center), picked number highlighted blue
- **Game screen** — split layout:
  - Top half: countdown timer / live called numbers display / last-called highlighted
  - Bottom half: interactive BINGO card with tap-to-mark
- **BINGO detection** — row, column, diagonal (both diagonals)
- **Claim button** — auto-shows when BINGO detected
- **Admin panel** — stats dashboard, wallet management, game management
- **Game history** — personal log with wins/losses
- **Leaderboard** — top 20 by wins
- **Dark/Light theme toggle** (bottom-right footer)
- **Design** — Orbitron + Exo 2 fonts, dark gold/amber theme, confetti win animation

---

## Quick Start (Development)

### 1. Install dependencies
```bash
cd benz-bingo
pip install flask flask-cors
```

### 2. Run the backend
```bash
python app.py
```

Server starts at `http://localhost:5000`

### 3. Open the frontend
Navigate to `http://localhost:5000` in your browser.  
In dev mode, a fake Telegram user is generated automatically.

---

## Production Deployment

### Step 1: Set up your server (e.g. Ubuntu VPS)

```bash
pip install flask flask-cors gunicorn python-telegram-bot
```

### Step 2: Configure environment variables

```bash
export BOT_TOKEN="your_telegram_bot_token_here"
export ADMIN_TG_IDS="123456789,987654321"   # comma-separated Telegram user IDs for admins
export PORT=5000
```

### Step 3: Run with Gunicorn

```bash
gunicorn --bind 0.0.0.0:5000 --workers 2 --threads 4 app:app
```

### Step 4: Set up Nginx reverse proxy

```nginx
server {
    listen 80;
    server_name your-domain.com;

    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

### Step 5: Enable HTTPS (required for Telegram WebApp!)

```bash
sudo certbot --nginx -d your-domain.com
```

### Step 6: Update frontend API_BASE

In `static/index.html`, the `API_BASE` auto-detects the origin.  
If backend and frontend are on different URLs, update line:
```js
const API_BASE = 'https://your-backend-domain.com';
```

### Step 7: Start the Telegram bot

```bash
export BOT_TOKEN="your_token"
export WEBAPP_URL="https://your-domain.com"
python bot.py
```

---

## Telegram Bot Setup

1. Message [@BotFather](https://t.me/BotFather) on Telegram
2. Create a new bot: `/newbot`
3. Copy the **BOT_TOKEN**
4. Enable inline mode: `/setinline` → choose your bot → set placeholder text
5. Set bot commands:
   ```
   /setcommands → /start - Open Benz Bingo | /help - How to play
   ```
6. Create a WebApp button via `/newapp` and set your HTTPS URL

---

## Making Yourself Admin

Add your Telegram numeric user ID to the `ADMIN_TG_IDS` environment variable before starting the server. Your user ID can be found via [@userinfobot](https://t.me/userinfobot).

```bash
export ADMIN_TG_IDS="123456789"
```

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/auth` | Check if Telegram user exists |
| POST | `/api/register` | Register new user |
| GET  | `/api/user` | Get current user data |
| GET  | `/api/games` | List active games |
| POST | `/api/games` | Create new game |
| GET  | `/api/games/<id>` | Get game state + called numbers |
| POST | `/api/games/<id>/join` | Join game + generate card |
| POST | `/api/games/<id>/mark` | Mark a cell on your card |
| POST | `/api/games/<id>/claim` | Claim BINGO (server verifies) |
| GET  | `/api/history` | Personal game history |
| GET  | `/api/leaderboard` | Top 20 players |
| GET  | `/api/admin/stats` | Admin: global stats |
| GET  | `/api/admin/users` | Admin: all users |
| GET  | `/api/admin/games` | Admin: all games |
| POST | `/api/admin/funds` | Admin: adjust user wallet |
| POST | `/api/admin/games/<id>/finish` | Admin: force-finish + refund |

---

## Game Rules

- Select a bet amount (10, 50, 100, or 200 ETB)
- Pick a lucky number (1–100) — no two players can pick the same number in a game
- You receive a random 5×5 BINGO card (B=1-20, I=21-40, N=41-60, G=61-80, O=81-100)
- The center square (N3) is always FREE
- Countdown of **2 minutes** starts once **2+ players** join
- Numbers are called every **5 seconds** once the game is active
- Tap called numbers on your card to mark them
- Complete a full **row, column, or diagonal** → press **BINGO!**
- Server verifies your card server-side
- Winner receives **90% of the total pot** (10% house cut)

---

## Tech Stack

- **Backend**: Python 3.11+, Flask 3, Flask-CORS, SQLite3
- **Bot**: python-telegram-bot 21
- **Frontend**: Vanilla HTML/CSS/JS (no frameworks)
- **Fonts**: Orbitron + Exo 2 (Google Fonts)
- **Deployment**: Gunicorn + Nginx + Let's Encrypt
