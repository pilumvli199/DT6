# Railway deployment for Dhan WebSocket -> Telegram bot


This package is prepared to be deployed to Railway.app. You can either deploy by linking your GitHub repo or by using Docker.

Steps (GitHub deploy):
1. Create a new project on Railway and connect your GitHub repo (or push these files to a new repo).
2. In Railway project settings -> Variables, add:
   - DHAN_CLIENT_ID
   - DHAN_ACCESS_TOKEN
   - TELEGRAM_BOT_TOKEN
   - TELEGRAM_CHAT_ID
   - WS_URL (e.g. wss://...)
   - SYMBOLS (comma-separated)
3. Deploy. Railway will run the `web` process defined in `Procfile`.

Steps (Docker deploy):
1. Build: `docker build -t dhan_bot .`
2. Run: `docker run --env DHAN_CLIENT_ID=... --env ... dhan_bot`

Notes:
- For long-running background processes, Railway's `web` process is acceptable. If you prefer to mark it as a worker, update Procfile accordingly.
- Never commit your secrets to a public repository; always use Railway environment variables.
