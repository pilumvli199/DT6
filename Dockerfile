# Dockerfile for Railway (simple Python app)
FROM python:3.11-slim
WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir -r requirements.txt
# Railway sets environment variables via project settings; expose no ports for a worker style app
CMD ["python", "dhan_websocket_bot.py"]
