FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8765

CMD gunicorn app:app --bind 0.0.0.0:${PORT:-8765} --workers 1 --threads 4 --timeout 300 --preload
