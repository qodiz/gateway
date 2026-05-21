FROM python:3.11-slim

WORKDIR /app

# Установка зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование исходного кода
COPY main.py .

# Порт FastAPI
EXPOSE 8000

# Запуск приложения
CMD ["python", "main.py"]