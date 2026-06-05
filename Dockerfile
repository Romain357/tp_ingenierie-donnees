FROM python:3.10-slim
WORKDIR /app
COPY python/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "python/main.py"]