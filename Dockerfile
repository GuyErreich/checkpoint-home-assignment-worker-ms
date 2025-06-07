FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt  requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements.txt -r requirements-dev.txt

COPY src/worker/ ./

CMD ["python", "main.py"]