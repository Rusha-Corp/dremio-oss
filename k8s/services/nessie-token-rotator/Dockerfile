FROM python:3.12-slim

WORKDIR /app

COPY rotate_token.py .

RUN pip install --no-cache-dir requests

ENTRYPOINT ["python", "/app/rotate_token.py"]
