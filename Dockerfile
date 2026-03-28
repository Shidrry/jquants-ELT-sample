FROM python:3.11-slim

WORKDIR /app

# 依存があるなら requirements を先に入れる
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# アプリ本体
COPY . /app

# Cloud Run Job は deploy 時に command/args を渡すので entrypoint は固定でOK
ENTRYPOINT ["python"]

# app配下にpathを通す
ENV PYTHONPATH=/app