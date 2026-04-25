FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY server.py .
COPY data/ ./data/

ENV PYTHONUNBUFFERED=1
ENV MCP_TRANSPORT=stdio

EXPOSE 8000

ENTRYPOINT ["python", "server.py"]
