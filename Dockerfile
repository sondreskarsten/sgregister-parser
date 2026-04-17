FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY flatten.py cdc.py parse.py ./

ENV STATE_DIR=/data
CMD ["python3", "-u", "parse.py"]
