FROM python:3.13-slim-trixie

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 \
    PATH="/usr/lib/jvm/java-21-openjdk-amd64/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir \
    dotenv==0.9.9 \
    "openpyxl>=3.1.5" \
    pyspark==4.2.0

COPY pipeline ./pipeline

COPY scripts ./scripts

CMD ["python", "pipeline/main.py"]
