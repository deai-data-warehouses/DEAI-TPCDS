FROM python:3.13-slim-trixie

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 \
    PATH="/usr/lib/jvm/java-21-openjdk-amd64/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SPARK_LOCAL_IP=127.0.0.1 \
    SPARK_LOCAL_HOSTNAME=localhost

RUN sed -i 's|http://deb.debian.org|https://deb.debian.org|g' /etc/apt/sources.list.d/debian.sources \
    && apt-get update -o Acquire::Retries=3 \
    && apt-get install -y -o Acquire::Retries=3 --no-install-recommends \
        gcc \
        libc6-dev \
        make \
        bison \
        flex \
        openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN python -m pip install --no-cache-dir \
    dotenv==0.9.9 \
    jinja2==3.1.6 \
    openpyxl==3.1.5 \
    pydantic==2.14.1 \
    pyspark==4.2.0

COPY pipeline ./pipeline
COPY config ./config
COPY scripts ./scripts

# No default stage: each one is run explicitly, e.g.
# `docker compose run --rm pipeline python pipeline/prepare.py --scale-factor 1`
CMD ["echo", "Run a stage: python pipeline/{prepare,convert,benchmark}.py --scale-factor N (see README)"]
