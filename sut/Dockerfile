FROM python:3.12-slim
# iproute2 (tc netem) and iptables let the harness inject delay, loss and partitions inside the container
RUN apt-get update && apt-get install -y --no-install-recommends iproute2 iptables \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY cluster.py .
ENV PYTHONUNBUFFERED=1
