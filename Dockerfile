ARG DYNAMODB_LOCAL_VERSION=2.5.0

FROM amazoncorretto:17-al2023

ARG DYNAMODB_LOCAL_VERSION

RUN dnf install -y --allowerasing python3.11 python3.11-pip shadow-utils curl tar gzip && \
    dnf clean all

RUN ln -sf /usr/bin/python3.11 /usr/bin/python3

RUN python3 -m pip install boto3 cfddb supervisor pytest moto[dynamodb] rich

RUN mkdir -p /dynamodb && \
    curl -L "https://d1ni2b6xgvw0s0.cloudfront.net/v2.x/dynamodb_local_latest.tar.gz" \
    | tar -xz -C /dynamodb

RUN mkdir -p /data /tmp/dlq

WORKDIR /app
COPY entrypoint.sh init.py replicator.py dlq_replay.py supervisord.conf ./
RUN chmod +x entrypoint.sh

HEALTHCHECK \
  --interval=5s \
  --timeout=3s \
  --start-period=60s \
  --retries=3 \
  CMD curl -sf http://localhost:8099/health | python3 -c \
      "import sys,json; d=json.load(sys.stdin); sys.exit(0 if d['status']=='ready' else 1)"

ENTRYPOINT ["./entrypoint.sh"]
