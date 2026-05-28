# Dremio OSS image — coordinator and executor are the same binary;
# role is determined by the dremio.conf injected via ConfigMap.
FROM eclipse-temurin:11-jre-jammy

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends less procps curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system dremio --gid 999 \
    && useradd --system --uid 999 --gid dremio --home /var/lib/dremio dremio \
    && mkdir -p /opt/dremio /opt/dremio/data /opt/dremio/conf /var/run/dremio /var/log/dremio \
    && chown -R dremio:dremio /opt/dremio /var/run/dremio /var/log/dremio

COPY dremio-distribution.tar.gz /tmp/
RUN tar xfz /tmp/dremio-distribution.tar.gz -C /opt/dremio --strip-components=1 \
    && rm /tmp/dremio-distribution.tar.gz

EXPOSE 9047/tcp 31010/tcp 32010/tcp 45678/tcp

USER dremio
WORKDIR /opt/dremio
ENV DREMIO_HOME=/opt/dremio
ENV DREMIO_PID_DIR=/var/run/dremio
ENV DREMIO_GC_LOGS_ENABLED="yes"
ENV DREMIO_GC_LOG_TO_CONSOLE="yes"
ENV DREMIO_LOG_DIR="/var/log/dremio"
ENTRYPOINT ["bin/dremio", "start-fg"]
