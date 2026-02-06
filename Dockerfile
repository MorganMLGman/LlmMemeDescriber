FROM dhi.io/python:3.14-debian13-dev AS builder
ARG TARGETARCH
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential pkg-config gcc libffi-dev libssl-dev wget xz-utils ca-certificates rustc cargo \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN python3 -m pip install --upgrade pip setuptools wheel

COPY Pipfile Pipfile.lock /app/
RUN python3 -m pip install --no-cache-dir pipenv \
  && cd /app \
  && PIPENV_VENV_IN_PROJECT=1 pipenv install --deploy --ignore-pipfile \
  && rm -rf /root/.cache /root/.local /tmp/* /var/tmp/*

RUN set -e; \
    ARCH=$([ "$TARGETARCH" = "arm64" ] && echo "arm64" || echo "amd64") && \
    cd /tmp && \
    wget "https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-${ARCH}-static.tar.xz" && \
    wget "https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-${ARCH}-static.tar.xz.md5" && \
    md5sum -c "ffmpeg-release-${ARCH}-static.tar.xz.md5" && \
    mkdir -p /tmp/ffmpeg-static && \
    tar -C /tmp/ffmpeg-static --strip-components=1 -xf "ffmpeg-release-${ARCH}-static.tar.xz" && \
    mv /tmp/ffmpeg-static/ffmpeg /usr/bin/ffmpeg && \
    mv /tmp/ffmpeg-static/ffprobe /usr/bin/ffprobe && \
    rm -rf /tmp/ffmpeg* && \
    chmod +x /usr/bin/ffmpeg /usr/bin/ffprobe

RUN mkdir -p /data && chmod 755 /data

FROM dhi.io/python:3.14-debian13 AS production
WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /usr/bin/ffmpeg /usr/bin/ffmpeg
COPY --from=builder /usr/bin/ffprobe /usr/bin/ffprobe
COPY --from=builder /data /data
VOLUME ["/data"]

ENV PYTHONUNBUFFERED=1
ENV LOGGING_LEVEL=INFO
ENV GOOGLE_GENAI_MODEL=gemini-3-flash-preview
ENV PATH=/app/.venv/bin:$PATH

COPY llm_memedescriber /app/llm_memedescriber
COPY PROMPT.txt /app/
COPY --chmod=0755 entrypoint.py /app/entrypoint.py
COPY --chmod=0755 healthcheck.py /app/healthcheck.py

EXPOSE 8443

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD ["/app/.venv/bin/python", "/app/healthcheck.py"]

ENTRYPOINT ["/app/.venv/bin/python", "/app/entrypoint.py"]
CMD ["python", "-m", "uvicorn", "llm_memedescriber.app:app", "--host", "0.0.0.0", "--port", "8443", "--ssl-certfile=/data/certs/server.crt", "--ssl-keyfile=/data/certs/server.key", "--log-level", "info", "--no-access-log"]
