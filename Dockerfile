FROM --platform=$BUILDPLATFORM python:3.14-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libffi-dev libssl-dev wget xz-utils \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

ARG TARGETPLATFORM=linux/amd64
RUN set -xe && \
    case "$TARGETPLATFORM" in \
      "linux/amd64") FFMPEG_ARCH="linux64-gpl" ;; \
      "linux/arm64") FFMPEG_ARCH="linuxarm64-gpl" ;; \
      "linux/arm/v7") FFMPEG_ARCH="linuxarmv7l-gpl" ;; \
      *) echo "Unsupported platform: $TARGETPLATFORM" && exit 1 ;; \
    esac && \
    wget -q --tries=3 "https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-master-latest-${FFMPEG_ARCH}.tar.xz" && \
    tar -xf "ffmpeg-master-latest-${FFMPEG_ARCH}.tar.xz" && \
    mkdir -p /app/ffmpeg-bin && \
    cp "ffmpeg-master-latest-${FFMPEG_ARCH}"/bin/ffmpeg /app/ffmpeg-bin/ && \
    cp "ffmpeg-master-latest-${FFMPEG_ARCH}"/bin/ffprobe /app/ffmpeg-bin/ && \
    chmod +x /app/ffmpeg-bin/ffmpeg /app/ffmpeg-bin/ffprobe && \
    rm -rf "ffmpeg-master-latest-${FFMPEG_ARCH}".tar.xz "ffmpeg-master-latest-${FFMPEG_ARCH}" /tmp/* /var/tmp/*

COPY Pipfile Pipfile.lock /app/
RUN pip install --no-cache-dir pipenv && \
    pipenv install --deploy --system --ignore-pipfile && \
    rm -rf /root/.cache /root/.local /tmp/* /var/tmp/*

FROM python:3.14-slim AS production

WORKDIR /app

RUN mkdir -p /data /cache && chmod 755 /data /cache && \
    apt-get update && apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
VOLUME ["/data"]

COPY --from=builder /usr/local /usr/local
COPY --from=builder /app/ffmpeg-bin /usr/local/bin

ENV PYTHONUNBUFFERED=1
ENV LOGGING_LEVEL=INFO
ENV GOOGLE_GENAI_MODEL=gemini-2.5-flash

COPY llm_memedescriber /app/llm_memedescriber
COPY PROMPT.txt /app/
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/ || exit 1

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["uvicorn", "llm_memedescriber.app:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "info", "--no-access-log"]
