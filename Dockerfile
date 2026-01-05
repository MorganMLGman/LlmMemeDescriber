FROM --platform=$BUILDPLATFORM dhi.io/python:3.14-debian13-dev AS builder

ARG TARGETPLATFORM=linux/amd64
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       gcc libffi-dev libssl-dev wget xz-utils ca-certificates \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN set -xe \
    && case "$TARGETPLATFORM" in \
         "linux/amd64") FFMPEG_ARCH="linux64-gpl" ;; \
         "linux/arm64") FFMPEG_ARCH="linuxarm64-gpl" ;; \
         *) echo "Unsupported platform: $TARGETPLATFORM" && exit 1 ;; \
       esac \
    && wget -q --tries=3 "https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-master-latest-${FFMPEG_ARCH}.tar.xz" \
    && tar -xf "ffmpeg-master-latest-${FFMPEG_ARCH}.tar.xz" \
    && mkdir -p /app/ffmpeg-bin \
    && cp "ffmpeg-master-latest-${FFMPEG_ARCH}"/bin/ffmpeg /app/ffmpeg-bin/ \
    && cp "ffmpeg-master-latest-${FFMPEG_ARCH}"/bin/ffprobe /app/ffmpeg-bin/ \
    && chmod +x /app/ffmpeg-bin/ffmpeg /app/ffmpeg-bin/ffprobe \
    && rm -rf "ffmpeg-master-latest-${FFMPEG_ARCH}.tar.xz" "ffmpeg-master-latest-${FFMPEG_ARCH}" /tmp/* /var/tmp/*

COPY Pipfile Pipfile.lock /app/
RUN python3 -m pip install --no-cache-dir pipenv \
  && cd /app \
  && PIPENV_VENV_IN_PROJECT=1 pipenv install --deploy --ignore-pipfile \
  && rm -rf /root/.cache /root/.local /tmp/* /var/tmp/*

RUN mkdir -p /data /cache && chmod 755 /data /cache

FROM dhi.io/python:3.14-debian13 AS production

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/ffmpeg-bin /usr/local/bin
COPY --from=builder /data /data
COPY --from=builder /cache /cache
VOLUME ["/data"]

ENV PYTHONUNBUFFERED=1
ENV LOGGING_LEVEL=INFO
ENV GOOGLE_GENAI_MODEL=gemini-2.5-flash
ENV PATH=/app/.venv/bin:$PATH

COPY llm_memedescriber /app/llm_memedescriber
COPY PROMPT.txt /app/
COPY --chmod=0755 entrypoint.sh /app/entrypoint.sh

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD ["python3","-c","import urllib.request,sys;\ntry:\n  urllib.request.urlopen('http://localhost:8000')\nexcept Exception:\n  sys.exit(1)"]

ENTRYPOINT ["/app/.venv/bin/python","/app/entrypoint.sh"]
CMD ["python", "-m", "uvicorn", "llm_memedescriber.app:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "info", "--no-access-log"]
