FROM dhi.io/python:3.14-debian13-dev AS builder
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential pkg-config gcc libffi-dev libssl-dev wget xz-utils ca-certificates rustc cargo ffmpeg \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN python3 -m pip install --upgrade pip setuptools wheel

COPY Pipfile Pipfile.lock /app/
RUN python3 -m pip install --no-cache-dir pipenv \
  && cd /app \
  && PIPENV_VENV_IN_PROJECT=1 pipenv install --deploy --ignore-pipfile \
  && rm -rf /root/.cache /root/.local /tmp/* /var/tmp/*

RUN mkdir -p /data /cache && chmod 755 /data /cache

FROM dhi.io/python:3.14-debian13-dev AS ffmpeg-builder
ARG TARGETARCH

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN mkdir -p /ffmpeg-libs && \
    LIBDIR=$(if [ "$TARGETARCH" = "amd64" ]; then echo "x86_64-linux-gnu"; else echo "aarch64-linux-gnu"; fi) && \
    cp -r /usr/lib/$LIBDIR/libav* /ffmpeg-libs/ 2>/dev/null || true && \
    cp -r /usr/lib/$LIBDIR/libsw* /ffmpeg-libs/ 2>/dev/null || true && \
    cp -r /usr/lib/$LIBDIR/libpostproc* /ffmpeg-libs/ 2>/dev/null || true

FROM dhi.io/python:3.14-debian13 AS production
WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
COPY --from=ffmpeg-builder /usr/bin/ffmpeg /usr/bin/ffmpeg
COPY --from=ffmpeg-builder /usr/bin/ffprobe /usr/bin/ffprobe
COPY --from=ffmpeg-builder /ffmpeg-libs /usr/lib/
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
  CMD ["/app/.venv/bin/python","-c","import urllib.request,sys;\ntry:\n  urllib.request.urlopen('http://localhost:8000')\nexcept Exception:\n  sys.exit(1)"]

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["python", "-m", "uvicorn", "llm_memedescriber.app:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "info", "--no-access-log"]
