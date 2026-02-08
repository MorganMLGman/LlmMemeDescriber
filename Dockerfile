FROM python:3.14-slim AS builder
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

RUN mkdir -p /data && chmod 755 /data

FROM python:3.14-slim AS production
ARG TARGETARCH
WORKDIR /app

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
       libva2 libva-drm2 libva-wayland2 \
       libdrm2 libdrm-common \
       va-driver-all \
       ffmpeg \
    && if [ "$TARGETARCH" = "amd64" ]; then \
         apt-get install -y --no-install-recommends intel-media-va-driver i965-va-driver || true; \
       fi \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /data /data
VOLUME ["/data"]

ENV PYTHONUNBUFFERED=1
ENV LOGGING_LEVEL=INFO
ENV GOOGLE_GENAI_MODEL=gemini-3-flash-preview
ENV LIBVA_DRIVER_NAME=iHD
ENV PATH=/app/.venv/bin:$PATH

COPY llm_memedescriber /app/llm_memedescriber
COPY alembic /app/alembic
COPY alembic.ini /app/
COPY PROMPT.txt /app/
COPY --chmod=0755 entrypoint.py /app/entrypoint.py
COPY --chmod=0755 healthcheck.py /app/healthcheck.py

EXPOSE 8443
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD ["/app/.venv/bin/python", "/app/healthcheck.py"]

ENTRYPOINT ["/app/.venv/bin/python", "/app/entrypoint.py"]
CMD ["python", "-m", "uvicorn", "llm_memedescriber.app:app", "--host", "0.0.0.0", "--port=8443", "--ssl-certfile=/data/certs/server.crt", "--ssl-keyfile=/data/certs/server.key", "--log-level", "info", "--no-access-log"]
