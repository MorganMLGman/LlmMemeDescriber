#!/usr/bin/env python3
"""Health check script for LLM Meme Describer service."""
import urllib.request
import ssl
import sys
import os

try:
    # Check if NO_TLS mode is enabled
    no_tls = os.getenv("NO_TLS", "false").lower() in ("true", "1", "yes")

    if no_tls:
        # Plain HTTP health check
        urllib.request.urlopen('http://localhost:8080/health', timeout=5)
    else:
        # HTTPS health check with SSL verification disabled (for self-signed certs)
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        urllib.request.urlopen('https://localhost:8443/health', context=ctx, timeout=5)

    sys.exit(0)
except Exception as e:
    print(f"Health check failed: {e}", file=sys.stderr)
    sys.exit(1)
