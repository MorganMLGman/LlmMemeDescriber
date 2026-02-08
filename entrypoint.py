#!/app/.venv/bin/python
"""
Python entrypoint to avoid relying on /bin/sh in hardened runtime images.
Performs simple checks and execs the provided command.
"""
import os
import sys


def ensure_ssl_certificates():
  """Generate SSL certificates if they don't exist and export paths as env vars."""
  try:
    from llm_memedescriber.ssl_helpers import validate_certificate_files
    cert_path, key_path = validate_certificate_files(None, None)
    print(f"[startup] SSL certificates ready: {cert_path}")
    os.environ["SSL_CERT_PATH"] = cert_path
    os.environ["SSL_KEY_PATH"] = key_path
    return True
  except Exception as exc:
    print(f"[startup] ERROR: Failed to initialize SSL certificates: {exc}", file=sys.stderr)
    return False


def main():
  venv_bin = "/app/.venv/bin"
  if os.path.isdir(venv_bin):
    os.environ["PATH"] = venv_bin + ":" + os.environ.get("PATH", "")
    print(f"[startup] Activated venv at {venv_bin}")

  try:
    uid = os.getuid()
  except Exception as e:
    print(f"[startup] Warning: Failed to get uid: {e}")
    uid = "?"
  try:
    gid = os.getgid()
  except Exception as e:
    print(f"[startup] Warning: Failed to get gid: {e}")
    gid = "?"

  print(f"[startup] Running as: uid={uid} gid={gid}")

  test_path = "/data/.llm_mount_test"
  try:
    with open(test_path, "w") as f:
      f.write("")
    os.remove(test_path)
  except Exception:
    print("[startup] ERROR: /data is not writable. Aborting.")
    sys.exit(1)

  if not ensure_ssl_certificates():
    sys.exit(1)

  if len(sys.argv) <= 1:
    print("[startup] ERROR: No command provided. entrypoint requires CMD to be set in Dockerfile.")
    sys.exit(1)

  cmd = sys.argv[1:]

  expanded_cmd = []
  for arg in cmd:
    if arg.startswith("--ssl-certfile="):
      cert_path = os.environ.get("SSL_CERT_PATH", "/data/certs/server.crt")
      expanded_cmd.append(f"--ssl-certfile={cert_path}")
    elif arg.startswith("--ssl-keyfile="):
      key_path = os.environ.get("SSL_KEY_PATH", "/data/certs/server.key")
      expanded_cmd.append(f"--ssl-keyfile={key_path}")
    else:
      expanded_cmd.append(arg)

  print("[startup] Launching: ", " ".join(expanded_cmd))

  os.execvp(expanded_cmd[0], expanded_cmd)


if __name__ == "__main__":
  main()


