#!/app/.venv/bin/python
"""
Python entrypoint to avoid relying on /bin/sh in hardened runtime images.
Performs simple checks and execs the provided command.
"""
import os
import sys


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

  # Check /data is writable
  test_path = "/data/.llm_mount_test"
  try:
    with open(test_path, "w") as f:
      f.write("")
    os.remove(test_path)
  except Exception:
    print("[startup] ERROR: /data is not writable. Aborting.")
    sys.exit(1)

  if len(sys.argv) <= 1:
    print("[startup] ERROR: No command provided. entrypoint requires CMD to be set in Dockerfile.")
    sys.exit(1)

  cmd = sys.argv[1:]
  print("[startup] Launching: ", " ".join(cmd))

  # Replace current process with the requested command
  os.execvp(cmd[0], cmd)


if __name__ == "__main__":
  main()


