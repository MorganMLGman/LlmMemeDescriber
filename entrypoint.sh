#!/app/.venv/bin/python
"""
Python entrypoint to avoid relying on /bin/sh in hardened runtime images.
Performs simple checks and execs the provided command.
"""
import os
import sys
import subprocess


def main():
  # Activate venv by ensuring its bin is first in PATH
  venv_bin = "/app/.venv/bin"
  if os.path.isdir(venv_bin):
    os.environ["PATH"] = venv_bin + ":" + os.environ.get("PATH", "")
    print(f"[startup] Activated venv at {venv_bin}")

  try:
    uid = os.getuid()
    gid = os.getgid()
    user = subprocess.check_output(["id", "-un"]).decode().strip()
  except Exception:
    uid = "?"
    gid = "?"
    user = "?"

  print(f"[startup] Running as: user={user} uid={uid} gid={gid}")

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


