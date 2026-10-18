"""Execute fleet remote scripts against a temporary filesystem and fake systemd."""
import os
from pathlib import Path
import subprocess
import sys

root = Path(os.environ["HOST_ROOT"])

def redirect(text):
    for prefix in ("/opt/transparent-pir", "/usr/local/bin", "/etc/systemd/system", "/tmp/transparent-pir-"):
        text = text.replace(prefix, str(root) + prefix)
    return text

args = sys.argv[1:]
if args[0] != "bash":
    sys.exit(subprocess.run(["bash", "-c", redirect(" ".join(args))], check=False).returncode)
script = redirect(sys.stdin.read())
preamble = r'''
sudo() { "$@"; }
systemctl() {
  echo "$*" >>"$HOST_ROOT/systemctl.log"
  case "$1" in
    is-active) echo active;;
    is-enabled) echo enabled;;
    enable) [[ "${FAIL_ACTIVATION:-false}" != true || "$2" != --now ]];;
  esac
}
'''
result = subprocess.run([redirect(a) for a in args], input=preamble + script, text=True, check=False)
sys.exit(result.returncode)
