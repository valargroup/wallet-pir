"""roll-recent-replicas.py with MIN_OTHER_SERVING = 1: the other replica must be serving.
Explicit, logged one-off override for the 2026-10-09 a317455e roll (decision D2); committed defaults unchanged."""
import importlib.util, json, sys
from pathlib import Path
script = Path(sys.argv[1])
spec = importlib.util.spec_from_file_location("roll_recent", script)
module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
print(json.dumps({"event": "override", "MIN_OTHER_SERVING": {"default": module.MIN_OTHER_SERVING, "this_run": 1}, "decision": "D2"}), flush=True)
module.MIN_OTHER_SERVING = 1
sys.argv = [str(script)] + sys.argv[2:]
module.main()
