"""Run deploy-transparent-publisher.install_worker for one v11 worker: stage | install."""
import asyncio, importlib.util, json, sys
from pathlib import Path
scripts, fleet_config, worker_id, artifacts, rollback, mode = sys.argv[1:7]
spec = importlib.util.spec_from_file_location("publisher_deploy", Path(scripts) / "deploy-transparent-publisher.py")
D = importlib.util.module_from_spec(spec); spec.loader.exec_module(D)
fleet = D.LIVE.Fleet(json.loads(Path(fleet_config).read_text()))
worker = next(w for w in fleet.roster if w["id"] == worker_id)
asyncio.run(D.install_worker(fleet, worker, Path(artifacts), rollback, stage_only=(mode == "stage"), warm_seconds=1800))
print(json.dumps({"worker": worker_id, "mode": mode, "done": True}))
