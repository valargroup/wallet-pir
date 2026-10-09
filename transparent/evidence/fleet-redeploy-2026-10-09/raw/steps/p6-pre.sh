. /root/deploy-a317455e/env.sh
python3 $DEP/steps/membership.py
curl -s 127.0.0.1:8094/v1/status | python3 -c "import json,sys;s=json.load(sys.stdin);print('controller',s['phase'],s['public_height'],s['node_height'],s['freshness_seconds'])"
sha256sum /usr/local/bin/transparent-publish-controller /proc/$(systemctl show -p MainPID --value transparent-publish-controller)/exe
sha256sum /srv/transparent-activity/build/evidence/release-12ce12918446eaa56e2d766ec2f43d82c531abb9/artifacts/transparent-publish-controller
lslocks | grep wallet-pir-production || echo lock-free
