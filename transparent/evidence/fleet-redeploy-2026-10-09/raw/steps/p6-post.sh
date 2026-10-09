. /root/deploy-a317455e/env.sh
sha256sum /proc/$(systemctl show -p MainPID --value transparent-publish-controller)/exe /usr/local/bin/transparent-publish-controller
echo "NEWC=$NEWC"
systemctl show transparent-publish-controller -p NRestarts -p ActiveEnterTimestamp -p MemoryCurrent
curl -s https://transparent-pir.valargroup.dev/v1/shards > $DEP/map-after-controller.json
curl -s https://enhance-pir.valargroup.dev/v1/filters/shards > $DEP/map-after-controller-filters.json
cmp $DEP/map-after-controller.json $DEP/map-after-controller-filters.json && echo origins-agree
python3 $DEP/sealed-compare.py $DEP/map-before-controller.json $DEP/map-after-controller.json
python3 $DEP/sealed-compare.py $DEP/baseline/map-public.json $DEP/map-after-controller.json
A=/srv/transparent-activity/full-v11/publications/active.json
python3 -c "import json;a=json.load(open('$A'));p=json.load(open(a['directory']+'/publication.json'));print('active', a['directory'].rsplit('/',1)[-1], 'source_sha', p.get('source_sha'))"
journalctl -u transparent-publish-controller --since "$(date -u -d @$(cut -d. -f1 $DEP/controller-restart-start) '+%F %T')" -o cat | grep -iE 'error|warn|panic' | cut -c1-300 | head -20
echo "errors/warnings since restart: $(journalctl -u transparent-publish-controller --since "$(date -u -d @$(cut -d. -f1 $DEP/controller-restart-start) '+%F %T')" -o cat | grep -ciE 'error|warn|panic')"
python3 $DEP/steps/membership.py
