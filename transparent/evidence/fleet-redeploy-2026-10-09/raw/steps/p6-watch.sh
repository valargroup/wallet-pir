# Poll every 2 s for up to 300 s until both origins return 200 and the controller is serving at the tip.
. /root/deploy-a317455e/env.sh
end=$(( $(date +%s) + 300 ))
while [ $(date +%s) -lt $end ]; do
  a=$(curl -s -o /dev/null -m 3 -w '%{http_code}' https://transparent-pir.valargroup.dev/v1/shards)
  b=$(curl -s -o /dev/null -m 3 -w '%{http_code}' https://enhance-pir.valargroup.dev/v1/filters/shards)
  st=$(curl -s -m 2 127.0.0.1:8094/v1/status | python3 -c "import json,sys
try:
 s=json.load(sys.stdin);print(s.get('phase'),s.get('public_height'),s.get('node_height'),s.get('freshness_seconds'))
except Exception:print('unavailable')")
  echo "$(date -u +%T) $a $b $st"
  set -- $st
  if [ "$a" = 200 ] && [ "$b" = 200 ] && [ "$1" = serving ] && [ $(( $3 - $2 )) -le 2 ]; then echo SERVING; exit 0; fi
  sleep 2
done
echo NOT-SERVING-WITHIN-300S; exit 1
