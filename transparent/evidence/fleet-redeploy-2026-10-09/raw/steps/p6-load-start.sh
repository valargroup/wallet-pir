. /root/deploy-a317455e/env.sh
cat $LOAD/pins.json
systemctl start transparent-5qps-continuous; date -u +%FT%TZ | tee $DEP/p6-load-start; systemctl is-active transparent-5qps-continuous
