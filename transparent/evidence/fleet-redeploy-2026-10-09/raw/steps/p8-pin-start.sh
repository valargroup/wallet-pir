. /root/deploy-a317455e/env.sh
python3 $DEP/pin.py $NEWW transparent-pir-archive-03
systemctl start transparent-5qps-continuous; date -u +%FT%TZ | tee $DEP/p8-load-start; systemctl is-active transparent-5qps-continuous
