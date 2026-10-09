. /root/deploy-a317455e/env.sh
python3 $DEP/pin.py $NEWW transparent-pir-recent-01 transparent-pir-recent-02
systemctl start transparent-5qps-continuous; date -u +%FT%TZ | tee $DEP/p5-load-start; systemctl is-active transparent-5qps-continuous
