. /root/deploy-a317455e/env.sh
systemctl stop transparent-5qps-continuous; date -u +%FT%TZ | tee $DEP/mixed-window-end
test ! -e $LOAD/latched.json && echo stopped-clean
python3 $DEP/steps/membership.py
