NEW_SHA=06a972db469f302d8c561416d4674e937fdde0cb; SHORT=${NEW_SHA:0:8}
R=/opt/transparent-publisher/releases/$NEW_SHA
SRC=/srv/transparent-activity/ops/sources/4c85b6c20ced1e2077245491e77d3afc98bfd644
S=$SRC/transparent/ops/scripts
FLEET=/opt/transparent-publisher/v11/fleet.json
OUT=/opt/transparent-publisher/rollback/roll-$SHORT
DEP=/root/deploy-$SHORT
LOCK=/run/lock/wallet-pir-production.lock
LOAD=/srv/transparent-activity/canonical-load/v11
W="ssh -o BatchMode=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile=/opt/transparent-publisher/credentials/known_hosts -o IdentitiesOnly=yes -i /opt/transparent-publisher/credentials/deploy-ssh"
R01=10.142.0.10; R02=10.142.0.8; A03=10.142.0.7; TD01=10.142.0.6
OLDW=34ba7ebbdb9c59de1ca9f4fd32be9b7058e9b2d2ab08dacaa63bb62cecc1970c
NEWW=2490c09d11d820aab457f74f614f738e8f8fc45149aa5417ffbf85b8b2818594
NEWS=9208555a4903945e6e7262df94db8934c532d83dad28d395b513ac1f9187bc66
