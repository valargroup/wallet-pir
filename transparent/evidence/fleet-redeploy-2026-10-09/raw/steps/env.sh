NEW_SHA=a317455e9feecdd2639d5348188c6a2341819a4d; SHORT=${NEW_SHA:0:8}
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
OLDW=6db1fa05cfaf7a6422f7307b394c95ef20129598ea591818c9d4da324ef20430
OLDC=a68dca012c35004ec9628368a87b1cda9ee21605404f0f8cb0ac40d4d8ffad4d
NEWW=34ba7ebbdb9c59de1ca9f4fd32be9b7058e9b2d2ab08dacaa63bb62cecc1970c
NEWS=9208555a4903945e6e7262df94db8934c532d83dad28d395b513ac1f9187bc66
NEWC=13af048e53bf2d5921ede069db582b9821b5d26a54169d84dc15bf1cbb8ba2ba
