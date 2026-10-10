NEW_SHA=9d2cbda0b4336258cfce76ca8ee77b095daa7fd7; SHORT=${NEW_SHA:0:8}
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
OLDW=2490c09d11d820aab457f74f614f738e8f8fc45149aa5417ffbf85b8b2818594
NEWW=6ab86d6a40273db316bd78c3d6c084362fef8f069c774d108b436cf9018f53bd
NEWS=ff3c0264f9ccb0eba3633098f495b1122776e70d2f7d359525ae41c3e4888232
OLDC=13af048e53bf2d5921ede069db582b9821b5d26a54169d84dc15bf1cbb8ba2ba
NEWC=6041ea475c39399a744d2a951cf29f2c09233df66174275fb0ada61283b40ea5
