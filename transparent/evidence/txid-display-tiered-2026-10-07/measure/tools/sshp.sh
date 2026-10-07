#!/bin/zsh
# usage: sshp.sh <coordinator|recent|archive|router> <cmd>
# Pinned options from the brief, plus connection reuse (ControlMaster) for slow links.
O=(-F /dev/null -i $HOME/.ssh/id_ed25519 -o BatchMode=yes -o IdentitiesOnly=yes -o ForwardAgent=no -o ConnectTimeout=10 -o StrictHostKeyChecking=yes -o UserKnownHostsFile=$HOME/.config/wallet-pir-deploy/known_hosts -o GlobalKnownHostsFile=/dev/null -o ControlMaster=auto -o ControlPersist=900 -o "ControlPath=$HOME/.ssh/cm-txid/%C" -o ServerAliveInterval=10)
P=(${O[@]/\%C/%%C})
h=$1; shift
case $h in
  coordinator) exec ssh $O root@167.99.42.60 "$@";;
  recent) exec ssh $O -o "ProxyCommand=ssh ${P[*]} -W %h:%p root@167.99.42.60" root@10.142.0.10 "$@";;
  archive) exec ssh $O -o "ProxyCommand=ssh ${P[*]} -W %h:%p root@167.99.42.60" root@10.142.0.7 "$@";;
  router) exec ssh $O -o "ProxyCommand=ssh ${P[*]} -W %h:%p root@167.99.42.60" root@10.142.0.11 "$@";;
esac
