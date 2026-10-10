# On the coordinator: if the production lock is held by a flock whose sshd parent's peer is $1 (this Mac), end that sshd.
L=/run/lock/wallet-pir-production.lock
flock -n $L true && { echo LOCK_FREE; exit 0; }
for p in $(lslocks -n -o PID,PATH | awk '/wallet-pir-production/{print $1}'); do
  pp=$(ps -o ppid= -p $p | tr -d ' ')
  case "$(ps -o comm= -p $pp)" in sshd*) ;; *) continue ;; esac
  ss -tnp | grep "pid=$pp," | grep -q "$1:" && { echo "ending orphan lock session $pp (flock $p)"; kill $pp; }
done
sleep 2; flock -n $L true && echo LOCK_FREE || echo LOCK_HELD
