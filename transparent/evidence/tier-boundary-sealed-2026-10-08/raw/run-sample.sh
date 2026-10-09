# samples the scope memory every 5 s while it runs
unit=$1
while systemctl is-active --quiet $unit; do
  echo "$(date -u +%T) $(systemctl show -p MemoryCurrent -p MemoryPeak --value $unit | tr "\n" " ") $(df -P / | awk "NR==2{print \$5}")"
  sleep 5
done
