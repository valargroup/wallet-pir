import json,time,urllib.request,urllib.error,sys
end=time.monotonic()+int(sys.argv[1])
with open(sys.argv[2],'x') as output:
 while True:
  row={'sampled_ms':time.time_ns()//1000000}
  for name,port in [('coordinator',8480),('worker',8481),('router',8482)]:
   try:
    with urllib.request.urlopen(f'http://127.0.0.1:{port}/internal/status-apm',timeout=2) as response:data=json.load(response)
    row[name]={'admission':data['admission'],'operations':data['operations']}
   except Exception as error:row[name]={'error':type(error).__name__}
  output.write(json.dumps(row)+'\n');output.flush()
  remaining=end-time.monotonic()
  if remaining<=0:break
  time.sleep(min(1,remaining))
