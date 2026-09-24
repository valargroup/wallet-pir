import subprocess
root='/root/packing-budget-20260924'
base=['python3',root+'/run.py','--binary',root+'/packing_budget','--out',root+'/results','--cpus','0-3']
def measure(name,copies,concurrency,iterations,limit='7G',overlap=True,fixture='fixture32-v3'):
    cmd=base+['--name',name,'--limit',limit,'measure','--fixture',root+'/'+fixture,'--copies',str(copies),'--concurrency',str(concurrency),'--iterations',str(iterations),'--hold-ms','100']
    if overlap: cmd+=['--overlap']
    subprocess.run(cmd,check=True)
measure('s7-c4-build-7g-r2',7,4,128)
measure('s7-c4-build-7g-r3',7,4,128)
measure('s8-c4-build-8g',8,4,32,limit='8G')
measure('s7-c16-build-7g',7,16,16)
for rows in [4096,8192,16384]:
    fixture='fixture'+str(rows)
    subprocess.run(base+['--name','prepare'+str(rows),'--limit','12G','prepare','--output',root+'/'+fixture,'--rows',str(rows),'--queries','4'],check=True)
    measure('rows'+str(rows)+'-s1-c4',1,4,16,overlap=False,fixture=fixture)
