import os
import subprocess
root='/root/packing-budget-20260924'
for name in ['hint.bin','fixture.json']:
    with open(root+'/fixture32-v3/'+name,'rb') as f:
        os.posix_fadvise(f.fileno(),0,0,os.POSIX_FADV_DONTNEED)
subprocess.run(['python3',root+'/run.py','--binary',root+'/packing_budget','--out',root+'/results','--name','s7-c4-build-7g-cold','--limit','7G','--cpus','0-3','measure','--fixture',root+'/fixture32-v3','--copies','7','--concurrency','4','--iterations','128','--hold-ms','100','--overlap'],check=True)
