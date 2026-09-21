package main
import("encoding/json";"encoding/binary";"fmt";"os";"runtime";"time";"github.com/ahenzinger/simplepir/pir")
func emit(v any){json.NewEncoder(os.Stdout).Encode(v)}
func wire(m pir.Msg) []byte {b:=make([]byte,m.Size()*4);i:=0;for _,a:=range m.Data{for _,v:=range a.Data{binary.LittleEndian.PutUint32(b[i:],uint32(v));i+=4}};return b}
func main(){
 runtime.GOMAXPROCS(1);const n uint64=466986;const bits uint64=737*8
 pi:=pir.SimplePIR{};p:=pi.PickParams(n,bits,1024,32);db:=pir.SetupDB(n,bits,&p);db.Data=pir.MatrixZeros(p.L,p.M)
 mask:=uint64(1<<32)-1
 for r:=uint64(0);r<p.L;r++{for c:=uint64(0);c<p.M;c++{position:=(r/db.Info.Ne)*p.M+c;digit:=r%db.Info.Ne;v:=uint64(0);if position<n&&digit+1<db.Info.Ne{v=((position+1)*131+digit*37+(position>>8))%p.P};db.Data.Set((v+(1<<32)-p.P/2)&mask,r,c)}}
 emit(map[string]any{"event":"database","records":n,"record_bytes":737,"params":p,"info":db.Info,"unpacked_matrix_bytes":db.Data.Size()*4})
 targets:=[]uint64{0,n-1,p.M-1,p.M,n/2,12345}
 expected:=map[uint64][]uint64{};for _,i:=range targets{v:=[]uint64{};for j:=uint64(0);j<db.Info.Ne;j++{v=append(v,((db.Data.Get((i/p.M)*db.Info.Ne+j,i%p.M)+p.P/2)&mask)%p.P)};expected[i]=v}
 t:=time.Now();shared,_:=pi.InitCompressed(db.Info,p);emit(map[string]any{"event":"public_matrix","seconds":time.Since(t).Seconds(),"bytes":shared.Data[0].Size()*4})
 t=time.Now();state,hint:=pi.Setup(db,shared,p);elapsed:=time.Since(t).Seconds();hb:=wire(hint);emit(map[string]any{"event":"setup","seconds":elapsed,"hint_bytes":len(hb),"packed_database_bytes":db.Data.Size()*4});hb=nil;runtime.GC()
 for i:=0;i<103;i++{target:=targets[i%len(targets)];t=time.Now();secret,q:=pi.Query(target,shared,p,db.Info);qb:=wire(q);qm:=float64(time.Since(t).Nanoseconds())/1e6
 t=time.Now();ans:=pi.Answer(db,pir.MakeMsgSlice(q),state,shared,p);server:=float64(time.Since(t).Nanoseconds())/1e6;ab:=wire(ans)
 t=time.Now();correction:=pir.MatrixMul(hint.Data[0],secret.Data[0]);offset:=uint64(0);for j:=uint64(0);j<p.M;j++{offset+=(p.P/2)*q.Data[0].Get(j,0)};offset=(1<<32)-(offset&mask);ok:=true
 for j,want:=range expected[target]{r:=(target/p.M)*db.Info.Ne+uint64(j);v:=(ans.Data[0].Get(r,0)+(1<<32)-correction.Get(r,0))&mask;got:=(p.Round(v+offset)+p.P/2)%p.P;if got!=want{ok=false;panic(fmt.Sprintf("incorrect coefficient query=%d digit=%d got=%d want=%d",i,j,got,want))}}
 decode:=float64(time.Since(t).Nanoseconds())/1e6
 emit(map[string]any{"event":"query","i":i,"warmup":i<3,"target":target,"exact_all_record_coefficients":ok,"upload_bytes":len(qb),"download_bytes":len(ab),"server_ms":server,"query_and_serialize_ms":qm,"decode_ms":decode})
 }
 emit(map[string]any{"event":"complete"})
}
