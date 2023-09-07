import os
param=[3,5,7,10,20,30,40,50]
for it,item in enumerate(param):
  f=open(f"turing10m-ivfflat/workload-turing10m-ivfflat-script{it}.sh","w")
  f.write("load turing10m.bin turing10m\n")
  for _ in range(5):
    f.write(f"create_index turing10m data ivfflat {item}\n")
    f.write(f"similar_vector /ssd_root/dataset/turing10m/testQuery10K.fbin /ssd_root/dataset/turing10m/clu_msturing10M_gt100 turing10m data\n")
    f.write("drop_index turing10m\n")
  f.write("quit\n")
  f.close()
    
    
param2=[100,200,300,400,500,600,700,800]
for it,item in enumerate(param2):
  f=open(f"turing10m-hnsw/workload-turing10m-hnsw-script{it}.sh","w")
  f.write("load turing10m.bin turing10m\n")
  f.write(f"create_index turing10m data hnsw {item}\n")
  for _ in range(5):
    f.write(f"similar_vector /ssd_root/dataset/turing10m/testQuery10K.fbin /ssd_root/dataset/turing10m/clu_msturing10M_gt100 turing10m data\n")
    f.write(f"reset_para turing10m hnsw {item}\n")
  f.write("drop_index turing10m\n")
  f.write("quit\n")
  f.close()