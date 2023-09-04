import os
param=[3,5,7,10,20,30,40,50]
for it,item in enumerate(param):
  f=open(f"deep_10m-ivfflat/workload-deep10m-ivfflat-script{it}.sh","w")
  f.write("load deep10m.bin deep10m\n")
  for _ in range(5):
    f.write(f"create_index deep10m data ivfflat {item}\n")
    f.write(f"similar_vector /home/jin467/dataset/deep10M/deep1b_gt/deep1b/deep1B_queries.fvecs /home/jin467/dataset/deep10M/deep10M_groundtruth.ivecs deep10m data\n")
    f.write("drop_index deep10m\n")
  f.write(quit)
  f.close()
    