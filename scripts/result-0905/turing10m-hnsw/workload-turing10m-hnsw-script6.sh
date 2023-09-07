load turing10m.bin turing10m
create_index turing10m data hnsw 700
similar_vector /ssd_root/dataset/turing10m/testQuery10K.fbin /ssd_root/dataset/turing10m/clu_msturing10M_gt100 turing10m data
reset_para turing10m hnsw 700
similar_vector /ssd_root/dataset/turing10m/testQuery10K.fbin /ssd_root/dataset/turing10m/clu_msturing10M_gt100 turing10m data
reset_para turing10m hnsw 700
similar_vector /ssd_root/dataset/turing10m/testQuery10K.fbin /ssd_root/dataset/turing10m/clu_msturing10M_gt100 turing10m data
reset_para turing10m hnsw 700
similar_vector /ssd_root/dataset/turing10m/testQuery10K.fbin /ssd_root/dataset/turing10m/clu_msturing10M_gt100 turing10m data
reset_para turing10m hnsw 700
similar_vector /ssd_root/dataset/turing10m/testQuery10K.fbin /ssd_root/dataset/turing10m/clu_msturing10M_gt100 turing10m data
reset_para turing10m hnsw 700
drop_index turing10m
quit
