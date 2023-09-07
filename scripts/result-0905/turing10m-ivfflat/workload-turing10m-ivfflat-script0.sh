load turing10m.bin turing10m
create_index turing10m data ivfflat 3
similar_vector /ssd_root/dataset/turing10m/testQuery10K.fbin /ssd_root/dataset/turing10m/clu_msturing10M_gt100 turing10m data
drop_index turing10m
create_index turing10m data ivfflat 3
similar_vector /ssd_root/dataset/turing10m/testQuery10K.fbin /ssd_root/dataset/turing10m/clu_msturing10M_gt100 turing10m data
drop_index turing10m
create_index turing10m data ivfflat 3
similar_vector /ssd_root/dataset/turing10m/testQuery10K.fbin /ssd_root/dataset/turing10m/clu_msturing10M_gt100 turing10m data
drop_index turing10m
create_index turing10m data ivfflat 3
similar_vector /ssd_root/dataset/turing10m/testQuery10K.fbin /ssd_root/dataset/turing10m/clu_msturing10M_gt100 turing10m data
drop_index turing10m
create_index turing10m data ivfflat 3
similar_vector /ssd_root/dataset/turing10m/testQuery10K.fbin /ssd_root/dataset/turing10m/clu_msturing10M_gt100 turing10m data
drop_index turing10m
quit
