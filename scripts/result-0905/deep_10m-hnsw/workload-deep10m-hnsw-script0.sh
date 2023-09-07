load deep10m.bin deep10m
create_index deep10m data hnsw 100
similar_vector /ssd_root/dataset/deep1b/deep1b_gt/deep1B_queries.fvecs /ssd_root/dataset/deep1b/deep10M_groundtruth.ivecs deep10m data
reset_para deep10m hnsw 100
similar_vector /ssd_root/dataset/deep1b/deep1b_gt/deep1B_queries.fvecs /ssd_root/dataset/deep1b/deep10M_groundtruth.ivecs deep10m data
reset_para deep10m hnsw 100
similar_vector /ssd_root/dataset/deep1b/deep1b_gt/deep1B_queries.fvecs /ssd_root/dataset/deep1b/deep10M_groundtruth.ivecs deep10m data
reset_para deep10m hnsw 100
similar_vector /ssd_root/dataset/deep1b/deep1b_gt/deep1B_queries.fvecs /ssd_root/dataset/deep1b/deep10M_groundtruth.ivecs deep10m data
reset_para deep10m hnsw 100
similar_vector /ssd_root/dataset/deep1b/deep1b_gt/deep1B_queries.fvecs /ssd_root/dataset/deep1b/deep10M_groundtruth.ivecs deep10m data
reset_para deep10m hnsw 100
drop_index deep10m
quit
