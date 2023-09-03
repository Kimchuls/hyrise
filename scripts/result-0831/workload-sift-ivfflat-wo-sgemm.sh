load sift_base.csv sift_base
create_index sift_base data ivfflat 20
similar_vector ../scripts/vector_test/sift/sift_query_load_data.sh ../scripts/vector_test/sift/sift_groundtruth.ivecs sift_base data
drop_index sift_base
create_index sift_base data ivfflat 20
similar_vector ../scripts/vector_test/sift/sift_query_load_data.sh ../scripts/vector_test/sift/sift_groundtruth.ivecs sift_base data
drop_index sift_base
create_index sift_base data ivfflat 20
similar_vector ../scripts/vector_test/sift/sift_query_load_data.sh ../scripts/vector_test/sift/sift_groundtruth.ivecs sift_base data
drop_index sift_base
quit