load gist_base.csv gist_base
create_index gist_base data ivfflat 20
similar_vector ../scripts/vector_test/gist/gist_query_load_data.sh ../scripts/vector_test/gist/gist_groundtruth.ivecs gist_base data
drop_index gist_base
create_index gist_base data ivfflat 20
similar_vector ../scripts/vector_test/gist/gist_query_load_data.sh ../scripts/vector_test/gist/gist_groundtruth.ivecs gist_base data
drop_index gist_base
create_index gist_base data ivfflat 20
similar_vector ../scripts/vector_test/gist/gist_query_load_data.sh ../scripts/vector_test/gist/gist_groundtruth.ivecs gist_base data
drop_index gist_base
quit