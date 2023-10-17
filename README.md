# Welcome to HyriseVector

We present **HyriseVector**, a novel generalized vector database that achieves performance similar to that of highly optimized specialized vector databases. 
At a high level, HyriseVector distinguishes itself from other generalized vector databases in two key aspects. First, it is built on top of Hyrise, a main-memory column-based relational database that we have carefully chosen. Second, HyriseVector treats high-dimensional indexes as first-class citizens. 

Under the hood, HyriseVector introduces a suite of optimizations, including index-centric query optimization that pushes down top-k queries, batch-oriented execution that enables efficient index construction as well as vector similarity search and optimization for multi-core parallelism and SIMD to improve performance. Importantly, many of these design concepts and optimizations are applicable to other relational databases, and we discuss how to extend them to PostgreSQL. 

We compare HyriseVector with **9 vector databases**, including 4 generalized vector databases (PASE, pgvector, AnalyticDB-V, ClickHouse) and 5 specialized vector databases (Faiss, Milvus, Qdrant, Weaviate, and Pinecone), using 3 datasets (SIFT1M, GIST1M, DEEP10M). Experiments demonstrate that HyriseVector achieves performance comparable to that of highly optimized specialized vector databases and improves existing generalized vector databases by up to **40X**.

# Install HyriseVector

Our HyriseVector can be constructed following these steps:

```script
# It is currently not possible to download an anonymized repository neither to clone it.
git clone --recursive git@github.com:Anonymous/hyrise.git -b [#Branch name]     

# Enter the Hyrise directory
cd hyrise 

# Install script handle dependencies
./install_dependencies.sh

# Create the build directory
mkdir cmake-build-debug && cd cmake-build-debug

# Generate Makefiles
cmake ..

# Build the Console
make hyriseConsole -j [#Threads]
```

# System Structure
We illustrate the important parts in our HyriseVector implementation.

<img src="./ppt_create_index_node_structure2.png" width = 70% height = 70% alt="search" align=center />

# Code Structure

```txt
.
├── ...
├── cmake/
├── cmake-build-debug/  # build directory
├── CMakeLists.txt
├── install_dependencies.sh # install dependencies
├── README.md
├── requirements.txt
└── src/
    ├── ...
    ├── benchmark/
    ├── benchmarklib/ # Hyrise testing benchmark Implementation
    ├── bin/ # Hyrise Console, Client and Server 
    ├── CMakeLists.txt
    └── lib/
        ├── ...
        ├── logical_query_plan/ # Logical Query Plan
        ├── operators/ # Physical Query Plan
        ├── sql/ # SQL Translator
        └── storage/ # Storage Manager
            ├── ...
            ├── index/ # Index Manager
            │   ├── ...
            │   ├── abstract_vector_index.cpp
            │   ├── abstract_vector_index.hpp
            │   ├── hnsw/ # HNSW Index Implementation
            │   └── IVF_Flat/ # IVF_FLAT Index Implementation
            ├── table.cpp
            └── table.hpp # Table 
```

# Getting started

We can use the SQL sentense below to **create a table with vector variable**.
```sql
CREATE TABLE SIFT_BASE (id INT, dat VECTOR(10));
```

We can use the SQL sentense below to **insert vector data** into the table we have created.
```sql
INSERT INTO SIFT_BASE(id, dat) VALUES (1, VECTOR '[1, 2, 3, 4, 5, 6, 7, 8, 9, 10.1]');
```

We can use the SQL sentense below to **create vector index** of the table we have created and the column we select.
```sql
CREATE INDEX ON SIFT_BASE USING hnsw(dat, L2) WITH(M=16, ef_construction=40, efs=100);
```

We can use the SQL sentense below to **set parameter** in vector index we select.
```sql
SET SIFT_BASE.hnsw.efs=200;
```

We can use the SQL sentense below to search similar vectors according to a batch of queries.
```sql
SELECT id FROM SIFT_BASE ORDER BY dat <$> 
    '[1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9, 0.1]',
    '[2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9, 1.1, 0.1]'
LIMIT 100;
```

# Sample Experiment

Here are the process of executing an expriment.

First, we need to get the **SIFT_1M** dataset:
```txt
wget ftp://ftp.irisa.fr/local/texmex/corpus/sift.tar.gz
```
Next, it's necessary to use the SQL statements to create a table with 2 columns(id and vector data), and insert all the data into the new table.
```sql
create table SIFT_BASE(id int, data vector(128));
insert into sift_base values (0, 
    vector '[0.000000, 16.000000, 35.000000, 5.000000, 32.000000, 31.000000, 14.000000, 10.000000, 
            11.000000, 78.000000, 55.000000, 10.000000, 45.000000, 83.000000, 11.000000, 6.000000, 
            14.000000, 57.000000, 102.000000, 75.000000, 20.000000, 8.000000, 3.000000, 5.000000, 
            67.000000, 17.000000, 19.000000, 26.000000, 5.000000, 0.000000, 1.000000, 22.000000, 
            60.000000, 26.000000, 7.000000, 1.000000, 18.000000, 22.000000, 84.000000, 53.000000, 
            85.000000, 119.000000, 119.000000, 4.000000, 24.000000, 18.000000, 7.000000, 7.000000, 
            1.000000, 81.000000, 106.000000, 102.000000, 72.000000, 30.000000, 6.000000, 0.000000, 
            9.000000, 1.000000, 9.000000, 119.000000, 72.000000, 1.000000, 4.000000, 33.000000, 
            119.000000, 29.000000, 6.000000, 1.000000, 0.000000, 1.000000, 14.000000, 52.000000, 
            119.000000, 30.000000, 3.000000, 0.000000, 0.000000, 55.000000, 92.000000, 111.000000, 
            2.000000, 5.000000, 4.000000, 9.000000, 22.000000, 89.000000, 96.000000, 14.000000, 
            1.000000, 0.000000, 1.000000, 82.000000, 59.000000, 16.000000, 20.000000, 5.000000, 
            25.000000, 14.000000, 11.000000, 4.000000, 0.000000, 0.000000, 1.000000, 26.000000, 
            47.000000, 23.000000, 4.000000, 0.000000, 0.000000, 4.000000, 38.000000, 83.000000, 
            30.000000, 14.000000, 9.000000, 4.000000, 9.000000, 17.000000, 23.000000, 41.000000, 
            0.000000, 0.000000, 2.000000, 8.000000, 19.000000, 25.000000, 23.000000, 1.000000]');
insert into sift_base values (1, 
    vector '[14.000000, 35.000000, 19.000000, 20.000000, 3.000000, 1.000000, 13.000000, 11.000000, 
            16.000000, 119.000000, 85.000000, 5.000000, 0.000000, 5.000000, 24.000000, 26.000000, 
            0.000000, 27.000000, 119.000000, 13.000000, 3.000000, 9.000000, 19.000000, 0.000000, 
            0.000000, 11.000000, 73.000000, 9.000000, 10.000000, 3.000000, 5.000000, 0.000000, 
            92.000000, 38.000000, 17.000000, 39.000000, 32.000000, 7.000000, 15.000000, 47.000000, 
            119.000000, 111.000000, 53.000000, 27.000000, 8.000000, 0.000000, 0.000000, 52.000000, 
            5.000000, 7.000000, 63.000000, 51.000000, 84.000000, 43.000000, 0.000000, 1.000000, 
            12.000000, 8.000000, 20.000000, 25.000000, 33.000000, 30.000000, 2.000000, 5.000000, 
            59.000000, 23.000000, 25.000000, 105.000000, 25.000000, 23.000000, 5.000000, 18.000000, 
            119.000000, 15.000000, 7.000000, 13.000000, 14.000000, 19.000000, 95.000000, 119.000000, 
            5.000000, 0.000000, 0.000000, 14.000000, 119.000000, 103.000000, 93.000000, 39.000000, 
            11.000000, 4.000000, 1.000000, 4.000000, 13.000000, 43.000000, 62.000000, 18.000000, 
            2.000000, 0.000000, 0.000000, 8.000000, 44.000000, 65.000000, 7.000000, 1.000000, 
            3.000000, 0.000000, 0.000000, 1.000000, 19.000000, 45.000000, 94.000000, 95.000000, 
            13.000000, 7.000000, 0.000000, 0.000000, 3.000000, 52.000000, 119.000000, 52.000000, 
            15.000000, 2.000000, 0.000000, 0.000000, 0.000000, 11.000000, 21.000000, 33.000000]');
...(totally insert 1,000,000 rows)
```
Export the table using Hyrise console command as table can be imported more easily and quickly.
```txt
export SIFT_BASE [FilePath] # export the table
load [FilePath] SIFT_BASE # import the table
```
Construct an vector index based on the table and column we select.
```sql
create index on sift_base using ivfflat (data, L2) with (nlist=1000, nprobe=3);
```
Search the similar vectors based on the query vector(s).
```sql
select id from sift_base order by data <!> 
    '[1, 3, 11, 110, 62, 22, 4, 0, 43, 21, 22, 18, 6, 28, 64, 9,
      11, 1, 0, 0, 1, 40, 101, 21, 20, 2, 4, 2, 2, 9, 18, 35,
      1, 1, 7, 25, 108, 116, 63, 2, 0, 0, 11, 74, 40, 101, 116, 3, 
      33, 1, 1, 11, 14, 18, 116, 116, 68, 12, 5, 4, 2, 2, 9, 102,
      17, 3, 10, 18, 8, 15, 67, 63, 15, 0, 14, 116, 80, 0, 2, 22,
      96, 37, 28, 88, 43, 1, 4, 18, 116, 51, 5, 11, 32, 14, 8, 23,
      44, 17, 12, 9, 0, 0, 19, 37, 85, 18, 16, 104, 22, 6, 2, 26, 
      12, 58, 67, 82, 25, 12, 2, 2, 25, 18, 8, 2, 19, 42, 48, 11]',
    '[40, 25, 11, 0, 22, 31, 6, 8, 10, 3, 0, 1, 30, 91, 88, 18,
      38, 44, 16, 1, 5, 25, 70, 36, 1, 22, 10, 7, 10, 40, 61, 1, 
      60, 9, 8, 4, 111, 45, 21, 45, 9, 1, 1, 14, 111, 111, 70, 26, 
      95, 13, 3, 2, 3, 39, 111, 111, 20, 3, 11, 11, 1, 32, 70, 22, 
      48, 8, 9, 25, 60, 26, 14, 37, 4, 5, 65, 110, 111, 31, 1, 0, 
      101, 78, 84, 34, 4, 2, 2, 29, 33, 44, 25, 22, 2, 0, 4, 18, 
      54, 51, 24, 21, 12, 18, 5, 6, 11, 17, 100, 65, 50, 92, 37, 14, 
      23, 77, 95, 9, 3, 14, 60, 40, 4, 30, 23, 32, 10, 3, 19, 13]' 
limit 100;
```
There will be a file in `./hyrise/cmake-build-debug/sift_base-output.txt` containing all the ids for similar vectors storing in table **SIFT_BASE**. User can check the results with ground truth (stored in the downloaded file).

# Experiment results

<img src="./hyrise_search_generalized.png" width = 70% height = 70% alt="search" align=center />

<img src="./hyrise_build_general.png" width = 70% height = 70% alt="build" align=center />

<img src="./hyrise_size_general.png" width = 70% height = 70% alt="size" align=center />

# Conclusion

In this paper, we present HyriseVector, a generalized vector database that matches the performance of specialized vector databases in search, index construction, and index size. More importantly, HyriseVector improves the performance of existing generalized vector databases by one to two orders of magnitude.
