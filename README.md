# Are There Fundamental Limitations in Supporting Vector Data Management in Relational Databases? A Case Study of PostgreSQL

High-dimensional vector data is gaining increasing importance in data science applications. Consequently, various database systems have recently been developed to manage vector data. 
These systems can be broadly categorized into two types: specialized and generalized vector databases. Specialized vector databases are explicitly designed and optimized for storing and querying vector data, while generalized vector databases support vector data management within a relational database like PostgreSQL. It is expected (and confirmed by our experiments) that generalized vector databases exhibit slower performance. However, two key questions remain unanswered: (1) What are the limitations for relational databases to efficiently support vector data management? (2) How to overcome these limitations and build a new vector database within a relational database that achieves performance comparable to a highly optimized specialized vector database? 

To address the first question, we chose PostgreSQL as a representative relational database due to its popularity. We focused on Alibaba PASE, as it is a PostgreSQL-based vector database and is considered state-of-the-art among generalized vector databases. We analyzed the source code of PASE and compared its performance with the Faiss to identify the underlying root causes to the low performance in PASE. 

Based on these root causes and the lessons learned, we developed a novel generalized vector database termed HyriseVector, which is built within Hyrise, a main-memory relational database. HyriseVector achieves performance comparable to the highly optimized specialized vector database in search, index construction, and index size, thus answering the second question. In particular, HyriseVector improves existing generalized vector databases by up to 40×


# Repository Contents

## **postgresql-11.0** 
This directory contains the source code distribution of the PostgreSQL
database management system.


## **Faiss** 
This directory contains the source code of Faiss, a library for efficient similarity search and clustering of dense vectors

* **faiss/IndexIVFFlat.cpp:** Faiss index IVF_FLAT implementation

* **faiss/IndexIVFPQ.cpp:** Faiss index IVF_PQ implementation

* **faiss/IndexHNSW.cpp:** Faiss index HNSW implementation


## **PASE**

Code of PASE is in the directory **postgresql-11.0/contrib/pase**. 

* **ivfflat:** PASE index IVF_FLAT implementation

* **ivfpq:** PASE index IVF_PQ implementation

* **hnsw:** PASE index HNSW implementation

* **sql:** Sample SQL file for PASE

* **type:** Data types used in PASE

* **utils:** Util functions used in PASE

We implemented index IVF_PQ in PASE and the code is in **postgresql-11.0/contrib/pase/ivfpq**.

# Prerequisite

OpenMP 4.0.1

# Getting the Source
`git clone https://github.com/Anonymous-Vec/Vec-Exp.git`

## How to use PASE

### Start PostgreSQL

#### Checking the Required Environment

`sudo apt-get install build-essential libreadline-dev zlib1g-dev flex bison libxml2-dev libxslt-dev libssl-dev libxml2-utils xsltproc ccache`

`wget https://ftp.postgresql.org/pub/source/v11.0/postgresql-11.0.tar.gz`

`tar -zxvf postgresql-11.0.tar.gz`

Code of PG11 here can not be used directly since some importance files are not uploaded by git.

`cd postgresql-11.0`

#### Configure

`mkdir build`

`./configure --prefix=$/absolute/path/build CFLAGS="-O3" LDFLAGS="-fPIC -fopenmp" `

#### Compile
`make`

`make install`


#### Initial new cluster named "data" on a folder:

`build/bin/initdb -D data`

#### Set the size of shared buffer in postgresql-11.0/build/data/postgresql.conf/:
`shared_buffers = 160GB`

### Start PASE

`cd contrib/pase`

#### May need to change the Makefile:
`PG_CONFIG=postgresql-11.0/build/bin/pg_config`
#### Compile the PASE
`make USE_PGXS=1`

`make install`
	
#### Start the cluster:
`cd ../..`

`build/bin/pg_ctl -D data start` 
#### Create a database named "pasetest"
`build/bin/createdb -p 5432 pasetest`
#### Connect the database
`build/bin/psql -p 5432 pasetest`

### EXAMPLE CODE Used in Psql Command Line
#### Create Extension

`create extension pase;`

#### Create Table

`CREATE TABLE vectors_ivfflat_test ( id serial, vector float4[]);`

```
INSERT INTO vectors_ivfflat_test SELECT id, ARRAY[id
       ,1,1,1,1,1,1,1,1,1,1
       ,1,1,1,1,1,1,1,1,1,1
       ,1,1,1,1,1,1,1,1,1,1
       ,1,1,1,1,1,1,1,1,1,1
       ,1,1,1,1,1,1,1,1,1,1
       ,1,1,1,1,1,1,1,1,1,1
       ,1,1,1,1,1,1,1,1,1,1
       ,1,1,1,1,1,1,1,1,1,1
       ,1,1,1,1,1,1,1,1,1,1
       ,1,1,1,1,1,1,1,1,1,1
       ,1,1,1,1,1,1,1,1,1,1
       ,1,1,1,1,1,1,1,1,1,1
       ,1,1,1,1,1,1,1,1,1,1
       ,1,1,1,1,1,1,1,1,1,1
       ,1,1,1,1,1,1,1,1,1,1
       ,1,1,1,1,1,1,1,1,1,1
       ,1,1,1,1,1,1,1,1,1,1
       ,1,1,1,1,1,1,1,1,1,1
       ,1,1,1,1,1,1,1,1,1,1
       ,1,1,1,1,1,1,1,1,1,1
       ,1,1,1,1,1,1,1,1,1,1
       ,1,1,1,1,1,1,1,1,1,1
       ,1,1,1,1,1,1,1,1,1,1
       ,1,1,1,1,1,1,1,1,1,1
       ,1,1,1,1,1,1,1,1,1,1
       ,1,1,1,1,1
       ]::float4[] FROM generate_series(1, 50000) id;
```

#### Build Index

```
CREATE INDEX v_ivfflat_idx ON vectors_ivfflat_test
       USING
         pase_ivfflat(vector)
  WITH
    (clustering_type = 1, distance_type = 0, dimension = 256, clustering_params = "10,100");
```

#### Search Index

```
SELECT vector <#> '31111,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1'::pase as distance
    FROM vectors_ivfflat_test
    ORDER BY
    vector <#> '31111,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1'::pase
     ASC LIMIT 10;
```


### Original PASE Code:

- [Pase: PostgreSQL Ultra-High Dimensional Approximate Nearest Neighbor Search Extension](https://github.com/alipay/PASE)

## How to use Faiss

### Prerequisite

C++11 compiler (with support for OpenMP support version 2 or higher)


BLAS implementation (we strongly recommend using Intel MKL for best performance).

`sudo apt install intel-mkl`

`sudo apt-get install -y libopenblas-dev` 


CMake minimum required(VERSION 3.17)


### Compile and Build:
`cd faiss`

`cmake -B build . -DFAISS_ENABLE_GPU=OFF -DFAISS_ENABLE_PYTHON=OFF -DCMAKE_BUILD_TYPE=Release -DFAISS_OPT_LEVEL=generic `

`make -C build -j faiss`  

`sudo make -C build install` 

`make -C build 2-IVFFlat`

### Run the Example Code:
`./build/tutorial/cpp/2-IVFFlat` 

### Original Faiss Code:

- [Faiss: A Library for Efficient Similarity Search and Clustering of Dense Vectors](https://github.com/facebookresearch/faiss)

# Comparison Results

## Evaluating Index Construction

### IVF_FLAT
![plot](VecDB/results/ivfflat_build.png)
### IVF_PQ
![plot](VecDB/results/ivfpq_build.png)
### HNSW
![plot](VecDB/results/HNSW_build.png)


## Evaluating Index Size

### IVF_FLAT
![plot](VecDB/results/ivfflat_indexSize.png)
### IVF_PQ
![plot](VecDB/results/ivfpq_indexSize.png)
### HNSW
![plot](VecDB/results/HNSW_indexSize.png)

## Evaluating Search Performance

### IVF_FLAT
![plot](VecDB/results/ivfflat_SearchTime.png)
### IVF_PQ
![plot](VecDB/results/ivfpq_SearchTime.png)
### HNSW
![plot](VecDB/results/HNSW_SearchTime.png)



## We summarize the root causes of the performance gap as follows:

* **RC#1: SGEMM Optimization.**

* **RC#2: Tuple Accesses.**

* **RC#3: Parallel Execution.**

* **RC#4: Space Utilization.**

* **RC#5: K-means Implementation.**

* **RC#6: Heap Size in Top-k Computation.**

* **RC#7: Precomputed Table Implementation.**

## Future Direction: How to Bridge the Gap?

A follow-up of the work is how to overcome the root causes? In other words, how to build a new generalized vector database in the future that achieves comparable performance to the state-of-the-art specialized vector database? We show a few actionable guidelines and we are currently working on it.
* Step#1: Start from PostgreSQL-based PASE (or other relational databases). In order to overcome RC#2, there are different solutions. The first solution is to start from PASE, which is based on PostgreSQL. But we need to optimize HNSW by embedding the actual vector data to the index, which can avoid unnecessary random accesses to fetch vector data during graph traversal. The second solution is to add a memory-optimized table in PostgreSQL following GaussDB or use a main-memory relational database (e.g., MonetDB) to directly access tuples (vectors) in memory to reduce the overhead of tuple accesses.
* Step#2: Enable SGEMM. The system shall enable SGEMM to bypass the overhead of RC#1 and significantly improve the performance of index construction.
* Step#3: Optimized top-k computation. The system shall use the proper heap size (i.e., k) for top-k computation to overcome the overhead introduced by RC#6.
* Step#4: Parallelism. The system shall efficiently support both index construction and index search with multiple threads. This requires the implementation of the operator-level (e.g., vector search) parallelism in relational databases, which can bridge the performance gap due to RC#3.
* Step#5: More optimized implementations. The system needs to reduce space amplification, support optimized K-means and precomputated table as mentioned in RC#4, RC#5, and RC#7.

## Overall Message. 

The overall conclusion of the work is that, with a careful implementation, it is feasible to support vector data management inside a relational database that achieves comparable performance to the state-of-the-art specialized vector database. We do not see a fundamental limitation in using a relational database to support efficient vector data management. In this way, we can use a single relational database to support more applications that involve tables and vectors. The paper lays out seven useful root causes with actionable guidelines to build a generalized vector database step by step to achieve both high performance and generality in the future.

# HyriseVector

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

<img src="./TODS_build.png" width = 70% height = 70% alt="search" align=center />

<img src="./TODS_search.png" width = 70% height = 70% alt="build" align=center />

<img src="./TODS_size.png" width = 70% height = 70% alt="size" align=center />

# Conclusion

In this paper, we present HyriseVector, a generalized vector database that matches the performance of specialized vector databases in search, index construction, and index size. More importantly, HyriseVector improves the performance of existing generalized vector databases by one to two orders of magnitude.
