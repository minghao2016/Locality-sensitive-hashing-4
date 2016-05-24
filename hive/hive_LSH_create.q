-- create tables and functions
-- load raw data into raw table
add jar /home/hadoop/brickhouse/target/brickhouse-0.7.1-SNAPSHOT.jar;

CREATE TEMPORARY FUNCTION combine AS 'brickhouse.udf.collect.CombineUDF';
CREATE TEMPORARY FUNCTION combine_unique AS 'brickhouse.udf.collect.CombineUniqueUDAF';
CREATE TEMPORARY FUNCTION array_intersect AS "brickhouse.udf.collect.ArrayIntersectUDF";

CREATE EXTERNAL TABLE raw_shingles (f_id STRING, shingle STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

CREATE EXTERNAL TABLE shingles_hashed (f_id STRING, hash BIGINT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

CREATE TABLE random_xor (minhash_id INT, band_id INT, rand_xor BIGINT)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

CREATE EXTERNAL TABLE 
min_hashes (f_id STRING, minhash_id INT, band_id INT, min_hash BIGINT)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

CREATE EXTERNAL TABLE 
hash_bands (f_id STRING, band_id INT, hash_band ARRAY<BIGINT>)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

CREATE EXTERNAL TABLE 
hash_arrays (f_id STRING, hash_array ARRAY<BIGINT>)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

CREATE EXTERNAL TABLE 
cand_pair (f_id_a STRING, f_id_b STRING, similar_hash_bands INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH '/small_data/'
OVERWRITE INTO TABLE raw_shingles;

LOAD DATA INPATH '/large_data/'
OVERWRITE INTO TABLE raw_shingles;

