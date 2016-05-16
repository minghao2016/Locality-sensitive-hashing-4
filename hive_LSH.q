--install maven:http://stackoverflow.com/questions/12076326/how-to-install-maven2-on-redhat-linux
--clone brickhouse and compile jar: https://github.com/klout/brickhouse
--cd brickhouse/
--mvn package
add jar /home/hadoop/brickhouse/target/brickhouse-0.7.1-SNAPSHOT.jar;

CREATE FUNCTION combine AS 'brickhouse.udf.collect.CombineUDF';
CREATE FUNCTION combine_unique AS 'brickhouse.udf.collect.CombineUniqueUDAF';
CREATE FUNCTION array_intersect AS "brickhouse.udf.collect.ArrayIntersectUDF";

--1. copy the file to hdfs
--hadoop fs -cp s3://aws-logs-783442766762-us-west-2/elasticmapreduce/j-1KNLB00P17ZJN/data/plagirism_corpus_tr_8_shingle /8_shingle

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


LOAD DATA INPATH '/8_shingle/'
OVERWRITE INTO TABLE raw_shingles;


INSERT OVERWRITE TABLE shingles_hashed
SELECT f_id, hash(shingle) FROM raw_shingles;


-- 10 bands, 50 hashes
INSERT OVERWRITE TABLE random_xor
SELECT
ROW_NUMBER() OVER () AS minhash_id,
pmod(ROW_NUMBER() OVER () - 1, 10) AS band_id,
floor(rand() * 4294967295) AS rand_xor
FROM
shingles_hashed
LIMIT 50;


INSERT OVERWRITE TABLE min_hashes 
SELECT
h.f_id, x.minhash_id, x.band_id,
MIN(h.hash ^ x.rand_xor) AS min_hash
FROM
shingles_hashed h CROSS JOIN
random_xor x
GROUP BY
h.f_id, x.minhash_id, x.band_id;


INSERT OVERWRITE TABLE hash_bands
SELECT
f_id, band_id,
sort_array(collect_set(min_hash)) AS hash_band
FROM min_hashes
GROUP BY f_id, band_id;

INSERT OVERWRITE LOCAL DIRECTORY '/home/hadoop/cand_pair'
row format delimited 
fields terminated by ','
SELECT * FROM 
(SELECT
a.f_id AS f_id_a, b.f_id AS f_id_b,
COUNT(a.f_id) AS similar_hash_bands
FROM
hash_bands a
INNER JOIN 
hash_bands b 
ON a.band_id = b.band_id 
AND a.hash_band = b.hash_band
WHERE a.f_id < b.f_id
GROUP BY a.f_id, b.f_id) q
ORDER BY similar_hash_bands DESC;


INSERT OVERWRITE TABLE hash_arrays
SELECT
f_id, sort_array(collect_set(min_hash)) AS hash_array
FROM min_hashes
GROUP BY f_id;

INSERT OVERWRITE LOCAL DIRECTORY '/home/hadoop/j_sim'
row format delimited 
fields terminated by ',' 
SELECT * FROM
(SELECT
a.f_id AS f_id_a,
b.f_id AS f_id_b,
size(array_intersect(a.hash_array,b.hash_array))/(size(a.hash_array)+size(b.hash_array)-size(array_intersect(a.hash_array,b.hash_array))) AS j_similarity
FROM
hash_arrays a
CROSS JOIN 
hash_arrays b
WHERE a.f_id < b.f_id) temp
ORDER BY j_similarity DESC;




