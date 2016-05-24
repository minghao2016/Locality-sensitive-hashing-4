-- local sensitive hashing: search for candidate pairs and calculate j similairity

add jar /home/hadoop/brickhouse/target/brickhouse-0.7.1-SNAPSHOT.jar;

-- create array intersect function
CREATE TEMPORARY FUNCTION array_intersect AS "brickhouse.udf.collect.ArrayIntersectUDF";

set hive.cli.print.header=true;

--initial hash: hive default hash used here
--attempted to use python to hash but failed
INSERT OVERWRITE TABLE shingles_hashed
SELECT f_id, hash(shingle) FROM raw_shingles;

--create hash seeds
INSERT OVERWRITE TABLE random_xor
SELECT
ROW_NUMBER() OVER () AS minhash_id,
pmod(ROW_NUMBER() OVER () - 1, 100) AS band_id,
floor(rand() * (pow(2, 30)-1)) AS rand_xor
FROM
(SELECT 1 FROM shingles_hashed LIMIT 200) temp;

--calculate minhashes
INSERT OVERWRITE TABLE min_hashes 
SELECT
h.f_id, x.minhash_id, x.band_id,
MIN(h.hash ^ x.rand_xor) AS min_hash
FROM
shingles_hashed h 
CROSS JOIN
random_xor x
GROUP BY
h.f_id, x.minhash_id, x.band_id;

-- band minhashes
INSERT OVERWRITE TABLE hash_bands
SELECT
f_id, band_id,
sort_array(collect_set(min_hash)) AS hash_band
FROM min_hashes
GROUP BY f_id, band_id;

--search for candidate pairs by grouping by hash arrays in each band
INSERT OVERWRITE TABLE cand_pair 
SELECT
a.f_id AS f_id_a, b.f_id AS f_id_b,
COUNT(a.f_id) AS similar_hash_bands
FROM
hash_bands a
INNER JOIN 
hash_bands b 
ON a.band_id = b.band_id 
AND a.hash_band = b.hash_band
WHERE a.f_id < b.f_id
GROUP BY a.f_id, b.f_id;

--create minhash arrays for each file
INSERT OVERWRITE TABLE hash_arrays
SELECT
f_id, sort_array(collect_set(min_hash)) AS hash_array
FROM min_hashes
GROUP BY f_id;

--calculate j similarity of candidate pairs
INSERT OVERWRITE LOCAL DIRECTORY '/home/hadoop/j_sim_hive_cand_hash'
row format delimited 
fields terminated by ',' 
SELECT * FROM
(SELECT
c.f_id_a,
c.f_id_b,
size(array_intersect(a.hash_array,b.hash_array))/(size(a.hash_array)+size(b.hash_array)-size(array_intersect(a.hash_array,b.hash_array))) AS j_similarity
FROM
cand_pair c 
INNER JOIN 
hash_arrays a
ON c.f_id_a = a.f_id
INNER JOIN
hash_arrays b
ON c.f_id_b = b.f_id) temp
ORDER BY j_similarity DESC;


