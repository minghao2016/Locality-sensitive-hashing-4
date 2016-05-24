-- calculate pairwise j similarity of all files based on minhashes

add jar /home/hadoop/brickhouse/target/brickhouse-0.7.1-SNAPSHOT.jar;

CREATE TEMPORARY FUNCTION array_intersect AS "brickhouse.udf.collect.ArrayIntersectUDF";

set hive.cli.print.header=true;

INSERT OVERWRITE TABLE shingles_hashed
SELECT f_id, hash(shingle) FROM raw_shingles;

INSERT OVERWRITE TABLE min_hashes 
SELECT
h.f_id, x.minhash_id, x.band_id,
MIN(h.hash ^ x.rand_xor) AS min_hash
FROM
shingles_hashed h CROSS JOIN
random_xor x
GROUP BY
h.f_id, x.minhash_id, x.band_id;

INSERT OVERWRITE TABLE hash_arrays
SELECT
f_id, sort_array(collect_set(min_hash)) AS hash_array
FROM min_hashes
GROUP BY f_id;

INSERT OVERWRITE LOCAL DIRECTORY '/home/hadoop/j_sim_hive_hash50'
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



