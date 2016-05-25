"""
LSH spark implementation.

Usage:
  `spark-submit --executor-memory 10G --driver-memory 8G
   LSH.py inputfile p num_hahs num_buckets num_bands `
"""

import numpy as np
import functools
import sys

from pyspark import SparkContext


class LHS_Model():
      def __init__(self, p, num_hash, num_buckets, num_bands):
          """
          :param p: modulo, should be larger than the number of features
          :param num_hash: number of hashing functions
          :param num_buckets: number of buckets for hashing
          :param num_bands: number of bands
          """
          self.p = p
          self.num_hash = num_hash
          self.num_buckets = num_buckets
          self.num_bands = num_bands


      def _min_hash(self,a,b,v_idx):
          return np.array((((a * v_idx) + b) % self.p)\
                          % self.num_buckets).min()


      def _hash_functions(self):
          seeds = np.random.randint(self.p, size=(self.num_hash, 2))
          hash_funs = [functools.partial(self._min_hash,
                                          a=seed[0],
                                          b=seed[1])
                        for seed in seeds]
          return hash_funs


      def fit(self, data):
          """

          :param data:
          :return:
          """
          data = data.zipWithIndex()
          hash_funs = self._hash_functions(self)

          ### return RDD of (data_idx, band_idx): min_hash_value
          data_new = data.flatMap(lambda (v, data_idx):
                                            [[(data_idx,
                                               i % self.num_bands),
                                               hash_fun(v)] for i, hash_fun
                                   in enumerate(hash_funs)])\
                         .cache()


          ### return RDD (band_idx, hash_value): data_idx list

          data_bands = data_new.groupByKey()\
                               .map(lambda ((data_idx, band_idx),v):
                                           [(band_idx,
                                             hash(frozenset(v))),
                                             data_idx])\
                               .groupByKey()\
                               .cache()

          ### return RDD of (data_idx, bucket_idx)
          data_bucket = data_bands.map(lambda ((band_idx, hash_value),
                                              data_idx_list):
                                       frozenset(sorted(data_idx_list))).distinct() \
                                  .zipWithIndex()\
                                  .flatMap(lambda data_idx_list, bucket_idx:
                                                 map(lambda x: (np.long(x),
                                                                data_idx_list),
                                                     bucket_idx)) \
                                  .cache()


          ### return RDD of (bucket_idx, data_idx)
          bucket_data = data_bucket.map(lambda (x,v): (v,x))
          self.data_bucket = data_bucket
          self.bucket_data = bucket_data


if __name__ == "__main__":
   sc = SparkContext(appName='PySparkLSH')
   input_file = sys.argv[1]
   p = sys.argv[2]
   num_hash = sys.argv[3]
   num_buckets = sys.argv[4]
   num_bands = sys.argv[5]
   data = sc.textFile(input_file, 10)
   LSH_model = LHS_Model(p,num_hash, num_buckets, num_bands)
   LSH_model.fit(data)
   sc.stop()

