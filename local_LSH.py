import os
import re
import string
import time
from itertools import combinations
import random
import mmh3


def n_shingle_list(word_list, shingle_n):
    # convert word list to shingle list
    # input: a list of string for text
    # output: a set of ngram extracted from the text
    shingle_list = ['#'.join(word_list[i:i+shingle_n])
                    for i in range(len(word_list)-shingle_n+1)]
    return shingle_list


def replace_nonword(text):
    # replace both punctuation and non-ASCII characters
    return re.sub(r'[^\x00-\x7F]','',
                  filter(lambda x:x not in string.punctuation, text))


def seed_gen(n_hashes):
    # generate a list of hash seeds
    return [random.randint(0, 2**32-1) for _ in range(n_hashes)]


def min_hash(shingle_list, seed_list):
    # convert shingle list to minhash list
    init_hash = [mmh3.hash(x) for x in shingle_list]
    out_put_list = range(len(seed_list))
    for i in range(len(seed_list)):
        out_put_list[i] = min([ele^seed_list[i] for ele in init_hash])
        
    return out_put_list


def jaccard_sim(set1, set2):
    # calculate jaccard similarity
    intx = len(set1&set2)
    return float(intx) / (len(set1) + len(set2) - intx)


def file_to_minhash_list(path, shingle_n, seed_list):
    # convert each file to minhash list, store in dictionary
    # input: file directory
    # output: a list of tuples of (file_name, feature_vector)
    file_minhash_dict = {}
    for file_name in os.listdir(path):
        with open(os.path.join(path, file_name), 'r') as f:
            text = replace_nonword(f.read().lower())
            word_list = re.split("[ \n]+", text)
            shingle_list =\
                n_shingle_list(word_list, shingle_n)
            minhash_list = min_hash(shingle_list, seed_list)
            file_minhash_dict[file_name.replace('.txt','')] = \
                minhash_list
    return file_minhash_dict

def assign_band(hash_number, n_bands):
    # assign hash band to minhash ids
    return [[i for i in range(hash_number) if i % n_bands == j] for j in range(n_bands)]


def hash_band(hash_list, band_assignment):
    # split minhashes into hash bands
    return [tuple(sorted(set([hash_list[j] for j in band_assignment[i]]))) 
            for i in range(len(band_assignment))]


def cand_pair_search(file_hashband_dict, n_bands):
    # search for candidate pairs
    # group files by hash array in each band
    # return a set of candidate pairs
    cand_pairs = []
    for i in range(n_bands):
        cache = {}
        for k, v in file_hashband_dict.items():
            cache.setdefault(v[i], []).append(k)
        for v1 in cache.values():
            if len(v1)!=1:
                cand_pairs += [tuple(sorted(ele)) for ele in combinations(v1, 2)]
    cand_pairs = set(cand_pairs)
    return cand_pairs


if __name__ == '__main__':
    path = 'intrinsic-detection-corpus-txt'
    shingle_n = 8
    random.seed(88)
    hash_number = 200
    n_bands = 100

    # start timer
    start_time = time.time()
    seed_list = seed_gen(hash_number)
    print 'generated hash seed list'

    file_minhash_dict = file_to_minhash_list(path, shingle_n, seed_list)
    print 'mapped file to minhashes'

    band_assignment = assign_band(hash_number, n_bands)
    file_hashband_dict = {k:hash_band(v, band_assignment) for k,v in file_minhash_dict.items()}
    print 'mapped file hashbands'

    cand_pairs = cand_pair_search(file_hashband_dict, n_bands)
    print 'searched candidate pairs'

    # calculat j-sim for each candidate pair
    outlist = map(lambda x: (x[0],x[1], "%0.5f" %
                             jaccard_sim(
                                 set(file_minhash_dict[x[0]]),
                                 set(file_minhash_dict[x[1]])
                             )),
                  cand_pairs)
    outlist.sort(key=lambda line: line[2], reverse=True)
    print 'output j similarities'

    time_record = "--- %s seconds ---" % (time.time() - start_time)
    print time_record


    with open('time_record.txt', 'w') as fw:
        fw.write(time_record)

    # store j sim to file
    with open('j_sim_local_cand_hash%d.csv' % hash_number, 'a') as fw:
        for line in [','.join(ele) for ele in outlist]:
            fw.write(line+'\n')

