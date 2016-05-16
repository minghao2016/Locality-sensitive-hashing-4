import os
import re
import string
import time
from itertools import combinations

def n_shingle_set(word_list, shingle_n):
    # input: a list of string for text
    # output: a set of ngram extracted from the text
    shingle_list = ['#'.join(word_list[i:i+shingle_n])
                    for i in range(len(word_list)-shingle_n+1)]
    return set(shingle_list)

def replace_nonword(text):
    # replace both punctuation and non-ASCII characters
    # return filter(lambda x:x not in string.punctuation, text)
    return re.sub(r'[^\x00-\x7F]','',
                  filter(lambda x:x not in string.punctuation, text))

def file_to_shingle_list(path, shingle_n):
    # input: file directory
    # output: a list of tuples of (file_name, feature_vector)
    file_shingle_dict = {}
    for file_name in os.listdir(path):
        with open(os.path.join(path, file_name), 'r') as f:
            text = replace_nonword(f.read().lower())
            # print text[:20]
            word_list = re.split("[ \n]+", text)
            file_shingle_dict[file_name.replace('.txt','')] = \
                n_shingle_set(word_list, shingle_n)
    return file_shingle_dict

def jaccard_sim(set1, set2):
    intx = len(set1&set2)
    return intx / float(len(set1) + len(set2) - intx)
    
if __name__ == "__main__":
    path = 'corpus-20090418'
    shingle_n = 2
    start_time = time.time()
    file_shingle_dict = file_to_shingle_list(path, shingle_n)

    outlist = map(lambda x: (x[0],x[1], "%0.5f" %
                             jaccard_sim(
                                 file_shingle_dict[x[0]],
                                 file_shingle_dict[x[1]]
                             )),
                  combinations(file_shingle_dict.keys(), 2))
    outlist.sort(key=lambda line: line[2], reverse=True)
    output_text = '\n'.join([','.join(ele) for ele in outlist])
    with open('j_sim_local_ykw.csv', 'w') as fw:
        result_writer = fw.write(output_text)
    print("--- %s seconds ---" % (time.time() - start_time))

