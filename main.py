# file management

import os
import glob
import time

#file saving
import json

#utilities
from nltk.util import ngrams
from collections import Counter
from tqdm import tqdm
from itertools import islice

import re
import time

#multiprocessing
import psutil
from multiprocessing import Pool
import multiprocessing
import itertools


WORKING_DIR = '/content/drive/MyDrive/generate_n_grams'


def list_files(dir):
    """
    Lists the files int the directory
    """                                                                                                  
    r = []                                                                                                            
    subdirs = [x[0] for x in os.walk(dir)]                                                                            
    for subdir in subdirs:                                                                                            
        files = os.walk(subdir).next()[2]                                                                             
        if (len(files) > 0):                                                                                          
            for file in files:                                                                                        
                r.append(os.path.join(subdir, file))                                                                         
    return r


def generate_directories(working_directory = WORKING_DIR):
    """
    Generates the required directories in the WORKING DIR
    """
    dir_name= WORKING_DIR + '/ngrams_multiprocessing_nosave_benchmark/'
  
    for i in range(2,7):
      os.makedirs(dir_name + str(i))
    

def get_counts(counter_file):
    """Get counts as a dictionary from a counter.

    Args:
        counter (Counter): Counter of ngrams

    Returns:
        dict: reference_dict containing the ngrams
    """
    reference_dict = {}
  
    for item in counter_file.most_common():
      text= ' '.join(item[0])
      count = item[1]
      if not reference_dict.get(text,""):
        reference_dict[text] = count
    return reference_dict


def count_in_text_local(batch_num, text, dir_name_template, i):
    """Counts the text in the given batch of text.

    Args:
        batch_num (str/int): batch number for the file
        text (str): The lines in the given batch
        dir_name_template (str): template for the directory name
        i (int): the n-th gram from 2-6 grams
    """
    ngram_counts = Counter(ngrams(text.split(), i))
    temp_dict = get_counts(ngram_counts)
    dir_name = dir_name_template + str(i)
    with open(dir_name + '/{}.json'.format(batch_num), 'w') as fp:
      json.dump(temp_dict, fp)
    print('saved batch {batch_num} for {i} grams'.format(batch_num=batch_num, i=i))

def get_ngram_dicts(filename, save_dir):
    """Get ngram counts as a folder of dictionaries from a given text file.
    To save memory, the code reads the file line by line then per each batch of 
    100K lines, extracts ngram counts from the batches. 

    Args:
        filename (str): Directory of the corpus file
        save_dir (str): Directory to save the file
    """
    #ngram_dicts = {2: [], 3:[], 4:[], 5:[], 6:[]}
    with open(filename) as fp:
        text = ''
        count = 0
        segment_number = 1
        grams_n = [2,3,4,5,6]
        while True:
          line = fp.readline()
          if not line:
            if segment_number == 1:
              file_name = 'total_n_gram'
              params = [(file_name, text, save_dir, num) for num in grams_n]
              with Pool(processes=8) as pool:
                pool.starmap(count_in_text_local, params)
              #count_in_text_local('total_n_gram', text ,save_dir)
            else:
              file_name = segment_number
              params = [(file_name, text, save_dir, num) for num in grams_n]
              with Pool(processes=8) as pool:
                pool.starmap(count_in_text_local, params)
              #count_in_text_local(segment_number, text ,save_dir)
            #process_files_dict(save_dir)
            break
          text += line
          count += 1
          if count == 100000:
            if segment_number == 1:
              file_name = 'total_n_gram'
              params = [(file_name, text, save_dir, num) for num in grams_n]
              with Pool(processes=8) as pool:
                pool.starmap(count_in_text_local, params)
              #count_in_text_local('total_n_gram', text ,save_dir)
            else:
              file_name = segment_number
              params = [(file_name, text, save_dir, num) for num in grams_n]
              with Pool(processes=8) as pool:
                pool.starmap(count_in_text_local, params)
              #count_in_text_local(segment_number, text ,save_dir)
            #process_files_dict(save_dir)
            count = 0
            segment_number += 1 
            text = ''


def main():
    dir_name = WORKING_DIR + '/ngrams_multiprocessing_nosave_benchmark/'
    file_name =  WORKING_DIR + "/wikimatrix.en" #or insert filename here
    get_ngram_dicts(file_name, dir_name)



#Start the process
start = time.time()
print("process starting")
main()
end = time.time()
print(end - start)
