from functools import lru_cache
import pickle, bz2
import _pickle as cPickle
import json
import sys
import pandas as pd
import csv
import os

#This function saves dictionaries as pickle files in the storage.
def saveDictionaryAsPickleFile(dictionary, dictionaryPath):
    if dictionaryPath.rsplit(".")[-1] == "pickle":
        filePointer=open(dictionaryPath, 'wb')
        pickle.dump(dictionary,filePointer, protocol=pickle.HIGHEST_PROTOCOL)
        filePointer.close()
    else:
        with bz2.BZ2File(dictionaryPath, "w") as f: 
            cPickle.dump(dictionary, f)

#load csv file as a dictionary. Further preprocessing may be required after loading
def loadDictionaryFromCsvFile(filePath):
    if(os.path.isfile(filePath)):
        with open(filePath) as csv_file:
            reader = csv.reader(csv_file)
            dictionary = dict(reader)
        return dictionary
    else:
        print("Sorry! the file is not found. Please try again later. Location checked:", filePath)
        sys.exit()
        return 0

#to read json input files where, the data are stored with relation as key.
def readJson(filename):
    with open(filename) as f:
        data = json.load(f)
        df = pd.DataFrame(data["relation"])
        df_transposed = df.T
        # To use the first row as the header row:
        #new_header = df_transposed.iloc[0]
        df_transposed = df_transposed[1:]
        #df_transposed.columns = new_header
    return df_transposed

#load the pickle file as a dictionary
@lru_cache(maxsize=None)
def loadDictionaryFromPickleFile(dictionaryPath):
    print("Loading dictionary at:", dictionaryPath)
    if dictionaryPath.rsplit(".")[-1] == "pickle":
        filePointer=open(dictionaryPath, 'rb')
        dictionary = pickle.load(filePointer)
        filePointer.close()
    else:
        dictionary = bz2.BZ2File(dictionaryPath, "rb")
        dictionary = cPickle.load(dictionary)
    print("The total number of keys in the dictionary are:", len(dictionary))
    return dictionary


global label_dict, type_dict, class_dict, fact_dict
global yago_inverted_index, yago_relation_index, main_index_triples
global synth_type_kb, synth_relation_kb, synth_type_inverted_index, synth_relation_inverted_index
YAGO_PATH = r"yago/"
LABEL_FILE_PATH = YAGO_PATH + "yago-wd-labels_dict.pickle" 
TYPE_FILE_PATH = YAGO_PATH + "yago-wd-full-types_dict.pickle" 
CLASS_FILE_PATH = YAGO_PATH + "yago-wd-class_dict.pickle"
FACT_FILE_PATH = YAGO_PATH + "yago-wd-facts_dict.pickle"

YAGO_MAIN_INVERTED_INDEX_PATH = r"santos/hashmap/dialite_datalake_main_yago_index.pickle"
YAGO_MAIN_RELATION_INDEX_PATH = r"santos/hashmap/dialite_datalake_main_relation_index.pickle"
YAGO_MAIN_PICKLE_TRIPLE_INDEX_PATH = r"santos/hashmap/dialite_datalake_main_triple_index.pickle"

SYNTH_TYPE_KB_PATH =   r"santos/hashmap/dialite_datalake_synth_type_kb.pbz2"
SYNTH_RELATION_KB_PATH =   r"santos/hashmap/dialite_datalake_synth_relation_kb.pbz2"
SYNTH_TYPE_INVERTED_INDEX_PATH = r"santos/hashmap/dialite_datalake_synth_type_inverted_index.pbz2"
SYNTH_RELATION_INVERTED_INDEX_PATH = r"santos/hashmap/dialite_datalake_synth_relation_inverted_index.pbz2"

label_dict = loadDictionaryFromPickleFile(LABEL_FILE_PATH)
type_dict = loadDictionaryFromPickleFile(TYPE_FILE_PATH)
class_dict = loadDictionaryFromPickleFile(CLASS_FILE_PATH)
fact_dict = loadDictionaryFromPickleFile(FACT_FILE_PATH)

yago_inverted_index = loadDictionaryFromPickleFile(YAGO_MAIN_INVERTED_INDEX_PATH)
yago_relation_index = loadDictionaryFromPickleFile(YAGO_MAIN_RELATION_INDEX_PATH)
main_index_triples = loadDictionaryFromPickleFile(YAGO_MAIN_PICKLE_TRIPLE_INDEX_PATH)

synth_type_kb = loadDictionaryFromPickleFile(SYNTH_TYPE_KB_PATH)
synth_relation_kb = loadDictionaryFromPickleFile(SYNTH_RELATION_KB_PATH)
synth_type_inverted_index= loadDictionaryFromPickleFile(SYNTH_TYPE_INVERTED_INDEX_PATH)
synth_relation_inverted_index = loadDictionaryFromPickleFile(SYNTH_RELATION_INVERTED_INDEX_PATH)