# This script preprocesses the data lake for lsh ensemble index. The built index is saved for online phase.

from datasketch import MinHashLSHEnsemble, MinHash
import glob
import pandas as pd
import os
import signal
import pickle, bz2
import _pickle as cPickle

def timeout_handler(signum, frame):
    raise TimeoutError("indexing process timed out.")

#This function takes a column and determines whether it is text or numeric column
#This has been done using a well-known information retrieval technique
#Check each cell to see if it is text. Then if enough number of cells are 
#text, the column is considered as a text column.
def getColumnType(attribute, column_threshold=.5, entity_threshold=.5):
    attribute = [item for item in attribute if str(item) != "nan"]
    if len(attribute) == 0:
        return 0
    strAttribute = [item for item in attribute if type(item) == str]
    strAtt = [item for item in strAttribute if not item.isdigit()]
    for i in range(len(strAtt)-1, -1, -1):
        entity = strAtt[i]
        num_count = 0
        for char in entity:
            if char.isdigit():
                num_count += 1
        if num_count/len(entity) > entity_threshold:
            del strAtt[i]            
    if len(strAtt)/len(attribute) > column_threshold:
        return 1
    else:
        return 0
# collect the columns within the whole data lake for lsh ensemble.
def collect_columns(folder_path):
    csv_files = glob.glob(folder_path)
    all_columns = []
    column_dict = {}
    col_counter = 0
    tab_counter = 0
    for file in csv_files:
        timeout_duration = 10
        # Register the timeout handler function
        signal.signal(signal.SIGALRM, timeout_handler)
        # Set the alarm to trigger the timeout handler after the specified duration
        signal.alarm(timeout_duration)
        try:
            df = pd.read_csv(file)
            table_name = file.rsplit(os.sep,1)[-1]
            for column in df.columns:
                if getColumnType(df[column].tolist()) != 1: #check column Type
                    continue
                column_data = set(df[column].map(str))
                all_columns.append(column_data)
                column_dict[len(all_columns) - 1] = (table_name, column) # len(all_columns) - 1
                col_counter += 1
        finally:
            signal.alarm(0)
        tab_counter += 1
    print(f"Done! Total cols: {col_counter}, Tab_counter = {tab_counter}")
    return all_columns, column_dict

# preprocess data lake for lsh _ensemble.
def preprocess_lsh_ensemble(all_columns, threshold = 0.7, num_perm = 128, num_part = 32):
    # Create MinHash objects
    lsh_arrays= []
    for i in range (0,len(all_columns)):
        lsh_arrays.append(MinHash(num_perm=128))

    # update lsh arrays
    for i, col in enumerate(all_columns):
        for d in col:
            lsh_arrays[i].update(str(d).encode("utf8"))

    # Create an LSH Ensemble index with threshold and number of partition settings.
    lshensemble = MinHashLSHEnsemble(threshold=threshold, num_perm=num_perm,num_part=num_part)
    # Index takes an iterable of (key, minhash, size)
    iter_list = []
    for id, item in enumerate(lsh_arrays):
        iter_list.append((str(id), item, len(all_columns[id])))
    # lshensemble.index([("m2", m2, len(set2)), ("m3", m3, len(set3))])
    lshensemble.index(iter_list)
    return lshensemble

#This function saves dictionaries as pickle files in the storage.
def saveDictionaryAsPickleFile(dictionary, dictionaryPath):
    if dictionaryPath.rsplit(".")[-1] == "pickle":
        filePointer=open(dictionaryPath, 'wb')
        pickle.dump(dictionary,filePointer, protocol=pickle.HIGHEST_PROTOCOL)
        filePointer.close()
    else:
        with bz2.BZ2File(dictionaryPath, "w") as f: 
            cPickle.dump(dictionary, f)

datalake_path = "../data/dialite_datalake/*.csv"
all_columns, column_dict = collect_columns(datalake_path)
preprocessed_lshensemble = preprocess_lsh_ensemble(all_columns)
saveDictionaryAsPickleFile(preprocessed_lshensemble, "hashmap/dialite_preprocessed_lshensemble")
saveDictionaryAsPickleFile(column_dict, "hashmap/dialite_lsh_ensemble_column_dict")
