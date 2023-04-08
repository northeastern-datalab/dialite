# -*- coding: utf-8 -*-


#from tkinter import E
import numpy as np
import pandas as pd
import csv
import glob
import time
import os.path
from pathlib import Path
import sys
import re 
# from nltk.corpus import stopwords
# from nltk import word_tokenize, pos_tag


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
    
#removes punctuations and whitespaces from string. The same preprocessing
#is done in yago label file
def preprocessString(string):
    string =  re.sub(r'[^\w]', ' ', string)
    string = string.replace("nbsp",'')
    string =" ".join(string.split())
    return (string)

#removes punctuations and whitespaces from list items
def preprocessListValues(valueList):
    valueList = [x.lower() for x in valueList if checkIfNullString(x) !=0]
    valueList = [re.sub(r'[^\w]', ' ', string) for string in valueList]
    valueList = [x.replace('nbsp','') for x in valueList ] #remove html whitespace
    valueList = [" ".join(x.split()) for x in valueList]
    return valueList

#checks different types of nulls in string
def checkIfNullString(string):
    nullList = ['nan','-','unknown','other (unknown)','null','na', "", " "]
    if str(string).lower() not in nullList:
        return 1
    else:
        return 0
    
#remove hyphen and whitespaces from the table name
def cleanTableName(string):
    tableName = string.replace("-","")
    tableName = ' '.join(tableName.split("_"))
    tableName = '_'.join(tableName.split())
    return tableName

#needed if direct hit is not found in the KB
def extractNouns(stringList):
    sentence = ' '.join(item for item in stringList)
    nouns = [token for token, pos in pos_tag(word_tokenize(sentence)) if pos.startswith('N')]
    return nouns

#extracts tokens from the cell value or value list
def expandQuery(stringList):
    stringList = [item for item in stringList if type(item) == str]
    stringList = preprocessListValues(stringList)
    nounList = extractNouns(stringList)
    expandedQueryList = [words for segments in nounList for words in segments.split()]
    # handle phrase queries
    removeNouns = []
    for entity in expandedQueryList:
        entityList = entity.split(" ")
        if entityList.count('') > 0 and entityList.count('') <= 2:
            entityList.remove('')
        index = 0
        while index <= len(entityList) - 1:
            word = entityList[index]
            if word in nounList:
                if index + 1 < len(entityList):
                    nextWord = entityList[index + 1]
                    if entityList[index + 1] in nounList:
                        removeNouns.append(word)
                        removeNouns.append(entityList[index + 1])
                        expandedQueryList.append(word + " " + entityList[index + 1])
                        index += 1
            index += 1
                    
    finalNouns = [noun for noun in expandedQueryList if noun not in removeNouns]
    stopWordsRemovedList= [word for word in finalNouns if word.lower() not in stopwords.words('english')]
    return (list(set(stopWordsRemovedList)))


def getMatchingTables(item, weight, parameter):   
    returnList = []
    #print(merge)
    for each in item:
        temp = each
        tableName = temp[0]
        #tableName = cleanTableName(temp[0])
        tableScore = temp[-1] * weight *parameter
        returnList.append((tableName, tableScore))
    return returnList


#compute synthesized CS for the query table
def computeSynthColumnSemantics(input_table, synth_type_kb):
    #synthInvertedIndex  = {}
    all_column_semantics = {}
    col_id = 0
    for (columnName, columnData) in input_table.iteritems():
        sem = {}
        #creating the lookup table for data lake tables
        if getColumnType(input_table[columnName].tolist()) == 1:
            #print(table_name)
            input_table[columnName] = input_table[columnName].map(str)
            valueList = preprocessListValues(input_table[columnName].unique())                   
            hit_found = 0                
            #find bag of semantics for each column
            for value in valueList:
                if value in synth_type_kb:
                    item = synth_type_kb[value]
                    hit_found += 1
                    for temp in item:
                        semName = temp[0]
                        semScore = temp[-1] 
                        if semName in sem:
                            sem[semName] +=semScore
                        else:
                            sem[semName] = semScore
            for every in sem:
                sem[every] = sem[every]/hit_found

            if str(col_id) in all_column_semantics:
                print("red flag!!!")
            else:
                all_column_semantics[str(col_id)] = sem
        col_id += 1
    return all_column_semantics

#compute synthesized relationship semantics for the query table
def computeSynthRelation(inputTable, subjectIndex, synthKB):
    label = "r"
    synth_triple_dict = {}
    total_cols = inputTable.shape[1]
    subject_semantics = set()
    for i in range(0, total_cols -1):    
        if getColumnType(inputTable.iloc[:,i].tolist()) == 1: #the subject in rdf triple should be a text column
            for j in range(i+1, total_cols):
                if getColumnType(inputTable.iloc[:,j].tolist()) == 1: #the subject in rdf triple should be a text column
                    mergeRelSem = {}
                    dataFrameTemp = inputTable.iloc[:,[i,j]]
                    dataFrameTemp = (dataFrameTemp.drop_duplicates()).dropna()
                    projectedRowsNum = dataFrameTemp.shape[0]
                    
                    #find relation semantics for each value pairs of subjectIndex and j
                    for k in range(0,projectedRowsNum):
                        #extract subject and object
                        sub = preprocessString(str(dataFrameTemp.iloc[k][0]).lower())
                        obj = preprocessString(str(dataFrameTemp.iloc[k][1]).lower())
                        subNull = checkIfNullString(sub)
                        objNull = checkIfNullString(obj)
                        if subNull != 0 and objNull != 0:
                            item = []
                            value = sub+"__"+obj
                            if value in synthKB:
                                item = synthKB[value]
                                
                            else:
                                value = obj+"__"+sub
                                if value in synthKB:
                                    item = synthKB[value]
                                   
                            if len(item) > 0 :
                                for each in item:
                                    temp = each
                                    if temp[-1] >0:
                                        semName = temp[0]
                                        semScore = temp[-1] 
                                        if semName in mergeRelSem:
                                            mergeRelSem[semName] +=semScore/projectedRowsNum
                                        else:
                                            mergeRelSem[semName] = semScore/projectedRowsNum
                    

                    triple_list = []
                    for sem in mergeRelSem:
                        triple_list.append((sem, mergeRelSem[sem]))
                            
                    synth_triple_dict[str(i) + "-" + str(j)] = triple_list
                    if int(subjectIndex) == i or int(subjectIndex) == j:
                        for sem in mergeRelSem:
                            subject_semantics.add(sem)
    return synth_triple_dict, subject_semantics
  
#compute KB RS for the query table
def computeRelationSemantics(input_table, tab_id, LABEL_DICT, FACT_DICT):
    total_cols = input_table.shape[1]
    relation_dependencies = []
    entities_finding_relation = {}
    relation_dictionary = {}
    #compute relation semantics
    for i in range(0, total_cols-1):
            #print("i=",i)
        if getColumnType(input_table.iloc[:, i].tolist()) == 1: 
            #the subject in rdf triple should be a text column
            for j in range(i+1, total_cols):
                semantic_dict_forward = {}
                semantic_dict_backward = {}
                column_pairs = input_table.iloc[:, [i, j]]
                column_pairs = (column_pairs.drop_duplicates()).dropna()
                unique_rows_in_pair = column_pairs.shape[0]
                total_kb_forward_hits = 0
                total_kb_backward_hits = 0
                for k in range(0, unique_rows_in_pair):
                    #extract subject and object
                    subject_value = preprocessString(str(column_pairs.iloc[k][0]).lower())
                    object_value = preprocessString(str(column_pairs.iloc[k][1]).lower())
                    is_sub_null = checkIfNullString(subject_value)
                    is_obj_null = checkIfNullString(object_value)
                    if is_sub_null != 0:
                        sub_entities = LABEL_DICT.get(subject_value, "None")
                        if sub_entities != "None":
                            if is_obj_null != 0:    
                                obj_entities = LABEL_DICT.get(object_value, "None")
                                if obj_entities != "None":
                                    #As both are not null, search for relation semantics
                                    for sub_entity in sub_entities:
                                        for obj_entity in obj_entities:
                                            #preparing key to search in the fact file
                                            entity_forward = sub_entity + "__" + obj_entity
                                            entity_backward = obj_entity + "__" + sub_entity
                                            relation_forward = FACT_DICT.get(entity_forward, "None")
                                            relation_backward = FACT_DICT.get(entity_backward, "None")
                                            if relation_forward != "None":
                                                total_kb_forward_hits += 1
                                                #keep track of the entity finding relation. We will use this to speed up the column semantics search
                                                key = str(i)+"_"+subject_value
                                                if key not in entities_finding_relation:
                                                    entities_finding_relation[key] = {sub_entity}
                                                else:
                                                    entities_finding_relation[key].add(sub_entity)
                                                key  = str(j) + "_" + object_value
                                                if key not in entities_finding_relation:
                                                    entities_finding_relation[key] = {obj_entity}
                                                else:
                                                    entities_finding_relation[key].add(obj_entity)
                                                for s in relation_forward:
                                                    if s in semantic_dict_forward:
                                                        semantic_dict_forward[s] += 1 #relation semantics in forward direction
                                                    else:
                                                        semantic_dict_forward[s] = 1
                                            if relation_backward != "None":
                                                total_kb_backward_hits += 1
                                                #keep track of the entity finding relation. We will use this for column semantics search
                                                key = str(i)+"_"+subject_value
                                                if key not in entities_finding_relation:
                                                    entities_finding_relation[key] = {sub_entity}
                                                else:
                                                    entities_finding_relation[key].add(sub_entity)
                                                
                                                key  = str(j)+"_"+object_value
                                                if key not in entities_finding_relation:
                                                    entities_finding_relation[key] = {obj_entity}
                                                else:
                                                    entities_finding_relation[key].add(obj_entity)
                                                
                                                for s in relation_backward:
                                                    if s in semantic_dict_backward:
                                                        semantic_dict_backward[s] += 1 #relation semantics in reverse direction
                                                    else:
                                                        semantic_dict_backward[s] = 1
                if len(semantic_dict_forward) > 0:
                    relation_dependencies.append(str(i)+"-"+str(j))
                    relation_dictionary[str(i)+"-"+str(j)] = [(max(semantic_dict_forward, key=semantic_dict_forward.get), max(semantic_dict_forward.values())/ total_kb_forward_hits)]
                if len(semantic_dict_backward) >0:
                    relation_dependencies.append(str(j)+"-"+str(i))
                    relation_dictionary[str(j)+"-"+str(i)] = [(max(semantic_dict_backward, key=semantic_dict_backward.get), max(semantic_dict_backward.values())/ total_kb_backward_hits)]
    return entities_finding_relation, relation_dependencies, relation_dictionary

#yago column semantics for query table
def computeColumnSemantics(input_table, subject_index, LABEL_DICT, TYPE_DICT, CLASS_DICT, RELATION_DICT):
    col_id = 0
    not_found_in_yago = []
    column_dictionary = {}
    subject_semantics = ""
    for (columnName, columnData) in input_table.iteritems():
        if getColumnType(input_table[columnName].tolist()) == 1: #check column Type
            input_table[columnName] = input_table[columnName].map(str)                
            #get unique values in the column and preprocess them.
            value_list = preprocessListValues(input_table[columnName].unique())
            #search values in KB 
            all_found_types = {}
            total_kb_hits = 0
            if str(subject_index) == str(col_id):
              label = "sc"
            else:
              label = "c"
            for value in value_list:
                current_entities = set()
                current_types = set()
                current_entities = RELATION_DICT.get(str(col_id) + "_"+ value, "None")
                #print(current_entities)
                if current_entities != "None":
                    total_kb_hits += 1
                    for entity in current_entities:
                        if entity in TYPE_DICT:
                            temp_type = TYPE_DICT[entity]
                            for entity_type in temp_type:
                                current_types.add(entity_type)
                    for each_type in current_types:
                        if each_type in all_found_types:
                            all_found_types[each_type] +=1
                        else:
                            all_found_types[each_type] = 1        
                else: 
                    current_entities = LABEL_DICT.get(value, "None")
                    if current_entities != "None": #found in KB
                        total_kb_hits += 1
                        for entity in current_entities:
                            if entity in TYPE_DICT:
                                temp_type = TYPE_DICT[entity]
                                for entity_type in temp_type:
                                    current_types.add(entity_type)
                        for each_type in current_types:
                            if each_type in all_found_types:
                                all_found_types[each_type] +=1
                            else:
                                all_found_types[each_type] = 1    
                
            #find the top-level type with highest count.
            all_top_types = [v for v in sorted(all_found_types.items(), key=lambda kv: (-kv[1], kv[0])) if v[0] in CLASS_DICT]
            if all_top_types:
                selected_top_type = all_top_types[0][0]
                top_type_count = all_top_types[0][1]
                if label == "sc":
                    subject_semantics = selected_top_type
                children_of_top_types = CLASS_DICT[selected_top_type]
                #add children of top types to the bag of word
                for each in all_found_types:
                    if each in children_of_top_types and (all_found_types[each] / top_type_count) >= 0:
                        if int(col_id) not in column_dictionary:
                            column_dictionary[int(col_id)] = [(each, all_found_types[each]/total_kb_hits)]
                        else:
                            column_dictionary[int(col_id)].append((each, all_found_types[each]/total_kb_hits))
        col_id += 1
        
    return column_dictionary, subject_semantics


