# -*- coding: utf-8 -*-

import re 
import pickle
import os.path
import sys
import csv
import json
import string
import bz2
import _pickle as cPickle
import nltk
from nltk.corpus import stopwords
from nltk import word_tokenize, pos_tag
import pandas as pd
import numpy as np

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
    for entity in puncCleanedList:
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

