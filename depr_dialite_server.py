#This script connects table discovery algorithms, alignment and integration algorithms with ALITE.
import pandas as pd
import discover
import time

def upload_query_table(filelocation):
    file_name=filelocation.split("/")[-1]
    print("Query table name:", file_name)
    try:
        query_table = pd.read_csv(filelocation, encoding="latin-1", on_bad_lines="skip")
        query_table.head(5)
        print("Query table uploaded successfully.")
        return query_table
    except:
        print("Could not read the table.")
        return 0


def QuerySANTOS(query_table, intent_column, k):
    #connect to SANTOS and return top-k unionable tables.
    #for now we will use the dummy tables
    if k == 1:
        result = "Result"
    else:
        result = "Results"
    time.sleep(4)
    print("SANTOS top-"+str(k)+" "+ result +" added to the integration set.")

def QueryJOSIE(query_table, intent_column, k):
    #connect to JOSIE and return top-k unionable tables.
    #for now we will use the dummy tables
    if k == 1:
        result = "Result"
    else:
        result = "Results"
    time.sleep(4)
    print("JOSIE top-"+str(k)+" "+ result+" added to the integration set.")

def FindIntegrationIDs():
    #connect to ALIGN part of ALITE and assign dummy headers.
    time.sleep(4)
    print("Alignment task completed.")

def ApplyALITEIntegration():
    #connect to ALIGN part of ALITE and assign dummy headers.
    time.sleep(4)
    print("Integrated tables using ALITE.")

def ApplyOuterJoinIntegration():
    #connect to ALIGN part of ALITE and assign dummy headers.
    time.sleep(4)
    print("Integrated tables using Outer join.")

def randomly_generate_query_table(prompt):
    time.sleep(10)
    table = pd.read_csv("gpt_table.csv")
    return table