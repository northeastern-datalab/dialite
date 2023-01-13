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

def discover_tables(query_table, algorithm, k):
    integration_set = set()
    if "SANTOS" in algorithm:
        print("Enter the value of k for SANTOS:")
        k = int(input())
        print("Enter index of intent column:")
        intent_column = int(input())
        integration_set = QuerySANTOS(query_table, intent_column, k)

    if "JOSIE" in selected_algorithms:
        print("Enter the value of k for JOSIE:")
        k = int(input())
        print("Enter index of query column:")
        query_column = int(input())
        dialite.QueryJOSIE(query_table, query_column, k)

def randomly_generate_query_table(prompt):
    time.sleep(10)
    table = pd.read_csv("gpt_table.csv")
    return table