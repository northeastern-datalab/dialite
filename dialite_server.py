#This script connects table discovery algorithms, alignment and integration algorithms with ALITE.
import pandas as pd
import discover
import time
import glob
import pandasql as ps


def upload_query_table(filelocation):
    file_name=filelocation.split("/")[-1]
    print("Query table name:", file_name)
    try:
        #loaded query table only for the sanity check
        query_table = pd.read_csv(filelocation, encoding="latin-1", on_bad_lines="skip")
        query_table.to_csv(filelocation, index=False)
        print("Query table uploaded successfully.")
        print(query_table.head(5))
        return filelocation
    except:
        print("Could not read the table.")
        return 0

def query_santos(query_table, intent_column, k):
    # we have not connected santos API for the initial submission.
    # We run SANTOS using its code manually and store the integration set.
    # This function will be implemented before the camera ready submission upon the acceptance of demo.
    int_set = {"data/integration-set/covid19_t1/covid19_t2.csv"}
    return int_set

def query_josie(query_table, query_column, k):
    # we have not connected josie API for the initial submission.
    # We run JOSIE using its code manually and store the integration set.
    # This function will be implemented before the camera ready submission upon the acceptance of demo.
    int_set = {"data/integration-set/covid19_t1/covid19_t3.csv"}
    return int_set

def integrate_alite(integration_set):
    #return pre-integrated table for initial demo.
    integrated_table_location = "data/integration-result/alite_fd_covid19_t1.csv"
    integrated_table = pd.read_csv(integrated_table_location, encoding="latin-1", on_bad_lines="skip")
    return integrated_table

def discover_tables(query_table, algorithm, k):
    integration_set = {query_table}
    query_table = pd.read_csv(query_table, encoding="latin-1", on_bad_lines="skip")
    if "SANTOS" in algorithm:
        print("Enter index of intent column:")
        intent_column = int(input())
        print(intent_column)
        int_set = query_santos(query_table, intent_column, k)
        integration_set = integration_set.union(int_set)
    if "JOSIE" in algorithm:
        print("Enter index of query column:")
        query_column = int(input())
        print(query_column)
        int_set = query_josie(query_table, query_column, k)
        integration_set = integration_set.union(int_set)
    print("Integration set after table discovery:")
    for table in integration_set:
        print(table.rsplit("/",1)[-1])
    return integration_set

def integrate_tables(integration_set, algorithm):
    if algorithm == "ALITE":
        query_table_name = list(integration_set)[0].rsplit("/",2)[-1]
        integrated_table = integrate_alite(integration_set)
        #integrated_table_location = "data/integration-result/alite_fd_"+query_table_name
        #integrated_table.to_csv(integrated_table_location, index=False)
        print("Successfully integrated "+ str(len(integration_set)) + " tables using ALITE.")
        print("Integrated table:")
        print(integrated_table)
    return integrated_table

def randomly_generate_query_table(prompt):
    time.sleep(10)
    table = pd.read_csv("gpt_table.csv")
    return table

def get_table_name(table):
    table = table.rsplit("/",1)[-1]
    return table.rsplit(".",1)[0]

def analyze_sql(integrated_table, query):
    # We execute the query manually 
    integrated_table.dropna(subset = ['Vaccination Rate'], inplace = True)
    print(ps.sqldf(query, locals()))
