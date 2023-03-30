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
    if "stadiums" in list(integration_set)[0]: #.contains("stadiums"):
        integrated_table_location = "data/integration-result/alite_fd_stadiums_0.csv"
    else:
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
        #query_table_name = list(integration_set)[0].rsplit("/",2)[-1]
        integrated_table = integrate_alite(integration_set)
        #integrated_table_location = "data/integration-result/alite_fd_"+query_table_name
        #integrated_table.to_csv(integrated_table_location, index=False)
        print("Successfully integrated "+ str(len(integration_set)) + " tables using ALITE.")
        print("Integrated table:")
        print(integrated_table)
    
    if algorithm == "outer_join":
        #for initial submission, we return a pre-integrated table without calling the user defined function.
        integrated_table_location = "data/integration-result/outerjoin_stadiums_0.csv"
        integrated_table = pd.read_csv(integrated_table_location, encoding="latin-1", on_bad_lines="skip")
        print("Successfully integrated "+ str(len(integration_set)) + " tables using outer join algorithm.")
        print("Integrated table:")
        print(integrated_table)
    return integrated_table


def get_table_name(table):
    table = table.rsplit("/",1)[-1]
    return table.rsplit(".",1)[0]

def analyze_sql(integrated_table, query):
    # We execute the query manually 
    integrated_table.dropna(subset = ['Vaccination Rate'], inplace = True)
    print(ps.sqldf(query, locals()))


def randomly_generate_query_table(prompt):
    # We have not show GPT connection to keep the query table deterministic for the demo.
    time.sleep(5)
    table = pd.read_csv("data/query/gpt_table_1.csv")
    return table

def new_joinability_discovery_algorithm(df1, df2):
    join_df = pd.merge(df1, df2, how ='inner')
    return len(join_df)/max(len(df1), len(df2))

def new_outer_join_integration_algorithm(integration_set):
    table1_loc = integration_set.pop()
    table1 = pd.read_csv(table1_loc)
    for table2_loc in integration_set:
            table2 = pd.read_csv(table2_loc)
            table1 = table1.merge(table2, how = "outer")
    return table1


def analyze_er(integrated_table):
    # We use py entitymatching package to run the entity resolution algorithm.
    # Currently, we have not integrated the API. So we show the results of applying er using py entitymatching
    # manually. These number are generated from entitymatching package separetely and 
    # are also reported in the full ALITE paper.
    # The related notebook is available in this github repo with name: analyze_entity_resolution.ipynb
    if integrated_table.shape[0] > 115 :
        print("Precision: 0.795")
        print("Recall: 0.838")
        print("F-score: 0.816")
    else:
        print("Precision: 0.339")
        print("Recall: 0.397")
        print("F-score: 0.366")