#This script connects table discovery algorithms, alignment and integration algorithms with ALITE.
import pandas as pd
import discover
import time

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