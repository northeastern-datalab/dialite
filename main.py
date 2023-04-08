import santos.codes.santos as santos
import alite.alite_fd as alite
from flask import Flask, render_template, request, jsonify, send_file, abort
import pandas as pd
import csv
import os, socket
import openai
import glob
import time
import pickle, bz2
import _pickle as cPickle
import json
import sys
import stat
import shutil
from load_dictionaries import *
from waitress import serve
    
app = Flask(__name__)
app.config['query_table_folder'] = os.path.join('data', 'query')
app.config['integration_set_folder'] = os.path.join('data', 'integration-set')

def QueryGPT3(prompt, api_key):
    openai.api_key = api_key
    response = openai.Completion.create(
    model="text-davinci-003",
    prompt=prompt,
    temperature=0.7,
    max_tokens=256,
    top_p=1,
    frequency_penalty=0,
    presence_penalty=0
    )
    return response.choices[0]['text']

def ConvertTextToTable(text):
    # Splitting the string by newline character
    rows = text.split('\n')
    # Extracting column names from the first row
    columns = [col.strip() for col in rows[1].split('|') if col.strip()]
    # Extracting the data from the rest of the rows
    data_rows = []
    for row in rows[2:]:
        data_rows.append([col.strip() for col in row.split('|') if col.strip()])
    columns = data_rows.pop(0)
    separator = data_rows.pop(0)
    # Creating pandas DataFrame
    df = pd.DataFrame(data_rows, columns=columns)
    # print the resulting DataFrame
    return df

def find_first_string_col(df):
    for i, col in enumerate(df.columns):
        if df[col].apply(lambda x: isinstance(x, str)).sum() / len(df) >= 0.5:
            return i
    return 0

def find_string_cols(df):
    string_cols = []
    for i, col in enumerate(df.columns):
        if df[col].apply(lambda x: isinstance(x, str)).sum() / len(df) >= 0.75:
            string_cols.append({'value': i, 'text': col})
    return string_cols

def integrate_alite(integration_set):
    statistics = pd.DataFrame(
            columns = ["cluster", "n", "s", "f", "labeled_nulls",
                       "produced_nulls", "complement_time",
                       "complement_partitions", "largest_partition_size", "partitioning_used",
                       "subsume_time",
                       "subsumed_tuples", "total_time", "f_s_ratio"])
    # cluster_name = integration_set[0].rsplit(os.sep,1)[-1]
    # cluster_name = cluster_name.rsplit(".", 1)[0]
    # print("Cluster name:", cluster_name)
    result_FD, stats_df, debug_dict = alite.FDAlgorithm(integration_set)
    return result_FD
    #save result to hard drive
    #result_FD.to_csv(output_path+ cluster_name+".csv",index = False)
    #statistics = pd.concat([statistics, stats_df])
    #statistics.to_csv(stat_path, index = False)

def new_outer_join_integration_algorithm(integration_set):
    table1_loc = integration_set.pop()
    table1 = pd.read_csv(table1_loc)
    for table2_loc in integration_set:
            table2 = pd.read_csv(table2_loc)
            table1 = table1.merge(table2, how = "outer")
    return table1

def query_santos(query_table, intent_column, k):
    #query_table = pd.read_csv(query_table, encoding='latin1', on_bad_lines="skip")
    entity_finding_relations, relation_dependencies, relation_dictionary = santos.computeRelationSemantics(query_table, intent_column, label_dict, fact_dict)
    column_dictionary, subject_semantics = santos.computeColumnSemantics(query_table, intent_column, label_dict, type_dict, class_dict, entity_finding_relations)
    synthetic_triples_dictionary, synth_subject_semantics = santos.computeSynthRelation(query_table, intent_column, synth_relation_kb)
    synthetic_column_dictionary = santos.computeSynthColumnSemantics(query_table, synth_type_kb)
    current_relations = set()
    for item in relation_dependencies:    
        current_relations.add(item)
    query_table_triples = {}
    synth_query_table_triples = {}
    if len(column_dictionary) > 0:
            for i in range(0, max(column_dictionary.keys())):
                subject_type = column_dictionary.get(i, "None")
                if subject_type != "None":
                    for j in range(i+1, max(column_dictionary.keys()) + 1):
                        object_type = column_dictionary.get(j, "None")
                        relation_tuple_forward = "None"
                        relation_tuple_backward = "None"
                        if object_type != "None":
                            for subject_item in subject_type:
                                for object_item in object_type:
                                    subject_name = subject_item[0]
                                    subject_score = subject_item[1]        
                                    object_name = object_item[0]
                                    object_score = object_item[1]
                                    if str(i) + "-" + str(j) in current_relations:
                                        relation_tuple_forward = relation_dictionary.get(str(i) + "-" + str(j), "None")
                                    if str(j) + "-" + str(i) in current_relations:
                                        relation_tuple_backward = relation_dictionary.get(str(j) + "-" + str(i), "None")
                                    column_pairs = str(i) + "-" + str(j)
                                    if relation_tuple_forward != "None":
                                        relation_name = relation_tuple_forward[0][0]
                                        relation_score = relation_tuple_forward[0][1]
                                        triple_dict_key = subject_name + "-" + relation_name + "-" + object_name
                                        triple_score = subject_score * relation_score * object_score
                                        if triple_dict_key in query_table_triples:
                                            if triple_score > query_table_triples[triple_dict_key][0]:
                                                query_table_triples[triple_dict_key] = (triple_score, column_pairs)
                                        else:
                                            query_table_triples[triple_dict_key] = (triple_score, column_pairs)
                                    if relation_tuple_backward != "None":
                                        relation_name = relation_tuple_backward[0][0]
                                        relation_score = relation_tuple_backward[0][1]
                                        triple_dict_key = object_name + "-" + relation_name + "-" + subject_name
                                        triple_score = subject_score * relation_score * object_score
                                        if triple_dict_key in query_table_triples:
                                            if triple_score > query_table_triples[triple_dict_key][0]:
                                                query_table_triples[triple_dict_key] = (triple_score, column_pairs)
                                        else:
                                            query_table_triples[triple_dict_key] =  (triple_score, column_pairs)
        #check if synthetic KB has found triples
    for key in synthetic_triples_dictionary:
        if len(synthetic_triples_dictionary[key]) > 0:
            synthetic_triples = synthetic_triples_dictionary[key]
            for synthetic_triple in synthetic_triples:
                synthetic_triple_name = synthetic_triple[0]
                synthetic_triple_score = synthetic_triple[1]
                if synthetic_triple_name in synth_query_table_triples:
                    if synthetic_triple_score > synth_query_table_triples[synthetic_triple_name][0]:
                        synth_query_table_triples[synthetic_triple_name] = (synthetic_triple_score, key)
                else:
                    synth_query_table_triples[synthetic_triple_name] = (synthetic_triple_score, key)
    query_table_triples = set(query_table_triples.items())
    synth_query_table_triples = set(synth_query_table_triples.items())
    
    table_count_final = {}
    tables_containing_intent_column = {}
    #to make sure that the subject column is present
    if subject_semantics != "" and subject_semantics+"-c" in yago_inverted_index:
        intent_containing_tables = yago_inverted_index[subject_semantics+"-c"]
        for table_tuple in intent_containing_tables:
            tables_containing_intent_column[table_tuple[0]] = 1
    
    
    already_used_column = {}   
    for item in query_table_triples:
        matching_tables = main_index_triples.get(item[0], "None") #checks yago inv4erted index
        if matching_tables != "None":
            triple = item[0]
            query_score = item[1][0]
            col_pairs = item[1][1]
            for data_lake_table in matching_tables:
                dlt_name = data_lake_table[0]
                if triple in synth_subject_semantics:
                    tables_containing_intent_column[dlt_name] = 1
                dlt_score = data_lake_table[1]
                total_score = query_score * dlt_score
                if (dlt_name, col_pairs) not in already_used_column:
                    if dlt_name not in table_count_final:
                        table_count_final[dlt_name] = total_score
                    else:
                        table_count_final[dlt_name] += total_score
                    already_used_column[(dlt_name, col_pairs)] = total_score
                else:
                    if already_used_column[(dlt_name, col_pairs)] > total_score:
                        continue
                    else: #use better matching score
                        if dlt_name not in table_count_final:
                            table_count_final[dlt_name] = total_score
                        else:
                            table_count_final[dlt_name] -= already_used_column[(dlt_name, col_pairs)]
                            table_count_final[dlt_name] += total_score
                        already_used_column[(dlt_name, col_pairs)] = total_score
    synth_col_scores = {}
    for item in synth_query_table_triples:
        matching_tables = synth_relation_inverted_index.get(item[0], "None") #checks synth KB index
        if matching_tables != "None":
            triple = item[0]
            query_rel_score = item[1][0]
            col_pairs = item[1][1]
            for data_lake_table in matching_tables:
                dlt_name = data_lake_table[0]
                if triple in synth_subject_semantics:
                    tables_containing_intent_column[dlt_name] = 1
                dlt_rel_score = data_lake_table[1][0]
                dlt_col1 = data_lake_table[1][1]
                dlt_col2 = data_lake_table[1][2]
                query_col1 = col_pairs.split("-")[0]
                query_col2 = col_pairs.split("-")[1]
                dlt_col1_contents = {}
                dlt_col2_contents = {}
                query_col1_contents = {}
                query_col2_contents = {}
                if (dlt_name, dlt_col1) in synth_type_inverted_index:    
                    dlt_col1_contents = synth_type_inverted_index[(dlt_name, dlt_col1)]
                if (dlt_name, dlt_col2) in synth_type_inverted_index:    
                    dlt_col2_contents = synth_type_inverted_index[(dlt_name, dlt_col2)]
                if query_col1 in synthetic_column_dictionary:
                    query_col1_contents = synthetic_column_dictionary[query_col1]
                if query_col2 in synthetic_column_dictionary:
                    query_col2_contents = synthetic_column_dictionary[query_col2]
                #find intersection between dlt1 and query1
                
                max_score = [0,0,0,0]
                if col_pairs +"-"+dlt_col1 + "-"+ dlt_col2 in synth_col_scores:
                    total_score = synth_col_scores[col_pairs +"-"+dlt_col1 + "-"+ dlt_col2] * dlt_rel_score * query_rel_score
                else:
                    match_keys_11 = dlt_col1_contents.keys() & query_col1_contents.keys()
                    if len(match_keys_11) > 0:
                        for each_key in match_keys_11:
                            current_score = dlt_col1_contents[each_key] * query_col1_contents[each_key]
                            if current_score > max_score[0]:
                                max_score[0] = current_score
                    
                    match_keys_12 = dlt_col1_contents.keys() & query_col2_contents.keys()
                    if len(match_keys_12) > 0:
                        for each_key in match_keys_12:
                            current_score = dlt_col1_contents[each_key] * query_col2_contents[each_key]
                            if current_score > max_score[1]:
                                max_score[1] = current_score
                    
                    match_keys_21 = dlt_col2_contents.keys() & query_col1_contents.keys()
                    
                    if len(match_keys_21) > 0:
                        for each_key in match_keys_21:
                            current_score = dlt_col2_contents[each_key] * query_col1_contents[each_key]
                            if current_score > max_score[2]:
                                max_score[2] = current_score
                    
                    match_keys_22 = dlt_col2_contents.keys() & query_col2_contents.keys()
                    
                    
                    if len(match_keys_22) > 0:
                        for each_key in match_keys_22:
                            current_score = dlt_col2_contents[each_key] * query_col2_contents[each_key]
                            if current_score > max_score[3]:
                                max_score[3] = current_score
                    
                    
                    max_score = sorted(max_score, reverse = True)
                    synth_col_scores[col_pairs +"-"+dlt_col1 + "-"+ dlt_col2] = max_score[0] * max_score[1]    
                    total_score = query_rel_score * dlt_rel_score * max_score[0] * max_score[1]
                if (dlt_name, col_pairs) not in already_used_column:
                    if dlt_name not in table_count_final:
                        table_count_final[dlt_name] = total_score
                    else:
                        table_count_final[dlt_name] += total_score
                    already_used_column[(dlt_name, col_pairs)] = total_score
                else:
                    if already_used_column[(dlt_name, col_pairs)] > total_score:
                        continue
                    else: #use better matching RSCS' score
                        if dlt_name not in table_count_final:
                            table_count_final[dlt_name] = total_score
                        else:
                            table_count_final[dlt_name] -= already_used_column[(dlt_name, col_pairs)]
                            table_count_final[dlt_name] += total_score
                        already_used_column[(dlt_name, col_pairs)] = total_score   
        
    #to make sure that the match was because of intent column
    tables_to_throw = set()
    if len(tables_containing_intent_column) > 0:
        for shortlisted_table in table_count_final:
            if shortlisted_table not in tables_containing_intent_column:
                tables_to_throw.add(shortlisted_table)
    if len(tables_to_throw) > 0 and len(tables_to_throw) < len(table_count_final):
        for item in tables_to_throw:    
            del table_count_final[item]
        
    sortedTableList = sorted(table_count_final.items(), key=lambda x: x[1], reverse=True)
    return sortedTableList[:k]


@app.route("/")
def index():   
    query_tables = glob.glob(r"data"+os.sep+"query"+os.sep+"*")
    query_tables = [query.rsplit(os.sep,1)[-1] for query in query_tables]
    integration_sets = glob.glob(r"data"+os.sep+"integration-set"+os.sep+"*")
    integration_sets = [integration_set.rsplit(os.sep,1)[-1] for integration_set in integration_sets]
    return render_template('index.html', query_tables = query_tables, integration_sets = integration_sets)

@app.route('/update_available_query')
def update_available_query():
    query_tables = glob.glob(r"data"+os.sep+"query"+os.sep+"*")
    query_tables = [query.rsplit(os.sep,1)[-1] for query in query_tables]
    # Return the updated my_list variable as a JSON response
    return jsonify(query_tables=query_tables)

@app.route('/update_integration_sets')
def update_integration_sets():
    integration_sets = glob.glob(r"data"+os.sep+"integration-set"+os.sep+"*")
    integration_sets = [integration_set.rsplit(os.sep,1)[-1] for integration_set in integration_sets]
    # Return the updated my_list variable as a JSON response
    return jsonify(integration_sets=integration_sets)


@app.route("/upload_query", methods=["POST"])
def upload_table():
    try:
        table = request.files['query_table']
        #table_name = request.form['upload_query_name']
        if table:
            table_name = table.filename
            if table_name.rsplit(".")[-1] != "csv":
                return jsonify({'success': False, 'message': 'Upload failed! Only csv files are supported for now!'})
            query_table_path = app.config['query_table_folder']+os.sep+table_name
            if os.path.isfile(query_table_path) == True:
                return jsonify({'success': False, 'message': 'File already exists. Please upload a different query table!'})
            #print("Filename:", file)
            table.save(query_table_path)
            with open(query_table_path, 'r') as f:
                reader = csv.reader(f)
                for row in reader:
                    # process each row of the CSV file as desired
                    print(row)
                    break
            return jsonify({'success': True, 'message': 'File uploaded successfully!'})
        else:
            return jsonify({'success': False, 'message': 'No file to upload!'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})    

@app.route("/generate_query", methods=["POST"])
def generate_table():
    try:
        #"generate a table about covid with 5 columns and 10 rows"
        text_prompt = request.form['query_prompt'] 
        generated_query_name = request.form['generated_query_name']
        api_key = request.form['api_key']
        query_table_path = app.config['query_table_folder']+os.sep+generated_query_name+".csv"
        if os.path.isfile(query_table_path) == True:
                return jsonify({'success': False, 'message': 'File already exists. Please give a different name!'})
        table_text = QueryGPT3(text_prompt, api_key)
        table = ConvertTextToTable(table_text)
        table.to_csv(query_table_path, index=False)
        message =  "Query Table named "+ generated_query_name +".csv with "
        message += str(table.shape[0])+" rows and "+ str(table.shape[1]) +" columns "
        message += "Generated successfully!."           
        return jsonify({'success': True, 'message': message, 'table': 
                        table.to_html(index=False, render_links=True, \
                                    escape= False, col_space=100, justify="center", \
                                    table_id="gpt_generated_table", \
                                        classes='table table-striped table-hover table-bordered')
                        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e), 'table':"Table not generated"})

@app.route("/discover_tables", methods=["POST"])
def discover_tables():
    query_table_name = request.form['discover_query']
    query_table_path = app.config['query_table_folder'] + os.sep + query_table_name
    integration_set_path = app.config['integration_set_folder'] + os.sep + query_table_name.rsplit(".",1)[0]
    query_table = pd.read_csv(query_table_path, encoding="latin-1", on_bad_lines="skip")
    if not os.path.exists(integration_set_path):
        os.makedirs(integration_set_path, mode=0o777)
        query_table.to_csv(integration_set_path+os.sep+query_table_name,index=False)
        os.chmod(integration_set_path+os.sep+query_table_name, 0o777)
    else:
        files = glob.glob(integration_set_path+os.sep+"*")
        for f in files:
            os.remove(f)
        query_table.to_csv(integration_set_path+os.sep+query_table_name,index=False)
        os.chmod(integration_set_path+os.sep+query_table_name, 0o777)
        
    #else clean it before adding new tables after discovery.
    algorithm = request.form.getlist('discovery_method')
    intent_column = int(request.form['intent_column'])
    k = int(request.form['k'])
    integration_set = set()
    if "SANTOS" in algorithm:
        int_set = query_santos(query_table, intent_column, k)
        for item in int_set:
            integration_set.add(item[0])
            try:
                shutil.copy(r"data"+os.sep+"dialite_datalake"+os.sep+item[0],integration_set_path+os.sep+item[0])
            except Exception as e:
                print(e)
        # integration_set = integration_set.union(int_set)
    if "JOSIE" in algorithm:
        print("skip for now")
        #print("Enter index of query column:")
        #query_column = int(input())
        #print(query_column)
        #int_set = query_josie(query_table, query_column, k)
        #integration_set = integration_set.union(int_set)
    #print(integration_set)
    integration_list = list(integration_set)
    return jsonify({"success": True, "message":"Dataset Search is finished! You can find them in integrate tab.","integration_list":integration_list})

@app.route("/integrate_tables", methods=["POST"])
def integrate_tables():
    integration_set_name = request.form['select_integration_sets']
    integration_set_path = app.config['integration_set_folder'] + os.sep + integration_set_name
    exclude_list = request.form.getlist('exclude_list')
    # exclude_set = set()
    # for exclude in exclude_list:
    #     exclude_set.add(integration_set_path + os.sep + exclude)
    exclude_set = set([(integration_set_path + os.sep + str(exclude)) for exclude in exclude_list])
    # print(exclude_set)
    # return jsonify({'success': False, 'message': 'debugging'})
    try:
        if not os.path.exists(integration_set_path):
            return jsonify({'success': False, 'message': 'Integration set not found! Please run discovery again!'})
        else:
            integration_set = glob.glob(integration_set_path + os.sep + "*.csv")
            integration_set = list(set(integration_set) - exclude_set)
            if len(integration_set) <= 1: 
                query_table = pd.read_csv(integration_set[0], encoding="latin-1", on_bad_lines="skip")
                return jsonify({'success': True, 
                                'message': 'There is only one table in the integration set!',
                                'table': query_table.to_html(index=False, render_links=True, \
                                    escape= False, col_space=100, justify="center", \
                                    table_id="integrated_table", \
                                        classes='table table-striped table-hover table-bordered')
                                })  
            else:
                algorithm = request.form.get('integration_method')
                print("Integration set: ", integration_set)
                if algorithm == "OUTER":
                    integrated_table = new_outer_join_integration_algorithm(integration_set)
                    return jsonify({'success': True, 
                                'message': 'Integration successfull!.',
                                'table': integrated_table.to_html(index=False, render_links=True, \
                                    escape= False, col_space=100, justify="center", \
                                    table_id="integrated_table", \
                                        classes='table table-striped table-hover table-bordered')
                                }) 
                # elif algorithm == "NEW ALG": # integrate new algorithm here.
                else: #default is ALITE
                    integrated_table = integrate_alite(integration_set)
                    #print(integrated_table)
                    return jsonify({'success': True, 
                                'message': 'Integration successfull!',
                                'table': integrated_table.to_html(index=False, render_links=True, \
                                    escape= False, col_space=100, justify="center", \
                                    table_id="integrated_table", \
                                        classes='table table-striped table-hover table-bordered')
                                }) 
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route("/show_query_table", methods=["POST"])
def show_query_table():
    query_table_name = request.form['query_table_name']
    query_table_path = app.config['query_table_folder'] + os.sep + query_table_name
    query_table = pd.read_csv(query_table_path, encoding="latin-1", on_bad_lines="skip")
    string_columns = find_string_cols(query_table)
    return jsonify({'options':string_columns, \
                    'table': query_table.to_html(index=False, render_links=True, \
                                escape= False, col_space=100, justify="center", \
                                table_id="current_query_table", \
                                    classes='table table-striped table-hover table-bordered')})
    
@app.route("/show_integration_set", methods=["POST"])
def show_integration_set():
    integration_set_name = request.form['integration_set_name']
    integration_set_link = app.config['integration_set_folder'] + os.sep + integration_set_name
    table_list = glob.glob(integration_set_link+os.sep+"*")
    table_list = [table.rsplit(os.sep,1)[-1] for table in table_list]
    table_list = list(set(table_list) - {integration_set_name+".csv"}) #exclude query table so that it can't be checked out.
    return jsonify({'table_list':table_list, 'integration_set_link': integration_set_link})

@app.route('/download/')
def download_file():
    filename = request.args.get('file')
    if os.path.isfile(os.path.join(filename)):
        return send_file(filename, as_attachment=True)
    else:
        abort(404, "File not found") # Return a 404 error with an error message

if __name__ == "__main__":
    print("Press 1 to host the website live (this needs more information). Press any other keys to host the website locally.")
    choice = int(input())
    if choice == 1:
        hostname = socket.gethostname()
        ip = socket.gethostbyname(hostname)
        print("Enter Port number:")
        port = int(input())
        serve(app, host=hostname, port=port)
    else:
        app.run(debug=True)
