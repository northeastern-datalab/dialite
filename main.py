from flask import Flask, render_template, request, jsonify
import pandas as pd
import csv
import os
import openai

app = Flask(__name__)

def QueryGPT3(prompt):
    openai.api_key = "OPENAI_API_KEY"
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

@app.route("/")
def index():    
    return render_template('index.html')

@app.route("/upload_query", methods=["POST"])
def upload_table():
    try:
        table = request.files['query_table']
        #table_name = request.form['upload_query_name']
        if table:
            table_name = table.filename
            if table_name.rsplit(".")[-1] != "csv":
                return jsonify({'success': False, 'message': 'Upload failed! Only csv files are supported for now!'})
            table_path = "data"+os.sep+"query"+os.sep+table_name
            if os.path.isfile(table_path) == True:
                return jsonify({'success': False, 'message': 'File already exists. Please upload a different query table!'})
            #print("Filename:", file)
            table.save(table_path)
            with open(table_path, 'r') as f:
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
        table_path = "data"+os.sep+"query"+os.sep+generated_query_name+".csv"
        if os.path.isfile(table_path) == True:
                return jsonify({'success': False, 'message': 'File already exists. Please give a different name!'})
        table_text = QueryGPT3(text_prompt)
        table = ConvertTextToTable(table_text)
        table.to_csv(table_path, index=False)
        table_sample = table.head(5)
        message =  "Query Table named "+ generated_query_name +".csv with "
        message += str(table.shape[0])+" rows and "+ str(table.shape[1]) +" columns "
        message += "Generated successfully! See Sample table."           
        return jsonify({'success': True, 'message': message, 'table': table_sample.to_html()})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e), 'table':"Table not generated"})
if __name__ == "__main__":
    app.run(debug=True)