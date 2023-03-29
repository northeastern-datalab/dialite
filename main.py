from flask import Flask, render_template, request, jsonify
import pandas as pd
import csv
import os

app = Flask(__name__)


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
    
if __name__ == "__main__":
    app.run(debug=True)