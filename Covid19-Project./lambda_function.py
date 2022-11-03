import json
import boto3
import csv
import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode
s3_client = boto3.client('s3')
def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    csv_file = event['Records'][0]['s3']['object']['key']
    csv_file_obj = s3_client.get_object(Bucket=bucket, Key=csv_file)
    lines = csv_file_obj['Body'].read().decode('utf-8').split('\n')
    results = []
    for row in csv.DictReader(lines):
        results.append(row.values())
    print(results)
    connection = mysql.connector.connect(host='transaction-instance.c9zdxn3lynuz.us-east-1.rds.amazonaws.com',database='covid19database',user='admin',
    password='Rupesh162451')
    cursor = connection.cursor()
    cursor.execute("create table covid19 (State varchar(250),District varchar(250),Confirmed int,Active int,Recovered int,Deceased int)")
    connection.commit()
    mysql_empsql_insert_query = "INSERT INTO covid19 (State,District,Confirmed,Active,Recovered,Deceased)   VALUES (%s, %s, %s, %s, %s, %s)"
    cursor = connection.cursor()
    cursor.executemany(mysql_empsql_insert_query,results)
    connection.commit()
    print(cursor.rowcount, "Record inserted successfully into covid19 table")
    # TODO implement
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
