from flask import Flask
from pyspark.sql import SparkSession, Row
from sqlalchemy import create_engine
import pymssql

app = Flask(__name__)

spark = SparkSession.builder.appName("spark1").config("spark.jars", "/home/decoders/jdbc_driver/postql/postgresql-42.6.0.jar").getOrCreate()
DATABASE_URI = 'mssql+pymssql://sa:Welcome123@localhost:1433/master'

@app.route('/readfile', methods=['GET'])
def csv_read():
    current_db = spark.catalog.currentDatabase()
    print(current_db)
    data = spark.read.csv("kognitic_trials_adc/kognitic_trials_oncology_adc_trial_drug.csv", header=True)  # headers=True Given the Tables Headers
    data.show()
    getdata(data)
    spark.read.option('header', 'true').csv('fin Data.csv').show()
    columns = data.columns
    header_rdd = spark.sparkContext.parallelize([Row(header=columns)]) # for get header only
    print("headers List", header_rdd)
    return getdata(data)


def getdata(df):
    data = spark.read.csv("kognitic_trials_adc/kognitic_trials_oncology_adc_trial_drug.csv",header=True)  # headers=True Given the Tables Headers
    df.write.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "clinical_table") \
        .option("user", "postgres") \
        .option("password", "mypass") \
        .mode("overwrite") \
        .save()
    return {"data": "Data Added Succussfully"}


@app.route('/my-route')
def my_route():
    return 'This is my custom route from my_service.py!'
