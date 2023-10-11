from flask import Flask
from pyspark.sql import SparkSession, Row
from sqlalchemy import create_engine
import pymssql

app = Flask(__name__)

spark = SparkSession.builder.appName("spark1").config("spark.jars", "/home/decoders/jdbc_driver/mssql/sqljdbc_12.4.1.0_enu/sqljdbc_12.4/enu/jars/mssql-jdbc-12.4.1.jre8.jar").getOrCreate()
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
    # data.select(['Year', 'Variable_name']).show() #for filtering
    return getdata(data)


def getdata(df):
    data = spark.read.csv("kognitic_trials_adc/kognitic_trials_oncology_adc_trial_drug.csv",header=True)  # headers=True Given the Tables Headers
    # df.write.format("jdbc") \
    #     .option("url", "jdbc:mysql://localhost:3306/clinical_trial_db_relational?useSSL=false") \
    #     .option("driver", "com.mysql.jdbc.Driver") \
    #     .option("dbtable", "clinical_data") \
    #     .option("user", "root") \
    #     .option("password", "sam_8778") \
    #     .mode("overwrite") \
    #     .save()
    df.write.format("jdbc") \
        .option("url", "jdbc:sqlserver://localhost:1433;databaseName=clinical_data_base;encrypt=true;trustServerCertificate=true") \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .option("dbtable", "newTable") \
        .option("user", "sa") \
        .option("password", "Welcome123") \
        .mode("overwrite") \
        .save()
    return {"data": "Data Added Succussfully"}


@app.route('/my-route')
def my_route():
    return 'This is my custom route from my_service.py!'
