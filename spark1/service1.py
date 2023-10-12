from flask import Flask
from pyspark.sql import SparkSession, Row
from sqlalchemy import create_engine
import pymssql

app = Flask(__name__)

# spark = SparkSession.builder.appName("spark1").config("spark.jars", "/home/decoders/jdbc_driver/jar_files/mysql-connector-java-8.0.15.jar").getOrCreate()
spark = SparkSession \
    .builder \
    .appName("mongodbtest1") \
    .master('local')\
    .config("spark.mongodb.input.uri", "mongodb://admin:password@127.0.0.1:27017/admin.temproles") \
    .config("spark.mongodb.output.uri", "mongodb://admin:password@127.0.0.1:27017/admin.temproles") \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
    .getOrCreate()
@app.route('/readfile', methods=['GET'])
def csv_read():
    current_db = spark.catalog.currentDatabase()
    data = spark.read.csv("kognitic_trials_adc/kognitic_trials_oncology_adc_trial_drug.csv", header=True)  # headers=True Given the Tables Headers
    data.show()
    studentdf = spark.createDataFrame([
        Row(id=1, name='vijay', marks=67),
        Row(id=2, name='Ajay', marks=88),
        Row(id=3, name='jay', marks=79),
        Row(id=4, name='binny', marks=99),
        Row(id=5, name='omar', marks=99),
        Row(id=6, name='divya', marks=98),
    ])
    studentdf.select("id", "name", "marks").write \
        .format('com.mongodb.spark.sql.DefaultSource') \
        .option("uri", "mongodb://admin:password@127.0.0.1:27017/admin.temproles") \
        .save()
    return {"data": "Data Added Succussfully"}


def getdata(df):
    data = spark.read.csv("kognitic_trials_adc/kognitic_trials_oncology_adc_trial_drug.csv",header=True)  # headers=True Given the Tables Headers

    df.write.format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/clinical_trial_db_relational?useSSL=false") \
        .option("driver", "com.mysql.jdbc.Driver") \
        .option("dbtable", "newTable") \
        .option("user", "root") \
        .option("password", "sam_8778") \
        .mode("overwrite") \
        .save()
    return {"data": "Data Added Succussfully"}


@app.route('/my-route')
def my_route():
    return 'This is my custom route from my_service.py!'
