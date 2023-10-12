from flask import Flask
from pyspark.sql import SparkSession, Row


app = Flask(__name__)

# spark = SparkSession.builder.appName("spark1").config("spark.jars", "/home/decoders/jdbc_driver/jar_files/mysql-connector-java-8.0.15.jar").getOrCreate()
spark = SparkSession \
    .builder \
    .appName("mongodbtest1") \
    .master('local')\
    .config("spark.mongodb.input.uri", "mongodb://admin:password@127.0.0.1:27017/admin.tempusers") \
    .config("spark.mongodb.output.uri", "mongodb://admin:password@127.0.0.1:27017/admin.tempusers") \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
    .getOrCreate()


@app.route('/readfile', methods=['GET'])
def csv_read():
    data = spark.read.csv("kognitic_trials_adc/kognitic_trials_oncology_adc_trial_drug.csv", header=True)  # headers=True Given the Tables Headers
    data.show()

    data.write \
        .format('com.mongodb.spark.sql.DefaultSource') \
        .option("uri", "mongodb://admin:password@127.0.0.1:27017/admin.tempusers") \
        .save()
    return {"data": "Data Added Succussfully"}


@app.route('/my-route')
def my_route():
    return 'This is my custom route from my_service.py!'
