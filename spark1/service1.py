from flask import Flask
from pyspark.sql import SparkSession, Row
from sqlalchemy import create_engine
from flask import render_template
app = Flask(__name__)

spark = SparkSession.builder.appName("spark1").config("spark.jars",
                                                      "/home/decoders/jdbc_driver/mssql/sqljdbc_12.4.1.0_enu/sqljdbc_12.4/enu/jars/mssql-jdbc-12.4.1.jre8.jar").getOrCreate()


@app.route('/readfile', methods=['GET'])
def csv_read():
    current_db = spark.catalog.currentDatabase()
    print(current_db)
    data = spark.read.csv("kognitic_trials_adc/kognitic_trials_oncology_adc_trial_drug.csv",
                          header=True)  # headers=True Given the Tables Headers
    data.show()
    getdata(data)
    return getdata(data)


def getdata(df):
    data = spark.read.csv("kognitic_trials_adc/kognitic_trials_oncology_adc_trial_drug.csv",
                          header=True)  # headers=True Given the Tables Headers

    df.write.format("jdbc") \
        .option("url",
                "jdbc:sqlserver://localhost:1433;databaseName=clinical_data_base;encrypt=true;trustServerCertificate=true") \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .option("dbtable", "clinical_trail_drugs") \
        .option("user", "sa") \
        .option("password", "Welcome123") \
        .mode("overwrite") \
        .save()
    return render_template('display.html', string='Data Added on DataBase')


@app.route('/my-route')
def my_route():
    return 'This is my custom route from my_service.py!'
