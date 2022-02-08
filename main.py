"""
Step 1 - Installing PySpark: pip install pyspark
"""

"""
Step 2 - Data Processing:
"""

from pyspark.sql import SparkSession
import warnings

# Ignore Warnings:
warnings.filterwarnings("ignore")

"""
Step 3 - Get Start a Spark Session:
"""

session = SparkSession.builder.appName("Dataframe").getOrCreate()

print(session)

"""
Step 4 - Loading a DataBase:
"""

# 4.1 - Read a file .CSV
db = session.read.csv('data/covid_report.csv', header=True, inferSchema=True, sep=";")

# 4.2 - Checking Structure Base
print(db.printSchema())

# 4.3 - Checking the info type
print(type(db))

# 4.4 - Checking the 10 first records
print(db.head(10))
