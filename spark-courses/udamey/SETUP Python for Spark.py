# -*- coding: utf-8 -*-
"""
Make sure you give execute privileges
-----------------------------------------------------------------------------

           Spark with Python: Setup Spyder IDE for Spark

             Copyright : V2 Maestros @2016
                    
Execute this script once when Spyder is started on Windows
-----------------------------------------------------------------------------
"""
#%%
import os
import sys

# NOTE: Please change the folder paths to your current setup.
#Windows
if sys.platform.startswith('win'):
    print("win")
    #Where you downloaded the resource bundle
    os.chdir("E:/MyProjects-Workspace/Personal-Workspace/Spark/Resources/udamey")
    #Where you installed spark.    
    os.environ['SPARK_HOME'] = 'E:/MyProjects-Workspace/Personal-Workspace/Spark/spark-2.4.4-bin-hadoop2.7'
    os.environ['JAVA_HOME'] = "D:/Java/jdk1.8.0_92"
    os.environ['HADOOP_HOME'] = 'E:/MyProjects-Workspace/Personal-Workspace/Spark/spark-2.4.4-bin-hadoop2.7/winutils'
#other platforms - linux/mac
else:
    print("ubuntu !!!")
    os.chdir("/home/maverick/workspace/personal-workspace/spark-tutorials/spark-course/udamey")
    os.environ['SPARK_HOME'] = '/home/maverick/program-files/spark/spark-2.4.4-bin-hadoop2.7'

os.curdir
#%%
# Create a variable for our root path
SPARK_HOME = os.environ['SPARK_HOME']

#Add the following paths to the system path. Please check your installation
#to make sure that these zip files actually exist. The names might change
#as versions change.
sys.path.insert(0,os.path.join(SPARK_HOME,"python"))
sys.path.insert(0,os.path.join(SPARK_HOME,"python","lib"))
sys.path.insert(0,os.path.join(SPARK_HOME,"python","lib","pyspark.zip"))
sys.path.insert(0,os.path.join(SPARK_HOME,"python","lib","py4j-0.10.7-src.zip"))

#Initialize SparkSession and SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkContext

#%%
#Create a Spark Session

SpSession = SparkSession \
    .builder \
    .master("local") \
    .appName("Maverick") \
    .getOrCreate()
    .config("spark.executor.memory", "1g") \
    .config("spark.cores.max","2") \
    .config("spark.sql.warehouse.dir", "/home/maverick/workspace/personal-workspace/spark-tutorials/data-dump/dump")\
    .config("spark.local.dir", "/home/maverick/workspace/personal-workspace/spark-tutorials/data-dump/dump")\

#%%
#Get the Spark Context from Spark Session    
SpContext = SpSession.sparkContext

#Test Spark
testData = SpContext.parallelize([3,6,4,2])
testData.count()
#check http://localhost:4040 to see if Spark is running

#%%
from os import listdir
print(listdir('{0}/'))