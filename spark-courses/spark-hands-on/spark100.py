#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 19 11:11:27 2020

@author: maverick

Description: spark setup and spark session initialization
"""


#%%
import os
import sys
        
# add working directory
os.chdir("/home/maverick/workspace/personal-workspace/spark-workspace/spark-course/spark-hands-on")

# Create a variable for our root path
SPARK_HOME = os.environ['SPARK_HOME']

#Add the following paths to the system path. Please check your installation
#to make sure that these zip files actually exist. The names mig4ht change
#as versions change.
sys.path.insert(0,os.path.join(SPARK_HOME,"python"))
sys.path.insert(0,os.path.join(SPARK_HOME,"python","lib"))
sys.path.insert(0,os.path.join(SPARK_HOME,"python","lib","pyspark.zip"))
sys.path.insert(0,os.path.join(SPARK_HOME,"python","lib","py4j-0.10.7-src.zip"))


from pyspark.sql import SparkSession
#%%
spark = (SparkSession.builder
                         .master("local")
                         .appName("Maverick")
                         .config("spark.executor.memory", "1g")
                         .config("spark.cores.max","2")
                         .config("spark.sql.warehouse.dir", "/home/maverick/workspace/personal-workspace/spark-workspace/data-dump/dump")
                         .config("spark.local.dir", "/home/maverick/workspace/personal-workspace/spark-workspace/data-dump/dump")
                         .getOrCreate())
