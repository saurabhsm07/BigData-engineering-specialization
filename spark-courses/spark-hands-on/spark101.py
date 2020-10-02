#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr  8 15:22:28 2020

@author: maverick

Description: architecture study examples, test class
"""


#%%
from spark100 import spark
dataset_folder = "./../../datasets/"
#%%

"""
Read spark UI features based on below simple query
"""
#query to learn spark UI
test_op = (spark.read
                .option("header", "true")
                .csv(dataset_folder+"/databricks/retail-data/all/online-retail-dataset.csv")
                .repartition(2)
                .selectExpr("instr(Description, 'GLASS') >= 1 as is_glass")
                .groupBy("is_glass")
                .count()
                .collect())

print(test_op)
#%%

"""
check if col and expr result to same output
"""


bankDf = (spark.read.option('header', 'true')
                    .csv(dataset_folder + '/bank.csv'))


print(bankDf.show(5))

#%%
from pyspark.sql.functions import expr, col
 
data1 = expr('(((balance + 5)* 100) > loan)')
print(data1)

data2 = (((col('balance') + 5) * 100 ) > col('loan'))
print(data2)
print(bankDf.columns)

#%%

"""
Create a datafrane by creating row objs
"""
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, LongType
myManualSchema = StructType([
StructField("some", StringType(), True),
StructField("col", StringType(), True),
StructField("names", LongType(), False)
])
myRow = Row("Hello", None, 1)
myDf = spark.createDataFrame([myRow], myManualSchema)
myDf.show()
#%%