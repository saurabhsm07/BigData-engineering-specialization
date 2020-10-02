#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  9 15:33:58 2020

@author: maverick

Description: working with dataframes
"""

#%%
from spark100 import spark
dataset_folder = "./../../datasets/"

#%%
"""
Basic structured dataframes operations 
"""
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
"""
import a csv file in a dataframe
"""

# create a dataframe from the given csv dataset using sparksession
bank_df = (spark.read.csv(dataset_folder + 'bank.csv',
                              schema= None,
                              sep=';',
                              inferSchema= True,
                              header=True
                              )
           )
#%%
"""
general info about tavle and schema
"""

print(bank_df.show(5))
print(bank_df.printSchema())
#%%
"""
select columns in 5 different ways
"""
from pyspark.sql.functions import col, column, expr
print(bank_df.columns)
print(bank_df.select(col('job'), column('education'), expr('pdays'), bank_df['previous'], 'day').show(5))
#%%
"""
perform df column manipulation using expr and col functions
select + expr =====> selectExpr
"""
from pyspark.sql.functions import col, column, expr

colFunc= (((col('balance') + 5) * 100 ))
exprFunc = expr('((balance + 5)* 100) ')

print(bank_df.select(colFunc).show(3))
print(bank_df.select(exprFunc).show(3))
print(bank_df.selectExpr('((balance + 5)* 100) ').show(3))
#%%
"""
perform selectExpr examples
"""

print(bank_df.selectExpr('loan = "yes" as loanyes').groupBy('loanyes').count().show())
print(bank_df.selectExpr('sum(balance)').show())
#%%
"""
working with spark literals
"""
from pyspark.sql.functions import lit
print(bank_df.select(expr("*"), lit(56).alias("One")).agg({'One': 'sum'}).show())


#%%
"Adding columns to dataframe"
from pyspark.sql.functions import lit, expr
bank_df = bank_df.withColumn('hagababa',lit(786)) #simple column
bank_df = bank_df.withColumn('actual_balance', expr('if(marital = "married", (balance/2), (balance))'))
print(bank_df.show(5))
#%%
"delete a column"
bank_df = bank_df.drop(expr('hagababa')).drop(col('allah'))
print(bank_df.columns)
#%%
"rename a column"
bank_df = bank_df.withColumnRenamed('actual_balance', 'true_balance')
print(bank_df.columns)
#%%
"fitler columns 1"

"will yield same result"

print(bank_df.filter(col('marital') == 'married').count())
print(bank_df.where(col('marital') == 'married').count())
print(bank_df.filter(expr('marital') == 'married').count())
print(bank_df.filter('marital == "married"').count())
print(bank_df.where('marital == "married"').count())

#%%
"filter columns 2"

"will yield same result"

print(bank_df.filter(col('marital') == 'married').where('balance > 500').count())
print(bank_df.filter((col('marital') == 'married') & (col('balance') >  500)).count())
print(bank_df.filter('marital == "married" and balance > 500').count())

#%%
"get sample of the data"
print(bank_df.count())
print(bank_df.sample(withReplacement = False, fraction= 0.1, seed = 5).count())
#%%
" split the data into samples"

print(bank_df.count())
bank_df_2 = bank_df.randomSplit([0.25, 0.75], 5) #2 splits
print(bank_df_2[0].count(), bank_df_2[1].count())

bank_df_3 = bank_df.randomSplit([0.25, 0.25, 0.5], 5) # 3 splits
print(bank_df_3[0].count(), bank_df_3[1].count(), bank_df_3[2].count())

#%%
" union to append to the dataframe"

from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

# a dataset must be created from an array of rows
row_set_1 = [ Row('col 1'+ str(i), 'col 12'+ str(i), i) for i in range(0, 5)]  #array of rows 1
row_set_2 = [ Row('col 2'+ str(i), 'col 22'+ str(i), i) for i in range(0, 5)]   #array of rows 2

#specift schema 
schema = StructType([StructField(col_name, StringType(), True) for col_name in ['col_1', 'col_2', 'col_3']])
df1 = spark.createDataFrame(row_set_1, schema)  #dataframe 1 from rows set 1
df2 = spark.createDataFrame(row_set_2, schema)  #dataframe 2 from rows set 2


df3 = df1.union(df2)#perform union

print(df3.show())
#%%
"Sorting dataframes by columns:"


from pyspark.sql.functions import col

"orderby and sort work the same way"

print(bank_df.select('age', 'balance')
             .sort(bank_df['balance'].desc(), col('age').asc())
             .show(10))

print(bank_df.select('age', 'balance')
             .orderBy(bank_df['balance'].desc(), col('age').asc())
             .show(10))

#%%

"sorting dataframes by partition - better to do this before transformations"
print(bank_df.select('age', 'balance')
             .sortWithinPartitions(bank_df['balance'].desc(), col('age').asc())
             .show(10))


#%%
"repartition"

"""repartition when future partitions will be more than current 
    or if you will use filter by specific column allot"""
print(bank_df.rdd.getNumPartitions())

bank_df = bank_df.repartition(10) #increase partitions to 10
print(bank_df.rdd.getNumPartitions())

bank_df = bank_df.repartition(10, col('marital')) # repartition based on a column
print(bank_df.rdd.getNumPartitions())

#%%

"Coalesce"

"""
Coalesce, on the other hand, will not incur a full shuffle and will try to combine partitions
example :
    Node 1 = 1,2,3
Node 2 = 4,5,6
Node 3 = 7,8,9
Node 4 = 10,11,12

Then coalesce down to 2 partitions:

Node 1 = 1,2,3 + (10,11,12)
Node 3 = 7,8,9 + (4,5,6)
Notice that Node 1 and Node 3 did not require its original data to move.
"""
print(bank_df.rdd.getNumPartitions())
bank_df = bank_df.coalesce(5)
print(bank_df.rdd.getNumPartitions())
#%%
"""
Working with Different Types of Data
"""
#%%
"import dataset for runnig examples"

retail_df = (spark.read.format("csv")
                       .option("header", "true")
                       .option("inferSchema", "true")
                       .load(dataset_folder + "/databricks/retail-data/by-day/2010-12-01.csv"))
retail_df.printSchema()
retail_df.createOrReplaceTempView("reatail_table")
#%%
"working with booleans "
from pyspark.sql.functions import expr, col

print(retail_df.filter(col('InvoiceNo') == 536365)
         .select("InvoiceNo", "Description")
         .show(5, False))
#%%
"conditional statements working with booleans"
from pyspark.sql.functions import instr

priceFilter = col("UnitPrice") > 600 # filter condition 1
descripFilter = instr(retail_df.Description, "POSTAGE") >= 1 # filter condition 2

print('or statement : \n')
print(retail_df.where(priceFilter | descripFilter)
               .select('InvoiceNo', 'UnitPrice')
               .show(3, False)) #or statement


print('and statements : \n')

print(retail_df.where(priceFilter & descripFilter)
               .select('InvoiceNo', 'UnitPrice')
               .show(3, False)) #or statement

print(retail_df.where(priceFilter)
               .filter(descripFilter)
               .select('InvoiceNo', 'UnitPrice')
               .show(3, False)) #or statement
#%%
"Specifying conditions directly in columns "
from pyspark.sql.functions import instr

DOTCodeFilter = col("StockCode") == "DOT" #filter 1
priceFilter = col("UnitPrice") > 600    #filter 2
descripFilter = instr(col("Description"), "POSTAGE") >= 1 #filter 3

print("filtered data \n")
print(retail_df.withColumn("isExpensive", DOTCodeFilter & priceFilter & descripFilter)
              .where("isExpensive")
              .select("unitPrice", "isExpensive").show(5))
#%%
"counting things: working with numbers"

" example 1 create new column "

" all 3 return same results"
from pyspark.sql.functions import expr, pow
actual_quantity = pow((expr('quantity')* expr('unitPrice')), 2) + 5
print(retail_df.withColumn('actual_quantity', actual_quantity)
                .select('customerId','unitPrice', 'quantity', 'actual_quantity')
                .show(3, False))

print(retail_df.select(expr("CustomerId"),'unitPrice','quantity', actual_quantity.alias("actual_uantity"))
             .show(3))

print(retail_df.selectExpr("CustomerId","unitPrice","quantity","(POWER((Quantity * UnitPrice), 2.0) + 5) as actual_quantity").show(3))
#%%
"rounding things"
from pyspark.sql.functions import round, bround

print(retail_df.select('unitPrice', round('unitPrice', 1), bround('unitprice', 1)).show(5))
#%%
"Pearson correlation coefficient for two columns"

from pyspark.sql.functions import corr
print(retail_df.stat.corr("Quantity", "UnitPrice"))
print(retail_df.select(corr("Quantity", "UnitPrice")).show())
#%%
"Summary stats"
print(retail_df.describe().show())

"get summary stats manualy"
from pyspark.sql.functions import count, mean, stddev_pop, min, max

print(retail_df.select(count('Quantity').alias('count of quantity'), mean('Quantity'), stddev_pop('Quantity'), min('Quantity'), max('Quantity')).show())

"frequent items"
print(retail_df.stat.freqItems(['Quantity', 'UnitPrice']).show())

"approx quantile"
colName = "UnitPrice"
quantileProbs = [0.5]
relError = 0.05
print(retail_df.stat.approxQuantile("UnitPrice", quantileProbs, relError))
#%%
" string manipulations "

"changing column case"
from pyspark.sql.functions import initcap, upper, lower, col

print(retail_df.select(initcap(col("Description")), 
                       lower(col('Description')), 
                       upper(col('Description'))).show(5, False))
#%%
"regex replace"
from pyspark.sql.functions import regexp_replace
regex_string = "BLACK|WHITE|RED|GREEN|BLUE"
print(retail_df.select(
         regexp_replace(col("Description"), 
                           regex_string, 
                           "COLOR").alias("color_clean"),
                         col("Description"))
        .show(2))
#%%
"regex translate"
from pyspark.sql.functions import translate, lit
print(retail_df.select(translate(lit("Descreeption"), "leet", "1627"),lit("Descreption"))
.show(2))
#%%
"regex extract characters"
from pyspark.sql.functions import regexp_extract
extract_str = "(BLACK|WHITE|RED|GREEN|BLUE)"
print(retail_df.select(
regexp_extract(col("Description"), extract_str, 1).alias("color_clean"),
col("Description")).show(2))
#%%
"check if string exists in column values"
# in Python
from pyspark.sql.functions import instr, col
containsBlack = instr(col("Description"), "BLACK") >= 1
containsWhite = instr(col("Description"), "WHITE") >= 1
print(retail_df.withColumn("hasSimpleColor", containsBlack | containsWhite)
               .where("hasSimpleColor")
               .select("Description").show(3, False))
#%%
"creating multiple filters based on requirements"

#exmple 1
from pyspark.sql.functions import expr, locate

simpleColors = ["black", "white", "red", "green", "blue"]
def color_locator(column, color_string):
    return (locate(color_string.upper(), column)
            .cast("boolean")
            .alias("is_" + color_string))


selectedColumns = [color_locator(retail_df.Description, c) for c in simpleColors]
selectedColumns.append(expr("*")) # has to a be Column type


print(retail_df.select(*selectedColumns)
                .where(expr("is_blue AND is_red"))
               .select("Description").show(10, False))
#%%
#example 2
from pyspark.sql.functions import expr, instr, locate

def create_filter(sub_string, column_name):
    # return locate(sub_string.upper(), column_name).cast('boolean').alias('contains_' + sub_string)
    return (instr(column_name, sub_string.upper()) >= 1).alias('contains_' + sub_string)
      

text_data = ['random', 'test', 'cool', 'book', 'money']
complexFilter = [create_filter(text, retail_df.Description) for text in text_data] 
complexFilter.append(expr('*'))
print(complexFilter)
print(retail_df.select(*complexFilter)
                .where(expr("contains_money OR contains_random"))
               .select("Description").show(10, False))
#%%
"working with dates and timestamps"

"simple df with date and time stamp"
from pyspark.sql.functions import current_date, current_timestamp
date_df = (spark.range(10)
              .withColumn("today", current_date())
              .withColumn("timestamp", current_timestamp()))

print(date_df.printSchema())
print(date_df.show(5, False))
#%%
"add, sub track time"
from pyspark.sql.functions import date_add, date_sub, col
date_df.select(date_sub(col("today"), 5).alias('5 days ago'), date_add(col("today"), 5).alias('5 days ahead')).show(1)
date_df.selectExpr('(date_sub(today, 5)) as `5 days ago`','date_add(today, 5) as `5 days togo`').show(1, False)
#%%
"number of days and months between 2 dates"
from pyspark.sql.functions import datediff, months_between, to_date, lit, expr
print(date_df.withColumn("week_ago", date_sub(col("today"), 7))
              .select(datediff(col("week_ago"), col("today"))).show(1))

print(date_df.select(
                     to_date(lit("2016-01-01")).alias("start"),
                     to_date(lit("2017-05-22")).alias("end"))
            .select(months_between(col("start"), col("end"))).show(1))

print(date_df.withColumn('start', to_date(lit("2014-01-01")))
             .withColumn('end',to_date(lit("2017-05-22")))
            .select(months_between(col("start"), col("end"))).show(1))
#%%
"providing date format"
from pyspark.sql.functions import to_date

dateFormat = "yyyy-dd-MM"

cleanDateDF = spark.range(1).select(
to_date(lit("2017-12-11"), dateFormat).alias("date"),
to_date(lit("2017-20-12"), dateFormat).alias("date2"))
cleanDateDF.show()


from pyspark.sql.functions import to_timestamp
cleanDateDF.select(to_timestamp(col("date"), dateFormat).alias('timestamped date')).show()
#%%
"comparing data"
cleanDateDF.filter(col("date2") > lit("2019-12-12")).show()
#%%
"working with null data"

"Coalesce"

from pyspark.sql.functions import coalesce
retail_df.select(coalesce(col("Description"), col("CustomerId"))).show(20, False)
#%%
print(bank_df.select('job','loan').show(5)) 
#%%
# filter with distinct
print(bank_df.filter((bank_df['age'] > 20) & (25 > bank_df['age']))
       .select('job')
       .distinct()
       .show())
#%%
#%%
# select 
print(bank_df.groupBy('job', 'loan')
             .agg({'age': 'avg'})
             .orderBy(bank_df['job'], bank_df['loan'].desc())
             .withColumnRenamed('avg(age)', 'avg-age')
                # .explain()
              .show()
          )

#%%
numbers1000 = spark.range(1000).toDF('numbers')
evenNumbs = numbers1000.where(numbers1000['numbers'] % 2 == 0)
print(evenNumbs.count())
#%%
flight_summary = spark.read.csv(dataset_folder + 'flight-summary-2010.csv',
                                    schema= None,
                                    sep=",",
                                    inferSchema=True,
                                    header= True)
#%%
flight_summary = flight_summary.withColumnRenamed('DEST_COUNTRY_NAME', 'destination').withColumnRenamed('ORIGIN_COUNTRY_NAME', 'origin')
#%%
from pyspark.sql.functions import *
flight_transformation = (flight_summary
                         .withColumnRenamed('DEST_COUNTRY_NAME', 'destination')
                         .withColumnRenamed('ORIGIN_COUNTRY_NAME', 'origin')
                         .groupBy('destination')
                         .agg({'count': 'sum'})
                         .withColumnRenamed('sum(count)', 'times-visited')
                         .orderBy(desc('times-visited')))
print(flight_transformation.explain())

#%%
flight_df= flight_transformation

# register as a temp view
flight_df.createOrReplaceTempView('flightsinfo')
print(spark.sql('select * from flightsinfo').show())
#%%
# creating a dataframe from rdd
from pyspark.sql import Row

people_text_RDD = SpContext.textFile(dataset_folder + '/spark-data/people.txt')
people_row_RDD = (people_text_RDD.map(lambda person: person.split(','))
                     .map(lambda parts: Row(name=parts[0], age=parts[1])))
people_df = spark.createDataFrame(people_row_RDD)
print(people_df.show())
print(people_df.printSchema())
#%%
# programmatically specifying the schema 
from pyspark.sql.types import *

people_text_RDD_1 = SpContext.textFile(dataset_folder + '/spark-data/people.txt')
people_tuple_RDD = (people_text_RDD_1.map(lambda person: person.split(','))
                                    .map(lambda parts: (parts[0], int(parts[1]))))
column_names = ['name', 'age']
fields = [StructField(column, StringType(), True) for column in column_names]
schema = StructType(fields)

people_df_1 = spark.createDataFrame(people_tuple_RDD, schema)
print(people_df_1.show())
print(people_df_1.printSchema())


#%%

