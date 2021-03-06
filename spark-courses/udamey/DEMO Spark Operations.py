# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------

                   Spark with Python

             Copyright : V2 Maestros @2016
                    
Code Samples : Spark Processing
-----------------------------------------------------------------------------
"""
#Please run SETUP Python for Spark.py before this.

#............................................................................
##   Loading and Storing Data
#............................................................................
#%%
#Load from a collection
collData = SpContext.parallelize([4,3,8,5,8])
collData.collect()
#%%
#Load the file. Lazy initialization
autoData = SpContext.textFile("auto-data.csv")
autoData.cache()
#%%
#Loads only now.
autoData.count()
#%%
autoData.first()
#%%
autoData.take(5)
#%%
for line in autoData.collect():
    print(line)
#%%    
#Save to a local file. First collect the RDD to the master
#and then save as local file.
autoDataFile = open("auto-data-saved.csv","w")
autoDataFile.write("\n".join(autoData.collect()))
autoDataFile.close()
#%%

irisRDD = SpContext.textFile('iris.csv')

#%%

irisRDD.collect()
#%%
#............................................................................
##   Transformations
#............................................................................

#Map and create a new RDD
tsvData=autoData.map(lambda x : x.replace(",","\t"))
tsvData.take(5)
#%%
#Filter and create a new RDD
toyotaData=autoData.filter(lambda x: "toyota" in x)
toyotaData.count()
#%%
#FlatMap
words=toyotaData.flatMap(lambda line: line.split(","))
words.count()
words.take(20)
#%%
#Distinct
for numbData in collData.distinct().collect():
    print(numbData)
#%%
#Set operations
words1 = SpContext.parallelize(["hello","war","peace","world"])
words2 = SpContext.parallelize(["war","peace","universe"])
#%%
for unions in words1.union(words2).distinct().collect():
    print(unions)
#%%
for intersects in words1.intersection(words2).collect():
    print(intersects)
#%%    
#Using functions for transformation
#cleanse and transform an RDD
def cleanseRDD(autoStr) :
    if isinstance(autoStr, int) :
        return autoStr
    attList=autoStr.split(",")
    #convert doors to a number str
    if attList[3] == "two" :
         attList[3]="2"
    else :
         attList[3]="4"
    #Convert Drive to uppercase
    attList[5] = attList[5].upper()
    return ",".join(attList)
 #%%   
cleanedData=autoData.map(cleanseRDD)
#%%
cleanedData.collect()
#%%

def updatedDatatype(irisRow):
    value_list = irisRow.split(',')
    value_list = [float(val) if val.isnumeric() else val for val in value_list]
    return value_list
updatediris = irisRDD.map(updatedDatatype)

#%%
updatediris.take(5)

#%%

def get_versicolor(row):
    if row[4] == 'versicolor':
        return row
    
onlyVersicolor = updatediris.filter(get_versicolor)

#%%

onlyVersicolor.count()
#%%
#............................................................................
##   Actions
#............................................................................

#reduce - compute the sum
collData.collect()
#%% 
collData.reduce(lambda x,y: x+y)
#%%
#find the shortest line
autoData.reduce(lambda x,y: x if len(x) < len(y) else y)
#%%

#Use a function to perform reduce 
def getMPG( autoStr) :
    if isinstance(autoStr, int) :
        return autoStr
    attList=autoStr.split(",")
    if attList[9].isdigit() :
        return int(attList[9])
    else:
        return 0

#find average MPG-City for all cars    
autoData.reduce(lambda x,y : getMPG(x) + getMPG(y))/(autoData.count()-1)
#%%    
def isfloat(value):
  try:
    float(value)
    return True
  except ValueError:
    return False

def getAvgSepalLength( irisRow):
    if isinstance(irisRow, int) or isinstance(irisRow, float):
        return irisRow;
    iris_attr = irisRow.split(",");
    if isfloat(iris_attr[0]):
        return float(iris_attr[0]);
    else:
        return 0;

#%%

#print(irisRDD.take(10)[2].split(",")[0])
irisRDD.reduce(lambda x,y : getAvgSepalLength(x) + getAvgSepalLength(y))/(irisRDD.count() - 1)
#%%
#............................................................................
##   Working with Key/Value RDDs
#............................................................................

#create a KV RDD of auto Brand and Horsepower
cylData = autoData.map( lambda x: ( x.split(",")[0], x.split(",")[7]))
#%%
cylData.take(5)
#%%
cylData.keys().collect()
#%%
#Remove header row
header = cylData.first()
cylHPData= cylData.filter(lambda line: line != header)
#%%
#Find average HP by Brand
#Add a count 1 to each record and then reduce to find totals of HP and counts
addOne = cylHPData.mapValues(lambda x: (x, 1))
addOne.collect()
#%%
brandValues= addOne.reduceByKey(lambda x, y: (int(x[0]) + int(y[0]), x[1] + y[1])) 
#%%
brandValues.first()
#%%
brandValues.take(5)
#%%
print(brandValues.count())
#%%
sqlContext.setConf("spark.sql.shuffle.partitions", 2048)
#%%
#find average by dividing HP total by count total
brandValues.mapValues(lambda x: int(x[0])/int(x[1])). \
    collect()
#%%
   
iris_headers = irisRDD.first()
print(iris_headers)
#%%
iris_mapped = irisRDD.filter(lambda line : line != iris_headers)
iris_mapped.first()

#%%
def formIrisKV(row):
    iris_attr = row.split(',')
    return(iris_attr[4], iris_attr[0])
iris_kv = iris_mapped.map(formIrisKV)

#%%
smallest_petals = iris_kv.reduceByKey(lambda x,y: x if x[0] < y[0]  else y)
#%%

smallest_petals.collect()
#%%
largest_petals = iris_kv.reduceByKey(lambda x,y: x if x[0] > y[0]  else y)
largest_petals.collect()
#%%
#............................................................................
##   Advanced Spark : Accumulators & Broadcast Variables
#............................................................................

#function that splits the line as well as counts sedans and hatchbacks
#Speed optimization

    
#Initialize accumulator
sedanCount = SpContext.accumulator(0)
hatchbackCount =SpContext.accumulator(0)

#Set Broadcast variable
sedanText=SpContext.broadcast("sedan")
hatchbackText=SpContext.broadcast("hatchback")
#%%
def splitLines(line) :

    global sedanCount
    global hatchbackCount

    #Use broadcast variable to do comparison and set accumulator
    if sedanText.value in line:
        sedanCount +=1
    if hatchbackText.value in line:
        hatchbackCount +=1
        
    return line.split(",")

#%%
#do the map
splitData=autoData.map(splitLines)

#Make it execute the map (lazy execution)
splitData.count()
print(sedanCount, hatchbackCount)
#%%
#............................................................................
##   Advanced Spark : Partitions
#............................................................................
collData.getNumPartitions()
#%%
#Specify no. of partitions.
collData=SpContext.parallelize([3,5,4,7,4],4)
#%%
collData.cache()
#%%
collData.count()
#%%
collData.getNumPartitions()
#%%

avg_sepal_length = SpContext.broadcast(5.84)
abv_avg_count = SpContext.accumulator(0)
#%%
abv_avg_iris = iris_mapped
abv_avg_iris.first()
#%%
def get_abv_avg(row):
    global abv_avg_count
    attrs = row.split(",")
    if float(attrs[0]) > avg_sepal_length.value:
        abv_avg_count +=1;
    return attrs
abv_avg_rdd = abv_avg_iris.map(get_abv_avg)
#%%

print(abv_avg_rdd.count())
print(abv_avg_count)
#%%
#localhost:4040 shows the current spark instance