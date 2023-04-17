#!/usr/bin/env python
from pyspark import SparkContext
from operator import add
# On peut définir les ressources à allouer au programme via le SparkContext
# Le SparkContext peut être configuré via les options de spark-submit soit dans le programme  
sc = SparkContext("spark://smaster:7077", "MonProgramme")
myRdd =  sc.textFile("/data/tpspark/wash_dc_crime_incidents_2013.csv")

result = myRdd.map(lambda x : ("rowcount",1)).reduceByKey(add).collect()
for r in result:
    print(r)
