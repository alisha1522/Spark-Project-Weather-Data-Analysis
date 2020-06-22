import sys

from pyspark.sql import *
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import split, col
from pyspark.sql.session import SparkSession
#sc = SparkContext('local')
spark = SparkSession.builder.appName('clean').getOrCreate()

if __name__ == "__main__":
        f=open("result3","a")
    	# loading all data from folder 'temporary'
    	newInData = spark.read.csv('temporary2019',header=False,sep=' ')
    	cleanData = newInData.drop('_c1','_c4','_c6','_c8','_c10','_c12','_c14')
    	# cleaning all the data
    	cleanData = cleanData.withColumnRenamed('_c0','STN').withColumnRenamed('_c2','YEARMODA')\
                    .withColumnRenamed('_c3','TEMP').withColumnRenamed('_c5','DEWP')\
                    .withColumnRenamed('_c7','SLP').withColumnRenamed('_c9','STP')\
                    .withColumnRenamed('_c11','VISIB').withColumnRenamed('_c13','WDSP')\
                    .withColumnRenamed('_c15','MXSPD').withColumnRenamed('_c16','GUST')\
                    .withColumnRenamed('_c17','MAX').withColumnRenamed('_c18','MIN')\
                    .withColumnRenamed('_c19','PRCP').withColumnRenamed('_c20','SNDP')\
                    .withColumnRenamed('_c21','FRSHTT')
    	cleanData=cleanData.withColumn("PRCP", split(col("PRCP"), "[A-I]").getItem(0)).withColumn("P1", split(col("PRCP"), "[A-I]").getItem(1))
    	cleanData=cleanData.withColumn("MIN", split(col("MIN"), "\\*").getItem(0)).withColumn("mnm", split(col("MIN"), "\\*").getItem(1))
    	cleanData=cleanData.withColumn("MAX", split(col("MAX"), "\\*").getItem(0)).withColumn("mxm", split(col("MAX"), "\\*").getItem(1))
    	cleanData=cleanData.withColumn("MAXM", cleanData['MAX'].cast('double'))
    	cleanData=cleanData.withColumn("MINM", cleanData['MIN'].cast('double'))
    	cleanData=cleanData.withColumn("GUSTN", cleanData['GUST'].cast('double'))
    	cleanData=cleanData.withColumn("PRCPN", cleanData['PRCP'].cast('double'))
    	# dropping old columns
        cleanData1 = cleanData.drop('P1', 'mnm', 'mxm', 'MAX', 'MIN', 'GUST', 'PRCP')

     # renaming columns
        cleanData1=cleanData1.withColumnRenamed('PRCPN','PRCP')
        cleanData1=cleanData1.withColumnRenamed('MAXM','MAX')
        cleanData1=cleanData1.withColumnRenamed('MINM','MIN')
        cleanData1=cleanData1.withColumnRenamed('GUSTN','GUST')
        cleanData1.show()
        
        stp=cleanData1.count()
        miss_stp=cleanData1.filter(cleanData1.STP ==9999.9).count()
 		percent=(float(miss_stp)/float(stp))*100  
        f.write("PERCENTAGE MISSING STP\n")
        f.write(str(percent)"\n")
        cleanData.unpersist()
        cleanData1.unpersist()
	    f.close()

        