import sys

from pyspark.sql import *
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import split, col
from pyspark.sql.session import SparkSession
#sc = SparkContext('local')
spark = SparkSession.builder.appName('clean').getOrCreate()

if __name__ == "__main__":
    f=open("result1.txt","a")
    overall_max=0
    overall_max_data=None
    overall_min=0
    overall_min_data=None
	for i in range(2010,2019):
    	# loading all data from folder 'temporary'
    	newInData = spark.read.csv('temporary'+str(i) ,header=False,sep=' ')
    	cleanData = newInData.drop('_c1','_c4','_c6','_c8','_c10','_c12','_c14')
    	print(names)
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
        
        max_temp=cleanData1.filter(cleanData1.MAX !=9999.9).agg( {'MAX' : 'max'} ).collect()[0][0]
        max_data=cleanData.select("STN","MAX","YEARMODA").where(cleanData.MAX == max_temp)
        if (max_temp>overall_max):
        	overall_max=max_temp
        	overall_max_data=max_data
        min_temp=cleanData1.filter(cleanData1.MIN !=9999.9).agg( {'MIN' : 'min'} ).collect()[0][0]
        min_data=cleanData.select("STN","MIN","YEARMODA").where(cleanData.MIN == min_temp)
         if (min_temp<overall_min):
        	overall_min=min_temp
        	overall_min_data=min_data
        stn_code_max= str(max_data.collect()[0][0])
        max_temp_max= str(max_data.collect()[0][1])
        year_max= str(max_data.collect()[0][2])
        f.write("STN \t MAX \t YEARMODA\n")
        f.write(stn_code_max+" \t" +  max_temp_max +"\t" + year_max +"\n")
        stn_code_min= str(min_data.collect()[0][0])
        min_temp_min= str(min_data.collect()[0][1])
        year_min= str(min_data.collect()[0][2])
        f.write("STN \t MIN \t YEARMODA\n")
        f.write(stn_code_min+" \t" +  min_temp_min +"\t" + year_min +"\n")
        max_data.unpersist()
        cleanData.unpersist()
        cleanData1.unpersist()
    stn_code_max= str(overall_max_data.collect()[0][0])
    max_temp_max= str(overall_max_data.collect()[0][1])
    year_max= str(overall_max_data.collect()[0][2])
    f.write("STN \t MAX \t YEARMODA\n")
    f.write(stn_code_max+" \t" +  max_temp_max +"\t" + year_max +"\n")
    stn_code_min= str(overall_min_data.collect()[0][0])
    min_temp_min= str(overall_min_data.collect()[0][1])
    year_min= str(overall_min_data.collect()[0][2])
    f.write("STN \t MIN \t YEARMODA\n")
    f.write(stn_code_min+" \t" +  min_temp_min +"\t" + year_min +"\n") 
    f.close()

        