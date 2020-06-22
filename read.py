from pyspark.sql.session import SparkSession
#sc = SparkContext('local')
spark = SparkSession.builder.appName('clean').getOrCreate()

if __name__ == "__main__":
    for i in range(2010,2020):
        file_path = "/data/weather/"+ str(i)
        inTextData = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path)
        #inTextData.show(2, False)

        name_list = inTextData.schema.names
        print(name_list)
        name_list = str(name_list).strip("['']").split(' ')
        names = []
        for item in name_list:
            if len(item)>0:
                names.append(item)
        print(names)

        rdd1 = inTextData.rdd
        rdd1.take(2)
        rdd2 = rdd1.map(lambda x: str(x).split('=')[1])
        rdd2.take(4)
        rdd3 = rdd2.map(lambda x: ' '.join(x.split()))
        rdd3.take(4)
        rdd4 = rdd3.map(lambda x: x[1:-2])
        #rdd4.lambda x: x[1:-2])
        # saving data in folder 'temporary'
        rdd4.saveAsTextFile('temporary'+ str(i))

        # loading all data from folder 'temporary'
        newInData = spark.read.csv('temporary'+str(i),header=False,sep=' ')
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
        cleanData1.write.csv("clean"+str(i)+".csv")