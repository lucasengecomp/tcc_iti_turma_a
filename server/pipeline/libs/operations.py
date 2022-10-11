import os
import sys

sys.path.append(os.path.dirname(__file__))

import pyspark
from lifetimes import BetaGeoFitter
import json

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, FloatType
from pyspark.sql.functions import col,udf,to_date,lit,datediff,date_add
from pyspark.sql.functions import max as sqlMax
from encryptionAlgorhitms import AESDeterministicCipher, AESNonDeterministicCipher
from encryptionAlgorhitms import FernetCipher
from encryptionAlgorhitms import MD5Hash

spark = SparkSession.builder.master("local[2]") \
                    .appName("dataConversion") \
                    .getOrCreate()


def readTable(tableName):

    df = spark.read \
              .option("header",True) \
              .option("inferSchema", True) \
              .option("delimiter", ",") \
              .csv(os.path.join(os.path.dirname(__file__), "..", "..", "data", "{}.csv".format(tableName)))

    return df


def readFile(filePath):

    df = spark.read \
              .option("header",True) \
              .option("inferSchema", True) \
              .option("delimiter", ",") \
              .csv(filePath)

    return df


def selectColumns(df, columnList):

    df = df.select([c for c in df.columns if c in columnList])

    return df


def anonColumns(df, columnList):
    
    for entry in columnList:
        #enc = udf(lambda x: AESDeterministicCipher.encrypt(key, x), StringType())
        #enc = udf(lambda x: FernetCipher.encrypt(key, x), StringType())
        enc = udf(lambda x: MD5Hash.encode(x), StringType())
        df = df.withColumn(entry, enc(col(entry)))

    return df


def joinTables(df1, df2, field1, field2, optype="fullouter"):

    if field1 == field2:

        return df1.join(df2, field1, optype)

    else:
        return df1.join(df2, df1[field1] == df2[field2], optype) 


def filterColumn(df, column, condition, operation):

    if operation == "==":

        return df[df[column] == condition]

    elif operation == "!=":
	
        return df[df[column] != condition]


def columnToDate(df, column, dateFormat, output):

    return df.select(to_date(col(column), dateFormat).alias(output), *[c for c in df.columns])


def daysBetweenDates(df, col1, col2, output):

    return df.select(datediff(col(col1), col(col2)).alias(output), *[c for c in df.columns])


def addDaysToDate(df, column, numDays, output):

    return df.withColumn(output, date_add(df[column], numDays))


def groupbyColumn(df, columns, agg=None):
    
    if isinstance(columns, str):
        df = df.groupBy(columns)

    elif isinstance(columns, list):
        df = df.groupBy(*columns)

    else:
        raise Exception("Invalid type for columns")

    if agg is not None:
        return df.agg(*[item["type"](item["column"]) for item in agg])

    else:
        return df


def columnsOperations(df, column1, column2, opType, outputColumn):

    if opType == "+":
        return df.withColumn(outputColumn, col(column1) + col(column2))

    elif opType == "-":
        return df.withColumn(outputColumn, col(column1) - col(column2))

    elif opType == "/":
        return df.withColumn(outputColumn, col(column1) / col(column2))

    elif opType == "*":
        return df.withColumn(outputColumn, col(column1) * col(column2))


def literalsOperations(df, column, value, opType, outputColumn):

    if opType == "+":
        return df.withColumn(outputColumn, col(column) + lit(value))

    elif opType == "-":
        return df.withColumn(outputColumn, col(column) - lit(value))

    elif opType == "/":
        return df.withColumn(outputColumn, col(column) / lit(value))

    elif opType == "*":
        return df.withColumn(outputColumn, col(column) * lit(value))



def renameColumns(df, oldName, newName):
    
    return df.withColumnRenamed(oldName, newName)


def saveTable(df, saveType, **kwargs):

    if saveType == "csv":

        df.repartition(1) \
          .write.format("com.databricks.spark.csv") \
          .option("header", "true") \
          .save("{}.csv".format(kwargs["filename"]))


def getMaxValue(df, column, output):

    maxValue = df.agg(sqlMax(column)).collect()[0][0]

    return df.withColumn(output, lit(maxValue))


def copyTable(df, name):

    return df.alias(name)


def trainBetaGeoFitter(df, frequencyCol, recencyCol, tCol, name, penalizer_coef=0.005, save=None):

    dfPandas = df.toPandas()

    bgf = BetaGeoFitter(penalizer_coef = 0.005)
    bgf.fit(dfPandas['frequency'], 
            dfPandas['recency'], 
            dfPandas['T'])

    print(bgf)

    if save is not None:

        bgf.save_model('{}.pkl'.format(save))

    return bgf


def predictBetaGeoFitter(df, model, frequencyCol, recencyCol, tCol, time, output):

    def predict(f, r, t):

        return model.predict(time, f, r, t)

    dfPandas = df.toPandas()
    dfPandas["predicted"] = dfPandas.apply(lambda x: predict(x[frequencyCol], x[recencyCol], x[tCol]), axis=1)

    return spark.createDataFrame(dfPandas)
