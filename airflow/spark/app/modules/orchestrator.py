import json
import sys

from pyspark.sql import functions as F
from pyspark.sql import SparkSession

from modules import operations

OPS = {
    "readFile": operations.readFile,
    "readTable": operations.readTable,
    "selectColumns": operations.selectColumns,
    "filterColumn": operations.filterColumn,
    "columnToDate": operations.columnToDate,
    "groupbyColumn": operations.groupbyColumn,
    "renameColumns": operations.renameColumns,
    "joinTables": operations.joinTables,
    "getMaxValue": operations.getMaxValue,
    "copyTable": operations.copyTable,
    "daysBetweenDates": operations.daysBetweenDates,
    "saveTable": operations.saveTable,
    "addDaysToDate": operations.addDaysToDate,
    "columnsOperations": operations.columnsOperations,
    "literalsOperations": operations.literalsOperations,
    "trainBetaGeoFitter": operations.trainBetaGeoFitter,
    "predictBetaGeoFitter": operations.predictBetaGeoFitter
}

AGGS = {
    "sum": F.sum,
    "min": F.min,
    "max": F.max,
    "count": F.count
}

spark = (SparkSession
    .builder
    .getOrCreate()
)

def executePipeline(pipelineFile):

    with open(pipelineFile) as f:
        pipeline = json.loads(f.read())

    tables = {}
    models = {}

    for entry in pipeline:
        
        print(entry["name"])

        for op in entry["operations"]:

            print(op["type"])

            if op["type"] == "readFile" or op["type"] == "readTable":

                tables[entry["name"]] = OPS[op["type"]](**op["parameters"])

            elif op["type"] == "saveTable":

                OPS[op["type"]](tables[entry["name"]], **op["parameters"])

            elif op["type"] == "copyTable":

                tables[entry["name"]] = OPS[op["type"]](tables[op["parameters"]["df"]], entry["name"]) 

            elif op["type"] == "joinTables":

                tables[entry["name"]] = OPS[op["type"]](tables[op["parameters"]["df1"]], 
                                                        tables[op["parameters"]["df2"]], 
                                                        op["parameters"]["field1"], 
                                                        op["parameters"]["field1"],
                                                        op["parameters"].get("optype", "fullouter"))
            
            elif op["type"] == "groupbyColumn":

                if "agg" in op["parameters"]:
                
                    op["parameters"]["agg"] = [{"type": AGGS[item["type"]], "column": item["column"]}  for item in op["parameters"]["agg"]]

                tables[entry["name"]] = OPS[op["type"]](tables[entry["name"]], **op["parameters"])

            elif op["type"] == "trainBetaGeoFitter":

                models[op["parameters"]["name"]] = OPS[op["type"]](tables[entry["name"]], **op["parameters"]) 
           
            elif op["type"] == "predictBetaGeoFitter":


                op["parameters"]["model"] = models[op["parameters"]["model"]]

                tables[entry["name"]] = OPS[op["type"]](tables[entry["name"]], **op["parameters"])
 
            else:

                tables[entry["name"]] = OPS[op["type"]](tables[entry["name"]], **op["parameters"])

        
    return {key:tables[key].toPandas() for key in  tables}
