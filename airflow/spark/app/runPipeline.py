import os
import sys
import shutil
from os import listdir
from os.path import isfile, join, basename

sys.path.append(os.path.join(os.path.dirname(__file__), "server", "pipeline"))

import time
import json

from lifetimes import BetaGeoFitter
import pandas as pd

from modules import orchestrator


if __name__ == "__main__":

    pipelineName = sys.argv[-3]
    modelSplit = sys.argv[-2].split(",")
    modelInfo = {
        'name':  modelSplit[0],
        'frequencyColumn': modelSplit[1],
        'recencyColumn': modelSplit[2],
        'tColumn':   modelSplit[3],
        'idColumn':   modelSplit[4],
    }

    execName = sys.argv[-1]

    execFolder = os.path.join("/usr/local/spark/resources/data/output/", execName)
    os.mkdir(execFolder)

    pipelineFile = os.path.join("/usr/local/spark/resources/pipelines/", "{}.json".format(pipelineName))

    startExec = time.time()
    
    tables = orchestrator.executePipeline(pipelineFile)

    dataFolder = os.path.join(execFolder, "data")
    os.mkdir(dataFolder)
    for tableName in tables:
        tables[tableName].to_csv("{}.csv".format(os.path.join(dataFolder, tableName)), index=False)

    modelFolder = os.path.join(execFolder, "model")
    os.mkdir(modelFolder)
    trainTable = tables["gold"]
    bgf = BetaGeoFitter(penalizer_coef = 0.005)
    bgf.fit(trainTable[modelInfo["frequencyColumn"]],
            trainTable[modelInfo["recencyColumn"]],
            trainTable[modelInfo["tColumn"]])
    bgf.save_model('{}.pkl'.format(os.path.join(modelFolder, modelInfo["name"])))

    endExec = time.time()
    
    execInfo = {
        "pipeline": pipelineName,
        "modelName": modelInfo["name"],
        "executionTime": round(endExec - startExec, 3),
        "startTime": time.strftime('%Y/%m/%d %H:%M:%S', time.localtime(startExec))
    }
    execInfoFile = os.path.join(execFolder, "execInfo.txt")
    with open(execInfoFile, "w") as f:
        f.write(json.dumps(execInfo, indent=4))

    execParamsFile = os.path.join(execFolder, "execParams.txt")
    modelInfo.update({"pipeline": pipelineName})
    with open(execParamsFile, "w") as f:
        f.write(json.dumps(modelInfo, indent=4))

    path_origem = "/usr/local/spark/resources/data/dataset"
    path_destino = os.path.join("/usr/local/spark/resources/data/bkp", execName)
    ext = "csv"
    
    os.mkdir(path_destino)

    for item in [join(path_origem, f) for f in listdir(path_origem) if isfile(join(path_origem, f)) and f.endswith(ext)]:
        shutil.move(item, join(path_destino, basename(item)))
        print('moved "{}" -> "{}"'.format(item, join(path_destino, basename(item))))
    
