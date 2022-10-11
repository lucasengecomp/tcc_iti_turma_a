import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "server", "pipeline"))

import time
import json

from lifetimes import BetaGeoFitter
import pandas as pd

import orchestrator


if __name__ == "__main__":

    pipelineName = sys.argv[-3]
    modelInfo = json.loads(sys.argv[-2].strip()[1:-1])
    execName = sys.argv[-1]

    execFolder = os.path.join(os.path.dirname(__file__), "server", "executions", execName)
    os.mkdir(execFolder)

    pipelineFile = os.path.join(os.path.dirname(__file__), "server", "pipeline", "registered", "{}.json".format(pipelineName))

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
