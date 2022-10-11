import glob
import json
import os
from subprocess import Popen, PIPE

from flask import abort, request
from flask_restx import Namespace, Resource, fields, reqparse, inputs

import appauth
import utils


ns = Namespace('api/train', 
               description='APIs para Solicitar Novos Treinamentos e Consultar Treinamentos Antigos')

modelInfoModel = ns.model('ModelInfo', {
    'name': fields.String(
        description='Nome do Pipeline'
    ),
    'frequencyColumn': fields.String(
        description='Nome da Coluna de Frequência'
    ),
    'recencyColumn': fields.String(
        description='Nome da Coluna de Recência'
    ),
    'tColumn': fields.String(
        description='Nome da Coluna T'
    ),
    'idColumn': fields.String(
        description='Nome da Coluna de Id'
    )
})

trainParametersModel = ns.model('TrainParameters', {
    'pipeline': fields.String(
        description='Nome do Pipeline'
    ),
    'model': fields.Nested(
        modelInfoModel,
        description='Informações do Modelo'
    ),
    'alias': fields.String(
        description='Nome da Execução Atual'
    )
})

trainExecInfoModel = ns.model('TrainExecutionInfo', {
    'pipeline': fields.String(
        description='Nome do Pipeline'
    ),
    'modelName': fields.String(
        description='Nome do Modelo'
    ),
    'executionTime': fields.Float(
        description='Tempo da Execução'
    ),
    'startTime': fields.String(
        description='Data de Execução do Treinamento do Modelo'
    )
})

trainSuccessResponseModel = ns.model('TrainSuccessResponse', {
    'message': fields.String(
        description='Mensagem da Execução'
    ),
    'results': fields.Nested(
        trainExecInfoModel,
        description='Informações da Execução'
    )
})

trainInfoListModel = ns.model('TrainInfoList', {
    'totalResults': fields.Integer(
        description='Total de Execuções'
    ),
    'results': fields.Nested(
        trainExecInfoModel,
        description='Informações da Execução',
        as_list=True
    )
})


parser = reqparse.RequestParser()
parser.add_argument('token', 
                    type=str,
                    help="Validação de Acesso",
                    default=True, 
                    required=True)


@ns.route('')
class training(Resource):

    @ns.doc(params={'token': {'description': 'Token de Autenticação', 'type': 'string', 'default': ''}})
    @ns.response(500, 'Erro Interno do Servidor')
    @ns.response(401, 'Token Inválido ou Não Fornecido')
    @ns.marshal_list_with(trainInfoListModel)
    def get(self):
        
        executionFiles = glob.glob(os.path.join(os.path.dirname(__file__), "..", "..", "executions", "*", "execInfo.txt"))

        executionInfo = []
        for entry in executionFiles:
            with open(entry, "r") as f:
                executionInfo.append(json.loads(f.read()))
       
        return {"results": executionInfo, "totalResults": len(executionInfo)}

    @ns.doc(params={'token': {'description': 'Token de Autenticação', 'type': 'string', 'default': ''}})
    @ns.response(500, 'Erro Interno do Servidor')
    @ns.response(401, 'Token Inválido ou Não Fornecido')
    @ns.expect(trainParametersModel)
    @ns.marshal_with(trainSuccessResponseModel)
    def post(self):
        
        if not appauth.validateToken(**parser.parse_args()):
            ns.abort(401)

        execFolder = os.path.join(os.path.dirname(__file__), "..", "..", "executions", request.json["alias"])
        if os.path.isdir(execFolder):
            ns.abort(400, "Alias Passado Já Existe")

        process = Popen([
                         "spark-submit", 
                         os.path.join(os.path.dirname(__file__), "..", "..", "..", "runPipeline.py"), 
                         request.json["pipeline"], 
                         "'{}'".format(json.dumps(request.json["model"])), 
                         request.json["alias"]
                        ],
                        stdout=PIPE, 
                        stderr=PIPE)
        stdout, stderr = process.communicate()

        execResultsFile = os.path.join(execFolder, "execInfo.txt")
        with open(execResultsFile, "r") as f:
            results = json.loads(f.read())
        
        #print(stdout.decode("utf-8"))
        #print(stderr.decode("utf-8"))

        return {"message": "Treinamento Feito!", "results": results}


@ns.route('/<string:execAlias>')
class history(Resource):

    @ns.doc(params={'token': {'description': 'Token de Autenticação', 'type': 'string', 'default': ''}})
    @ns.response(500, 'Erro Interno do Servidor')
    @ns.response(404, 'Cliente Não Encontrado')
    @ns.response(401, 'Token Inválido ou Não Fornecido')
    @ns.marshal_with(trainExecInfoModel)
    def get(self, execAlias):
    
        if not appauth.validateToken(**parser.parse_args()):
            ns.abort(401)

        execFolder = os.path.join(os.path.dirname(__file__), "..", "..", "executions", execAlias)
        execInfoFile = os.path.join(execFolder, "execInfo.txt")

        if not os.path.isdir(execFolder):
            ns.abort(404)

        with open(execInfoFile, "r") as f:
            execInfo = json.loads(f.read())

        return execInfo

