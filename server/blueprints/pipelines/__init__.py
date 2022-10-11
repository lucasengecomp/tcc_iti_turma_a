import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", "libs"))
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", "pipeline"))

import glob
import json
from http import HTTPStatus

from flask import abort, request
from flask_restx import Namespace, Resource, fields, reqparse, inputs

import appauth
import utils


ns = Namespace('api/pipeline', 
               description='APIs para Solicitar Novos Treinamentos e Submeter Arquivos de Treinamento')


parametersFields = {"*": fields.Wildcard(utils.ParametersParser())}
operationModel = ns.model('Operation', {
    'type': fields.String(
        description='Nome da Operação Aplicado'
    ),
    'parameters': fields.Nested(ns.model("Parameters", parametersFields))
})


pipelineModel = ns.model('Pipeline', {
    'name': fields.String(
        description='Nome da Tabela que Conterá o Resultado das Operações'
    ),
    'operations': fields.Nested(
        operationModel,
        description='Conjunto de Operações que Serão Aplicadas na Tabela',
        as_list=True
    )
})

pipelineWrapperModel = ns.model('PipelineWrapper', {
    'name': fields.String(
        description='Nome do Pipeline'
    ),
    'pipeline': fields.Nested(
        pipelineModel,
        description='Conjunto de Instruções do Pipeline',
        as_list=True
    )
})

pipelineWrapperListModel = pipelineModel = ns.model('pipelineWrapperList', {
    'totalRecords': fields.Integer(
        description='Número de Pipelines Registrados',
    ),
    'pipelines': fields.Nested(
        pipelineWrapperModel,
        description='Conjunto de Pipelines',
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
class pipeline(Resource):

    @ns.doc(params={'token': {'description': 'Token de Autenticação', 'type': 'string', 'default': ''}})
    @ns.response(500, 'Erro Interno do Servidor')
    @ns.response(401, 'Token Inválido ou Não Fornecido')
    @ns.marshal_list_with(pipelineWrapperListModel)
    def get(self):

        if not appauth.validateToken(**parser.parse_args()):
            ns.abort(401)          

        output = {"pipelines": []}

        for item in glob.glob(os.path.join(os.path.dirname(__file__), "..", "..", "pipeline", "registered", "*.json")):
        
            with open(item, "r") as f:
                output["pipelines"].append({"name": os.path.basename(item).split(".")[0], "pipeline": json.loads(f.read())})

        output["totalRecords"] = len(output["pipelines"])

        return output

    @ns.doc(params={'token': {'description': 'Token de Autenticação', 'type': 'string', 'default': ''}})
    @ns.response(500, 'Erro Interno do Servidor')
    @ns.response(401, 'Token Inválido ou Não Fornecido')
    @ns.expect(pipelineWrapperModel)
    @ns.marshal_with(pipelineWrapperModel)
    def post(self):
        
        registeredPipelines = glob.glob(os.path.join(os.path.dirname(__file__), "..", "..", "pipeline", "registered", "*.json"))
        registeredPipelines = [os.path.basename(item).split(".")[0] for item in registeredPipelines]

        if request.json['name'] in registeredPipelines:
            ns.abort(400, 'Pipeline Já Cadastrado')

        pipelineFile = os.path.join(os.path.dirname(__file__), "..", "..", "pipeline", "registered", "{}.json".format(request.json['name']))

        with open(pipelineFile, "w") as f:
            f.write(json.dumps(request.json['pipeline'], indent=4))

        return {"message": "Pipeline Criado"}


    @ns.doc(params={'token': {'description': 'Token de Autenticação', 'type': 'string', 'default': ''}})
    @ns.response(500, 'Erro Interno do Servidor')
    @ns.response(401, 'Token Inválido ou Não Fornecido')
    @ns.expect(pipelineWrapperModel)
    @ns.marshal_with(pipelineWrapperModel)
    def put(self):
        
        registeredPipelines = glob.glob(os.path.join(os.path.dirname(__file__), "..", "..", "pipeline", "registered", "*.json"))
        registeredPipelines = [os.path.basename(item).split(".")[0] for item in registeredPipelines]

        if request.json['name'] not in registeredPipelines:
            ns.abort(400, 'Pipeline Não Existe')


        pipelineFile = os.path.join(os.path.dirname(__file__), "..", "..", "pipeline", "registered", "{}.json".format(request.json['name']))

        with open(pipelineFile, "w") as f:
            f.write(json.dumps(request.json['pipeline'], indent=4))

        return {"message": "Pipeline Atualizado"}


@ns.route('/<string:pipelineName>')
class single(Resource):

    @ns.doc(params={'token': {'description': 'Token de Autenticação', 'type': 'string', 'default': ''}})
    @ns.response(500, 'Erro Interno do Servidor')
    @ns.response(404, 'Cliente Não Encontrado')
    @ns.response(401, 'Token Inválido ou Não Fornecido')
    @ns.marshal_with(pipelineWrapperModel)
    def get(self, pipelineName):
    
        if not appauth.validateToken(**parser.parse_args()):
            ns.abort(401)

        pipelineFile = os.path.join(os.path.dirname(__file__), "..", "..", "pipeline", "registered", "{}.json".format(pipelineName))

        with open(pipelineFile, "r") as f:
            pipelineInstructions = json.loads(f.read())

        return {"name": os.path.basename(pipelineFile).split(".")[0], "pipeline": pipelineInstructions}

