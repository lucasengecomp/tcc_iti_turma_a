import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", "libs"))

import csv
import glob
import json

from flask import abort, request
from flask_restx import Namespace, Resource, fields, reqparse, inputs
from lifetimes import BetaGeoFitter

import appauth


ns = Namespace('api/clients', 
               description='APIs para obtenção de predições de clientes')


clientModel = ns.model('Client', {
    'id': fields.String(
        description='Identificador do Cliente'
    ),
    'predict': fields.Float(
        description='Predição para Cliente'
    )
})


clientListModel = ns.model('ClientList', {
    'clients': fields.Nested(
        clientModel,
        description='Lista de Clientes',
        as_list=True
    ),
    'totalRecords': fields.Integer(
        description='Número de Clientes',
    ),
})


parser = reqparse.RequestParser()
parser.add_argument('token', 
                    type=str,
                    help="Validação de Acesso",
                    default=True, 
                    required=True)


@ns.route('')
class all(Resource):

    @ns.doc(params={'token': {'description': 'Token de Autenticação', 'type': 'string', 'default': '', 'required': True},
                    'alias': {'description': 'Alias da Execução Cujo Modelo Será Utilizado', 'type': 'string', 'default': '', 'required': True}})
    @ns.response(500, 'Erro Interno do Servidor')
    @ns.response(401, 'Token Inválido ou Não Fornecido')
    @ns.marshal_list_with(clientListModel)
    def get(self):

        args = request.args.to_dict()

        if not appauth.validateToken(**args):
            abort(401)          

        execFolder = os.path.join(os.path.dirname(__file__), "..", "..", "executions", args["alias"])
        clientsInfo = os.path.join(execFolder, "data", "gold.csv")

        with open(clientsInfo, "r") as f:
            dictReader = csv.DictReader(f)
            clientsData = list(dictReader)

        paramsFile = os.path.join(execFolder, "execParams.txt")
        with open(paramsFile, "r") as f:
            modelParams = json.loads(f.read())

        modelFile = os.path.join(execFolder, "model", "{}.pkl".format(modelParams["name"]))
        bgf = BetaGeoFitter()
        bgf.load_model(modelFile)
        output = []
        for entry in clientsData:

            # Melhorar para generalizar coluna de id e frequencia, recencia e T
            output.append({"id": entry[modelParams["idColumn"]], "predict": bgf.predict(4, 
                                                                                        int(entry[modelParams["frequencyColumn"]]), 
                                                                                        int(entry[modelParams["recencyColumn"]]), 
                                                                                        int(entry[modelParams["tColumn"]]))})

        return {
            'clients': output,
            'totalRecords': len(output) 
        }


@ns.route('/<string:clientId>')
class single(Resource):

    @ns.doc(params={'token': {'description': 'Token de Autenticação', 'type': 'string', 'default': '', 'required': True},
                    'alias': {'description': 'Alias da Execução Cujo Modelo Será Utilizado', 'type': 'string', 'default': '', 'required': True}})
    @ns.response(500, 'Erro Interno do Servidor')
    @ns.response(404, 'Cliente Não Encontrado')
    @ns.response(401, 'Token Inválido ou Não Fornecido')
    @ns.marshal_with(clientModel)
    def get(self, clientId):
    
        args = request.args.to_dict()

        if not appauth.validateToken(**args):
            abort(401)

        execFolder = os.path.join(os.path.dirname(__file__), "..", "..", "executions", args["alias"])
        clientsInfo = os.path.join(execFolder, "data", "gold.csv")

        with open(clientsInfo, "r") as f:
            dictReader = csv.DictReader(f)
            clientsData = list(dictReader)

        paramsFile = os.path.join(execFolder, "execParams.txt")
        with open(paramsFile, "r") as f:
            modelParams = json.loads(f.read())

        modelFile = os.path.join(execFolder, "model", "{}.pkl".format(modelParams["name"]))
        bgf = BetaGeoFitter()
        bgf.load_model(modelFile)
        output = []
        for entry in clientsData:

            # Melhorar para generalizar coluna de id e frequencia, recencia e T
            if entry["id"] == clientId:
                return {"id": entry[modelParams["idColumn"]], "predict": bgf.predict(4, 
                                                                                     int(entry[modelParams["frequencyColumn"]]), 
                                                                                     int(entry[modelParams["recencyColumn"]]), 
                                                                                     int(entry[modelParams["tColumn"]]))}
