import os
import glob
import json
import csv
import shutil
from http import HTTPStatus

from flask import abort, request
from flask_restx import Namespace, Resource, fields, reqparse, inputs

import appauth
import utils


ns = Namespace('api/data', 
               description='APIs para Submeter Dados de Treinamento')


datumFields = {"*": fields.Wildcard(fields.String())}
datumModel = ns.model('Datum', datumFields)


dataModel = ns.model('Data', {
    'data': fields.Nested(
        datumModel,
        description='Conjunto de Dados da Tabela',
        as_list=True
    )
})


dataWrapperModel = ns.model('DataWrapper', {
    'name': fields.String(
        description='Nome da Tabela'
    ),
    'data': fields.Nested(
        datumModel,
        description='Conjunto de Dados da Tabela',
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
class dataOps(Resource):

    @ns.doc(params={'token': {'description': 'Token de Autenticação', 'type': 'string', 'default': ''}})
    @ns.response(500, 'Erro Interno do Servidor')
    @ns.response(401, 'Token Inválido ou Não Fornecido')
    @ns.expect(dataWrapperModel)
    @ns.marshal_with(dataWrapperModel)
    def post(self):

        if not appauth.validateToken(**parser.parse_args()):
            ns.abort(401, "Token Invalido")
        
        registeredTables = glob.glob(os.path.join(os.path.dirname(__file__), "..", "..", "data", "*"))
        registeredTables = [os.path.basename(item) for item in registeredTables]

        if request.json['name'] in registeredTables:
            ns.abort(400, 'Tabela já registrada')

        tableFolder = os.path.join(os.path.dirname(__file__), "..", "..", "data", "{}".format(request.json['name']))
        os.mkdir(tableFolder)

        schemaFile = os.path.join(tableFolder, "columns.txt")
        columns = request.json['data'][0].keys()
        with open(schemaFile, "w") as f:
            f.write("\n".join(columns))

        dataFile = os.path.join(tableFolder, "data.csv")
        with open(dataFile, "w", encoding='utf8', newline='') as f:
            dictWriter = csv.DictWriter(f, columns)
            dictWriter.writeheader()
            dictWriter.writerows(request.json['data'])

        return {"message": "Tabela Criada"}


@ns.route('/<string:tableName>')
class datum(Resource):

    @ns.doc(params={'token': {'description': 'Token de Autenticação', 'type': 'string', 'default': ''}})
    @ns.response(500, 'Erro Interno do Servidor')
    @ns.response(404, 'Cliente Não Encontrado')
    @ns.response(401, 'Token Inválido ou Não Fornecido')
    @ns.marshal_with(dataModel)
    def get(self, tableName):
    
        if not appauth.validateToken(**parser.parse_args()):
            ns.abort(401, "Token Invalido")
        
        dataFile = os.path.join(os.path.dirname(__file__), "..", "..", "data", "{}.csv".format(tableName))

        with open(dataFile, "r") as f:
            dictReader = csv.DictReader(f)
            data = list(dictReader)    

        return {'data': data}


    @ns.doc(params={'token': {'description': 'Token de Autenticação', 'type': 'string', 'default': ''}})
    @ns.response(500, 'Erro Interno do Servidor')
    @ns.response(401, 'Token Inválido ou Não Fornecido')
    @ns.expect(dataModel)
    @ns.marshal_with(dataModel)
    def put(self, tableName):

        if not appauth.validateToken(**parser.parse_args()):
            ns.abort(401, "Token Inválido")

        registeredTables = glob.glob(os.path.join(os.path.dirname(__file__), "..", "..", "data", "*"))
        registeredTables = [os.path.basename(item) for item in registeredTables]

        if tableName not in registeredTables:
            ns.abort(400, 'Tabela Não Existe')

        tableFolder = os.path.join(os.path.dirname(__file__), "..", "..", "data", "{}".format(tableName))
        schemaFile = os.path.join(tableFolder, "columns.txt")
        dataFile = os.path.join(tableFolder, "data.csv")

        with open(schemaFile, "r") as f:
            columns = [item.strip() for item in f.readlines()]
            columns.sort()

        for entry in request.json['data']:

            entryCols = list(entry.keys())
            entryCols.sort()

            if columns != entryCols:
                ns.abort(400, 'Garanta que Todos os Registros Submetidos Contém as Colunas Necessárias Preenchidas')

        with open(dataFile, "a") as f:
            dictWriter = csv.DictWriter(f, columns)
            dictWriter.writerows(request.json['data'])

        return {"message": "Tabela Atualizada"}


    @ns.doc(params={'token': {'description': 'Token de Autenticação', 'type': 'string', 'default': ''}})
    @ns.response(500, 'Erro Interno do Servidor')
    @ns.response(404, 'Cliente Não Encontrado')
    @ns.response(401, 'Token Inválido ou Não Fornecido')
    @ns.response(204, 'Sucesso (Sem Conteudo)')
    def delete(self, tableName):

        if not appauth.validateToken(**parser.parse_args()):
            ns.abort(401, "Token Invalido")
        
        tableFolder = os.path.join(os.path.dirname(__file__), "..", "..", "data", "{}".format(tableName))

        if not os.path.isdir(tableFolder):
            ns.abort(400, 'Tablea Fornecida Não Existe')

        shutil.rmtree(tableFolder)

        return '', 204       
