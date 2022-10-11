from flask import Flask, Blueprint, request
from flask_restx import Api
from server.blueprints.clients import ns as clientsNS
from server.blueprints.pipelines import ns as pipelinesNS
from server.blueprints.data import ns as dataNS
from server.blueprints.trainings import ns as trainingNS
from server.blueprints.docs import ns as docsNS

app = Flask(__name__)
api = Api(version='1.0', 
          title='Smart Sales API',
          description='API para pegar informações sobre clientes')


def initialize_app(app):

    app.config["RESTPLUS_VALIDATE"] = True
    app.config["ERROR_404_HELP"] = False

    api = Api(version='1.0',
              title='Smart Sales API',
              description='API para pegar informações sobre clientes')

    blueprint = Blueprint('base', __name__)
    api.init_app(blueprint)
    app.register_blueprint(blueprint)

    api.add_namespace(clientsNS)
    api.add_namespace(pipelinesNS)
    api.add_namespace(dataNS)
    api.add_namespace(trainingNS)
    api.add_namespace(docsNS)


if __name__ == '__main__':

    initialize_app(app)    
    app.run(debug=True, port=8000, host='0.0.0.0')
