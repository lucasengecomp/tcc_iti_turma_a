import os
import glob

from flask import request, render_template, render_template_string, make_response
from flask_restx import Namespace, Resource, reqparse

ns = Namespace('docs',
               'Documentação do Código')


@ns.route('')
class docsTemplate(Resource):

    @ns.response(200, 'Documentação Renderizada')
    @ns.response(500, 'Internal Server error')
    def get(self):

        #templates = glob.glob(os.path.join(os.path.dirname(__file__), "static", "**", "*.html"))
        template = os.path.join(os.path.dirname(__file__), "static", "index.html")

        with open(template, "r") as f:
            return make_response(f.read(), 200)
