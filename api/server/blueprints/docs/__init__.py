import os
import glob

from flask import request, redirect, make_response
from flask_restx import Namespace, Resource, reqparse

ns = Namespace('docs',
               'Documentação do Código')


@ns.route('')
class docsHome(Resource):

    @ns.response(200, 'Documentação Renderizada')
    @ns.response(500, 'Internal Server error')
    def get(self):

        template = os.path.join(os.path.dirname(__file__), "static", "index.html")

        with open(template, "r") as f:
            return make_response(f.read(), 200)


@ns.route('/<path:basePath>/project/<path:page>')
class docsLongLinkFixer(Resource):
    
    @ns.response(200, 'Documentação Renderizada')
    @ns.response(500, 'Internal Server error')
    def get(self, basePath, page):

        return redirect("/docs/{}".format(page))


@ns.route('/project/<path:page>')
class docsLinkFixer(Resource):

    @ns.response(200, 'Documentação Renderizada')
    @ns.response(500, 'Internal Server error')
    def get(self, page):

        return redirect("/docs/{}".format(page))

@ns.route('/<path:page>')
class docsTemplate(Resource):

    @ns.response(200, 'Documentação Renderizada')
    @ns.response(500, 'Internal Server error')
    def get(self, page):

        print("2")
        print(page)

        pages = page.split("/")

        template = os.path.join(os.path.dirname(__file__), "static", *pages)

        with open(template, "r") as f:
            return make_response(f.read(), 200)
