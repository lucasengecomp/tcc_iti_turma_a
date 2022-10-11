import json

from flask_restx import fields

class ParametersParser(fields.Raw):
    def format(self, value):

        try:
            return json.loads(value)
        except:
            pass

        return value
