"""Root level API endpoints"""
from flask_restx import Namespace, Resource

api = Namespace("/", description="Root level operations", path="/")


@api.route("/hello")
class Hello(Resource):
    """Root level API endpoints"""

    def get(self):
        """Say hello"""
        return "Welcome to the pipeline!", 200
