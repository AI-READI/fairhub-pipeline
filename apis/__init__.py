"""Initialize the api system for the backend"""
from flask_restx import Api

from apis.stage_one_namespace import api as stage_one_namespace
from apis.root import api as root_namespace

from .stage_one.stage_one_test import api as stage_one_test

api = Api(
    title="fairhub pipeline api",
    description="Trigger data pipeline jobs",
    doc="/docs",
)

api.add_namespace(root_namespace)
api.add_namespace(stage_one_namespace)
