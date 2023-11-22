"""Entry point for the application."""

import logging

from flask import Flask
from flask_cors import CORS
from waitress import serve

import config
from apis import api

# from pyfairdatatools import __version__


def create_app(config_module=None):
    """Initialize the core application."""
    # create and configure the app
    app = Flask(__name__)
    # `full` if you want to see all the details
    app.config["SWAGGER_UI_DOC_EXPANSION"] = "none"
    app.config["RESTX_MASK_SWAGGER"] = False

    # set up logging
    logging.basicConfig(level=logging.DEBUG)

    # Initialize config
    app.config.from_object(config_module or "config")

    # todo: add csrf protection
    # csrf = CSRFProtect()
    # csrf.init_app(app)

    if config.FAIRHUB_ACCESS_TOKEN:
        if len(config.FAIRHUB_ACCESS_TOKEN) < 32:
            raise RuntimeError(
                "FAIRHUB_ACCESS_TOKEN must be at least 32 characters long"
            )
    else:
        raise RuntimeError("FAIRHUB_ACCESS_TOKEN not set")

    if not config.AZURE_STORAGE_ACCESS_KEY:
        raise RuntimeError("AZURE_STORAGE_ACCESS_KEY not set")

    if not config.AZURE_STORAGE_CONNECTION_STRING:
        raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING not set")

    api.init_app(app)

    # cors_origins = [
    #     "https://brave-ground-.*-.*.centralus.2.azurestaticapps.net",
    #     "https://staging.fairhub.io",
    #     "https://fairhub.io",
    # ]

    # if app.debug:
    #     cors_origins.append("http://localhost:3000")

    # allow all origins for now
    cors_origins = ["*"]

    CORS(
        app,
        resources={
            "/*": {
                "origins": cors_origins,
            }
        },
        allow_headers=[
            "Content-Type",
            "Authorization",
            "Access-Control-Allow-Origin",
            "Access-Control-Allow-Credentials",
        ],
        supports_credentials=True,
    )

    @app.before_request
    def on_before_request():  # pylint: disable = inconsistent-return-statements
        # white listed routes
        # public_routes = [
        #     "/auth",
        #     "/docs",
        #     "/echo",
        #     "/swaggerui",
        #     "/swagger.json",
        # ]

        # for route in public_routes:
        #     if request.path.startswith(route):
        #         return

        # # check if the query param is set and if it is equal to the access token
        # try:
        #     if request.args.get("access_token") == config.FAIRHUB_ACCESS_TOKEN:
        #         return
        #     else:
        #         raise RuntimeError("Invalid access token")
        # except Exception as e:  # pylint: disable=broad-except
        #     app.logger.error(e)
        #     return "Unauthorized", 401
        return

    return app


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument(
        "-p", "--port", default=5000, type=int, help="port to listen on"
    )
    args = parser.parse_args()
    port = args.port

    print(f"Starting server on port {port}")
    print(f"API Docs: http://localhost:{port}/docs")

    flask_app = create_app()

    # flask_app.run(host="0.0.0.0", port=port)
    serve(flask_app, port=port)
