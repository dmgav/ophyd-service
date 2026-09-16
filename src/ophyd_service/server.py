import logging
import os
from pathlib import Path

from .app import build_app
from .config import construct_build_app_kwargs, parse_configs

logger = logging.getLogger(__name__)


def app_factory():
    """
    Return an ASGI app instance.

    Use a configuration file at the path specified by the environment variable
    OPHYD_SERVICE_CONFIG. If the env. variable is not set, then do not load
    configuration.

    This is intended to be used for horizontal deployment (using gunicorn, for
    example) where only a module and instance or factory can be specified.
    """
    logging.basicConfig(level=logging.WARNING)
    logging.getLogger("ophyd_service").setLevel("INFO")

    config_path = os.getenv("OPHYD_SERVICE_CONFIG", None)

    try:
        parsed_config = parse_configs(config_path) if config_path else {}
    except Exception as ex:
        logger.error(ex)
        raise

    # This config was already validated when it was parsed. Do not re-validate.
    kwargs = construct_build_app_kwargs(parsed_config, source_filepath=config_path)
    if config_path:
        logger.info(f"Using configuration from {Path(config_path).absolute()}")
    else:
        logger.info("No configuration file was specified. Using environment variables.")

    web_app = build_app(**kwargs)
    # uvicorn_config = parsed_config.get("uvicorn", {})
    # print_admin_api_key_if_generated(web_app, host=uvicorn_config.get("host"), port=uvicorn_config.get("port"))

    return web_app


app = app_factory()
