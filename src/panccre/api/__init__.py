"""API application factory and ASGI app export."""

from panccre.api.server import app, create_app

__all__ = ["app", "create_app"]
