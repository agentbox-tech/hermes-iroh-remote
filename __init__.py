"""iroh-remote plugin backend plugin: exposes register(ctx) for discovery."""
from .remote_plugin import IrohConnector, register

__all__ = ["IrohConnector", "register"]
