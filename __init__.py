"""iroh-connector backend plugin: exposes register(ctx) for discovery."""
from .connector import IrohConnector, register

__all__ = ["IrohConnector", "register"]
