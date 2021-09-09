from .cli import cli  # isort:skip

from permifrost.cli import permissions


def main():
    cli()


__all__ = ["permissions"]
