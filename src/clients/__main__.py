import typer
from .lastfm_client import main as lastfm_main

if __name__ == "__main__":
    typer.run(lastfm_main)