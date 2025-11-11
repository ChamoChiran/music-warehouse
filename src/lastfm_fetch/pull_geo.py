"""
pull_geo.py
Fetches country-level music data (top artists/tracks) from the Last.fm API.

Usage:
    python pull_geo.py --country "United States" --type "artists" --limit
"""

import os
import json
import requests
import typer
from datetime import datetime
from src.config import API_KEY, BASE_URL, DATA_DIR


app = typer.Typer()

def fetch_geo_data(
        country: str,
        chart_type: str,
        limit: int,
        page: int
):
    """
    Fetches country-level music data (top artists/tracks) from the Last.fm API.

    Args:
        country (str): The country to fetch data for.
        chart_type (str): The type of chart to fetch ('artists' or 'tracks').
        limit (int): Number of results to return per page.
        page (int): Page number to fetch.
    """
    method = f"geo.gettop{chart_type}"
    params = {
        "method": method,
        "country": country,
        "api_key": API_KEY,
        "format": "json",
        "limit": limit,
        "page": page,
    }

    response = requests.get(BASE_URL, params=params, timeout=100)
    response.raise_for_status()
    return response.json()

def save_response(
        data: dict,
        country: str,
        chart_type:str
):
    # Create sub folder for artists or tracks
    folder = DATA_DIR / chart_type.lower()
    folder.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    country_slug = country.lower().replace(" ", "_")

    file_path = folder / f"{country_slug}_{timestamp}.json"

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    typer.echo(f"Saved {chart_type} data for {country} → {file_path}")

# CLI entry point
def main(
        country: str = typer.Option(
            ...,
            "--country",
            "-c",
            help="Country to fetch data for"
        ),

        chart_type: str = typer.Option(
            ...,
            "--type",
            "-t",
            help="Type of chart to fetch ('artists' or 'tracks')"
        ),

        limit: int = typer.Option(
            50,
            "--limit",
            "-l",
            help="Number of results to return per page"
        ),

        page: int = typer.Option(
            1,
            "--page",
            "-p",
            help="Page number to fetch"
        ),
):
    if API_KEY:
        data = fetch_geo_data(country, chart_type, limit, page)
        save_response(data, country, chart_type)
    else:
        typer.secho("Error: LASTFM_API_KEY is not set in environment variables.", fg=typer.colors.RED)


if __name__ == "__main__":
    typer.run(main)