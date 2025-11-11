"""
lastfm_client.py
High-level client for orchestrating Last.fm geo data pulls.
Uses pull_geo.py utilities to fetch top artists and tracks by country.
Forms the ingestion (Extract) step of the ETL pipeline.
"""
import time
import typer
from datetime import datetime
from src.config import DATA_DIR, COUNTRIES
from src.lastfm_fetch import main as pull_geo_main

app = typer.Typer()

class LastfmClient:
    """
    A modular client for fetching Last.fm top artists and tracks
    for multiple countries.
    """
    def __init__(
            self,
            countries: list = COUNTRIES,
            limit: int = 50,
            delay: float = 1.5,
    ):
        self.countries = countries
        self.limit = limit
        self.delay = delay

        if not self.countries:
            raise ValueError("No countries provided for data fetching.")

        typer.echo(f"Loaded {len(self.countries)} countries for data fetching.")

    def run(self):
        """
        Run ingestion for all countries (artists + tracks).
        """
        start_time = datetime.now()
        print(f"Starting Last.fm data ingestion at {start_time:%Y-%m-%d %H:%M:%S}...")

        for country in self.countries:
            for chart_type in ["artists", "tracks"]:
                try:
                    typer.echo(f"Fetching top {chart_type} for {country}...")
                    pull_geo_main(
                        country=country,
                        limit=self.limit,
                        chart_type=chart_type,
                        page=1
                    )
                    time.sleep(self.delay)
                except Exception as e:
                    typer.echo(f"Error fetching {chart_type} for {country}: {e}")

        end_time = datetime.now()
        print(f"Finished ingestion at {end_time:%Y-%m-%d %H:%M:%S}.")
        print(f"Data saved under {DATA_DIR}/artists and {DATA_DIR}/tracks.")

# CLI entry point
def main(
        limit: int = typer.Option(
            50,
            "--limit",
            "-l",
            help="Limit the number of top artists to fetch per country."
        ),

        delay: float = typer.Option(
            1.5,
            "--delay",
            "-d",
            help="Delay in seconds between API requests to avoid rate limiting."
        ),
):
    client = LastfmClient(
        limit=limit,
        delay=delay
    )
    client.run()

if __name__ == "__main__":
    typer.run(main)














