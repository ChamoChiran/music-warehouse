"""
Transform Last.fm artist data into parquet format.
This script reads raw country-level artist chart data from data/raw/geo/tracks/,
cleans and flattens the data, and writes the transformed data to data/silver/geo/tracks/
"""

import json
import pandas as pd
import typer
from pathlib import Path
from datetime import datetime
from src.config import TRACKS_JSON_PATH, OUTPUT_DIR
from src.transform_data.transform_artists import transform_artist_data_country

app = typer.Typer()

def transform_track_data_country(
        file_path: Path
):

    """
    Process and transform raw track JSON data into parquet format.
    :param file_path:
    :return:
    """

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Extract country and tracks list
    tracks = data['tracks']['track']
    df = pd.json_normalize(tracks)

    # rename and select relevant columns
    df.rename(
        columns={
            "@attr.rank": "rank",
            "name" : "track_name",
            "duration" : "track_duration",
            "listeners" : "track_listeners",
            "mbid" : "track_mbid",
            "url" : "track_url",
            "artist.name" : "artist_name",
            "artist.mbid" : "artist_mbid",
            "artist.url" : "artist_url",
        },
        inplace=True
    )

    df = df.drop('image', axis=1)

    # infer metadata from file
    parts = file_path.stem.split("_")
    country = "_".join(parts[:-2])
    date_str = parts[-2]

    try:
        chart_date = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        typer.echo(f"Invalid date format: {date_str}")
        chart_date = datetime.today().date()

    # Add metadata columns
    df['chart_country'] = country
    df['chart_date'] = chart_date
    df['load_time'] = datetime.now()

    # fix column types
    df['rank'] = df['rank'].astype(int)
    df['track_listeners'] = df['track_listeners'].astype(int)

    return df

def transform_json_data(
        json_path: Path,
        output_dir: Path
):
    if not json_path.exists():
        typer.echo(f"File not found: {json_path}")
        raise typer.Exit(1)

    json_files = sorted(json_path.glob("*.json"))
    if not json_files:
        typer.echo(f"No .json files found: {json_path}")
        raise typer.Exit(1)

    typer.echo(f"Found {len(json_files)} .json files")

    all_dfs = []
    for file in json_files:
        typer.echo(f"Processing {file}")
        try:
            df = transform_track_data_country(file)
            all_dfs.append(df)
        except Exception as e:
            typer.secho(f"Error processing {file.name}: {e}", fg=typer.colors.RED)
        else:
            typer.echo(f"Finished processing {file.name}")

    if not all_dfs:
        typer.secho(f"No data found: {json_path}", fg=typer.colors.RED)
        raise typer.Exit(1)

    # combine all dataframes
    combined_df = pd.concat(all_dfs)
    typer.echo("Combined dataframe shape: {combined_df.shape}")

    output_dir = Path(output_dir / 'silver' / 'geo' / 'tracks')
    output_dir.mkdir(parents=True, exist_ok=True)

    # save as parquet
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"tracks_{timestamp}.parquet"
    combined_df.to_parquet(output_file, index=False)

    typer.secho(f"Saved combined parquet → {output_file}", fg=typer.colors.BRIGHT_GREEN)
    typer.echo("All artist JSONs combined successfully.")


# CLI entry point
def main(
        json_path: Path = typer.Option(
            TRACKS_JSON_PATH,
            "--json-path",
            "-j",
            help="Path to raw track JSON files",
        ),
        output_dir: Path = typer.Option(
            OUTPUT_DIR,
            "--output-dir",
            "-o",
            help="Output directory for transformed parquet files",
        ),
):
    transform_json_data(json_path, output_dir)

if __name__ == "__main__":
    app.command()(main)
    app()