"""
Transform Last.fm artist data into parquet format.
This script reads raw country-level artist chart data from data/raw/geo/artists/,
cleans and flattens the data, and writes the transformed data to data/silver/geo/artists/
"""

import json
import pandas as pd
import typer
from pathlib import Path
from datetime import datetime
from src.config import ARTIST_JSON_PATH, OUTPUT_DIR

app = typer.Typer()

def transform_artist_data_country(
        json_path: Path
):
    """
    Process and transform raw artist JSON data into parquet format.
    :param json_path:
    :param output_dir:
    :return:
    """

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Extract country and artists list
    artists = data['topartists']['artist']
    df = pd.json_normalize(artists)

    # rename and select relevant columns
    df.rename(
        columns={
            "@attr.rank": "rank",
            "name": "artist_name",
            "mbid": "artist_mbid",
            "listeners": "artist_listeners",
            "url": "artist_url",
        },
        inplace=True
    )

    df = df.drop('image', axis=1)

    # infer metadata from file
    parts = json_path.stem.split("_")
    country = "_".join(parts[:-2]) # include all from start don't include last 2
    date_str = parts[-2]

    try:
        chart_date = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        typer.echo(f"Invalid date {date_str}")
        chart_date = datetime.today().date()

    # Add metadata columns
    df['chart_country'] = country
    df['chart_date'] = chart_date
    df['load_time'] = datetime.now()

    # Fix column types
    df['rank'] = df['rank'].astype(int)
    df['artist_listeners'] = df['artist_listeners'].astype(int)

    return df

def transform_json_data(
        json_path: Path,
        output_dir: Path
):
    if not json_path.exists():
        typer.echo(f"File {json_path} does not exist")
        raise typer.Exit(1)

    json_files = sorted(json_path.glob("*.json"))
    if not json_files:
        typer.secho("No JSON files found in the specified directory.", fg=typer.colors.RED)
        raise typer.Exit(1)

    typer.echo(f"Found {len(json_files)} JSON files")

    all_dfs = []
    for file in json_files:
        typer.echo(f"Processing {file.name}...")
        try:
            df = transform_artist_data_country(file)
            all_dfs.append(df)
        except Exception as e:
            typer.secho(f"Error processing {file.name}: {e}", fg=typer.colors.RED)
        else:
            typer.secho(f"Successfully processed {file.name}", fg=typer.colors.GREEN)

    if not all_dfs:
        typer.secho("No dataframes were created from the JSON files.", fg=typer.colors.RED)
        raise typer.Exit(1)

    # Combine all dataframes
    combined_df = pd.concat(all_dfs)
    typer.echo(f"Combined dataframe shape: {combined_df.shape}")

    output_dir.mkdir(parents=True, exist_ok=True)

    # Save as parquet
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / 'silver' / 'geo' / 'artists' / f"artists_{timestamp}.parquet"
    combined_df.to_parquet(output_file, index=False)

    typer.secho(f"Saved combined parquet → {output_file}", fg=typer.colors.BRIGHT_GREEN)
    typer.echo("All artist JSONs combined successfully.")


# CLI entry point
def main(
        json_path: Path = typer.Option(
            ARTIST_JSON_PATH,
            "--json-path",
            "-j",
            help="Path to the directory containing raw artist JSON files"
        ),
        output_dir: Path = typer.Option(
            OUTPUT_DIR,
            "--output-dir",
            "-o",
            help="Directory to save the transformed parquet files"
        )
):
    transform_json_data(json_path, output_dir)

if __name__ == '__main__':
    app.command()(main)
    app()




































