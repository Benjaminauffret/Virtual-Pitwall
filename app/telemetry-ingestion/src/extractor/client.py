import fastf1
import pandas as pd
import logging
import argparse

from fastf1.core import Session


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

CACHE_DIR = 'cache'
fastf1.Cache.enable_cache(CACHE_DIR)


def parse_args() -> argparse.Namespace:
    """Configure et lit les arguments passés dans le terminal."""
    parser = argparse.ArgumentParser(description="Extracts telemetry data from Formula 1 session.")

    parser.add_argument("--year", type=int, required=True, help="The championship year")
    parser.add_argument("--race", type=int, required=True, help="The race number")
    parser.add_argument("--session_type", type=str, required=True, choices=['FP1', 'FP2', 'FP3', 'Q', 'SQ', 'S', 'R'], help="The session type")

    return parser.parse_args()


def get_session(year: int, race: int, session_type: str) -> Session:
    """
    Downloads and caches the data of a Formula 1 session.

    Args:
        year (int): The championship year (e.g., 2026).
        race (int): The race number in the season.
        session_type (str): The session type ('R' for Race, 'Q' for Qualifying).

    Returns:
        Session: The fastf1 Session object loaded with telemetry.
    """
    logging.info(f"Loading F1 session : {year} - Race n°{race} - Type '{session_type}'")
    session = fastf1.get_session(year, race, session_type)
    session.load()
    return session


def merge_data(cars: dict) -> pd.DataFrame:
    """
        Merges the telemetry of all cars and sorts them chronologically.

        Args:
            cars (dict): Dictionary containing telemetry data per driver.

        Returns:
            pd.DataFrame: A single DataFrame, sorted by date, ready for streaming.
    """
    logging.info("Merging telemetry data for all drivers...")
    all_telemetry = []

    for driver_id, car_data in cars.items():
        df = pd.DataFrame(car_data)
        df['Driver_id'] = driver_id
        all_telemetry.append(df)

    logging.info("Concatenating and sorting events chronologically...")
    df = pd.concat(all_telemetry, ignore_index=True)

    sorted_df = df.sort_values(by='Date')

    return sorted_df


def main() -> None:


    args = parse_args()

    try:
        race = get_session(args.year, args.race, args.session_type)
        cars_data = merge_data(race.car_data) # type: ignore
        weather_data = race.weather_data
        laps = race.laps
        pos_data = race.pos_data
        race_control_messages = race.race_control_messages
        session_status = race.session_status

        if not cars_data.empty:
            logging.info(f"Processing completed successfully: {len(cars_data)} telemetry rows ready.")

        if not weather_data.empty:
            logging.info(f"Processing completed successfully: {len(weather_data)} weather rows ready.")

        if not laps.empty:
            logging.info(f"Processing completed successfully: {len(laps)} laps rows ready.")

        if not pos_data.empty:
            logging.info(f"Processing completed successfully: {len(pos_data)} position data rows ready.")

        if not race_control_messages.empty:
            logging.info(f"Processing completed successfully: {len(race_control_messages)} race control messages rows ready.")

        if not session_status.empty:
            logging.info(f"Processing completed successfully: {len(session_status)} session status rows ready.")

    except Exception as e:
        logging.error(f"An error occurred during execution: {e}")


if __name__ == '__main__':
    main()
