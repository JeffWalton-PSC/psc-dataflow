import pandas as pd
from pathlib import WindowsPath
from prefect import flow, task, get_run_logger
from src.dataframe.task import write_csv_textfile



@task()
def combine_exporter_files():
    """
    Combines daily difference files from Starfish Exporter into one file.
    """

    logger = get_run_logger()
    logger.info(f"Start: combine_exporter_files()")

    exporter_path = WindowsPath('F:\Applications\Starfish\Files\exporter')
    output_path = WindowsPath('F:\Data\exporter_data')

    files = exporter_path.glob('*.csv')

    file_types = set()
    for f in files:
        ft = f.stem.split('-')[0]
        file_types.add(ft)

    for ft in file_types:
        files = exporter_path.glob(ft + '*.csv')
        combined_df = pd.concat( [ pd.read_csv(f) for f in files ] )
        combined_df = combined_df.drop_duplicates()
        outfile = output_path / (ft + '.csv')
        write_csv_textfile( combined_df, outfile)
        logger.info(f"Exporter {ft} file written: {outfile}")

    logger.info(f"End: combine_exporter_files()")


@flow()
def exporter_flow():
    """
    Processes Exporter files after they are retreived by the Starfish Exporter data pump.
    """

    logger = get_run_logger()
    logger.info(f"Start: exporter_flow()")

    combine_exporter_files()

    logger.info(f"End: exporter_flow()")


if __name__ == "__main__":
    exporter_flow()



