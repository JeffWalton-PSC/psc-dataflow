import shutil
from prefect import task, get_run_logger
from src.starfish import starfish_prod_sisdatafiles_path, starfish_test_sisdatafiles_path


@task()
def copy_to_starfish_sisdatafiles(file_to_copy:str):
    """
    Copy files to Starfish sisdatafiles directories.
    """
    logger = get_run_logger()
    logger.info(f"copy_to_starfish_sisdatafiles({file_to_copy=})")

    try:
        shutil.copy(file_to_copy, starfish_prod_sisdatafiles_path)
        logger.info(f"{file_to_copy} written to {starfish_prod_sisdatafiles_path}.")
    except Exception as err:
        logger.error(f"Error writing file to 'prod': {file_to_copy} [{err}]")

    try:
        shutil.copy(file_to_copy, starfish_test_sisdatafiles_path)
        logger.info(f"{file_to_copy} written to {starfish_test_sisdatafiles_path}.")
    except Exception as err:
        logger.error(f"Error writing file to 'test': {file_to_copy} [{err}]")

    logger.info(f"End: copy_to_starfish_sisdatafiles()")
