import pandas as pd
# import powercampus as pc
# import sys
# import time
# from datetime import timedelta
from prefect import flow, get_run_logger
from src.powercampus.task import read_table


@flow(retries=3, retry_delay_seconds=10)
def academic_table(year: str, term: str):
    """
    returns ACADEMIC table for year, term from PowerCampus
    """
    logger = get_run_logger()
    df = read_table('ACADEMIC', where=f"ACADEMIC_YEAR='{year}' AND ACADEMIC_TERM='{term}'")
    logger.debug(f"{df.shape=}")
    return df

@flow(retries=3, retry_delay_seconds=10)
def address_table():
    """
    returns ADDRESS table from PowerCampus
    """
    logger = get_run_logger()
    df = read_table('ADDRESS', )
    logger.debug(f"{df.shape=}")
    return df

@flow(retries=3, retry_delay_seconds=10)
def demographics_table(year: str, term: str):
    """
    returns DEMOGRAPHICS table for year, term from PowerCampus
    """
    logger = get_run_logger()
    df = read_table('DEMOGRAPHICS', where=f"ACADEMIC_YEAR='{year}' AND ACADEMIC_TERM='{term}' AND ACADEMIC_SESSION=''")
    logger.debug(f"{df.shape=}")
    return df

@flow(retries=3, retry_delay_seconds=10)
def emailaddress_table():
    """
    returns EmailAddress table from PowerCampus
    """
    logger = get_run_logger()
    df = read_table('EmailAddress', )
    logger.debug(f"{df.shape=}")
    return df

@flow(retries=3, retry_delay_seconds=10)
def people_table():
    """
    returns PEOPLE table from PowerCampus
    """
    logger = get_run_logger()
    df = read_table('PEOPLE', )
    logger.debug(f"{df.shape=}")
    return df

@flow(retries=3, retry_delay_seconds=10)
def residency_table(year: str, term: str):
    """
    returns RESIDENCY table for year, term from PowerCampus
    """
    logger = get_run_logger()
    df = read_table('RESIDENCY', where=f"ACADEMIC_YEAR='{year}' AND ACADEMIC_TERM='{term}'")
    logger.debug(f"{df.shape=}")
    return df

