import datetime as dt
import pandas as pd
from datetime import timedelta
from prefect import task, get_run_logger
from prefect.tasks import task_input_hash
from src.powercampus import TASK_CEM, TASK_RETRIES, TASK_RDS
import local_db


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RDS,
    cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=TASK_CEM),
    )
def select(table:str, fields:list=None, where:str="", distinct=False, **kwargs) -> pd.DataFrame:
    """
    Function pulls data from PowerCampus database.

    Returns a pandas DataFrame.

    Example Usage:
    select("ACADEMIC", 
          fields=['PEOPLE_CODE_ID', 'ACADEMIC_YEAR', 'ACADEMIC_TERM'], 
          where="ACADEMIC_YEAR='2021' and ACADEMIC_TERM='FALL' and CREDITS>0", 
          distinct=True)

    """
    
    connection = local_db.connection()

    if fields is None:
        fields = "*"
    else:
        fields = ", ".join(fields)

    if where != "":
        where = "WHERE " + where

    if distinct:
        distinct = "DISTINCT "
    else:
        distinct = ""
    
    parsedates = None
    if kwargs:
        if 'parse_dates' in kwargs.keys():
            parsedates = kwargs['parse_dates']

    sql_str = (
        f"SELECT {distinct}{fields} "
        + f"FROM {table} "
        + where
    )
    # print(sql_str)
    return ( pd.read_sql_query(sql_str, connection, parse_dates=parsedates)
    )


# find the latest year_term
@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RDS,
    cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=TASK_CEM),
    )
def latest_year_term(df0: pd.DataFrame) -> pd.DataFrame:
    """
    Return df with most recent records based on ACADEMIC_YEAR and ACADEMIC_TERM
    """
    logger = get_run_logger()
    logger.info(f"latest_year_term({df0.shape=})")
    df = df0.copy()
    df = df[(df["ACADEMIC_YEAR"].notnull()) & (df["ACADEMIC_YEAR"].str.isnumeric())]
    df["ACADEMIC_YEAR"] = pd.to_numeric(df["ACADEMIC_YEAR"], errors="coerce")
    df_seq = pd.DataFrame(
        [
            {"term": "Transfer", "seq": 0},
            {"term": "SPRING", "seq": 1},
            {"term": "SUMMER", "seq": 2},
            {"term": "FALL", "seq": 3},
        ]
    )
    df = pd.merge(df, df_seq, left_on="ACADEMIC_TERM", right_on="term", how="left")
    df["term_seq"] = df["ACADEMIC_YEAR"] * 100 + df["seq"]

    #d = df.reset_index().groupby(["PEOPLE_CODE_ID"])["term_seq"].idxmax()
    df = df.loc[df.reset_index().groupby(["PEOPLE_CODE_ID"])["term_seq"].idxmax()]
    logger.info(f"latest_year_term() = {df.shape=}")

    return df


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RDS,
    cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=TASK_CEM),
    )
def add_col_yearterm(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds 'yearterm' column to dataframe df.
    """
    
    if ('ACADEMIC_YEAR' in df.columns) and ('ACADEMIC_TERM' in df.columns):
        df['yearterm'] = df['ACADEMIC_YEAR'] + '.' +  df['ACADEMIC_TERM'].str.title()
    else:
        # print("ERROR: columns not found ['ACADEMIC_YEAR', 'ACADEMIC_TERM']")
        raise KeyError("columns not found ['ACADEMIC_YEAR', 'ACADEMIC_TERM']")
    
    return df


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RDS,
    cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=TASK_CEM),
    )
def add_col_yearterm_sort(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds 'yearterm_sort' column to dataframe df.
    """
    
    yearterm_sort = ( lambda r:
        r['ACADEMIC_YEAR'] + '01' if r['ACADEMIC_TERM']=='SPRING' else
        (r['ACADEMIC_YEAR'] + '02' if r['ACADEMIC_TERM']=='SUMMER' else
        (r['ACADEMIC_YEAR'] + '03' if r['ACADEMIC_TERM']=='FALL' else
        r['ACADEMIC_YEAR'] + '00'))
    )
    
    if ('ACADEMIC_YEAR' in df.columns) and ('ACADEMIC_TERM' in df.columns):
        df['yearterm_sort'] = df.apply(yearterm_sort, axis=1)

    else:
        # print("ERROR: columns not found ['ACADEMIC_YEAR', 'ACADEMIC_TERM']")
        raise KeyError("columns not found ['ACADEMIC_YEAR', 'ACADEMIC_TERM']")
    
    return df