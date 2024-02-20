import datetime as dt
import pandas as pd
from prefect import flow, get_run_logger
from src.powercampus import FLOW_RDS, FLOW_RETRIES, table_fields
from src.powercampus.task import select, current_yearterm_df


@flow(retries=FLOW_RETRIES, retry_delay_seconds=FLOW_RDS)
def academic_table(begin_year: str) -> pd.DataFrame:
    """
    returns ACADEMIC table for year, term from PowerCampus
    """
    logger = get_run_logger()
    df = read_table('ACADEMIC', where=f"ACADEMIC_YEAR>='{begin_year}' ")
    logger.debug(f"academic_table: {df.shape=}")
    return df

@flow(retries=FLOW_RETRIES, retry_delay_seconds=FLOW_RDS)
def academiccalendar_table(year: str) -> pd.DataFrame:
    """
    returns ACADEMICCALENDAR table starting with year from PowerCampus
    """
    logger = get_run_logger()
    df = read_table('ACADEMICCALENDAR', where=f"ACADEMIC_YEAR>='{year}'")
    logger.debug(f"academiccalendar_table: {df.shape=}")
    return df

@flow(retries=FLOW_RETRIES, retry_delay_seconds=FLOW_RDS)
def address_table() -> pd.DataFrame:
    """
    returns ADDRESS table from PowerCampus
    """
    logger = get_run_logger()
    df = read_table('ADDRESS', )
    logger.debug(f"address_table: {df.shape=}")
    return df

@flow(retries=FLOW_RETRIES, retry_delay_seconds=FLOW_RDS)
def building_table() -> pd.DataFrame:
    """
    returns BUILDING table from PowerCampus
    """
    logger = get_run_logger()
    df = read_table('BUILDING', )
    logger.debug(f"building_table: {df.shape=}")
    return df

@flow(retries=FLOW_RETRIES, retry_delay_seconds=FLOW_RDS)
def code_day_table() -> pd.DataFrame:
    """
    returns CODE_DAY table from PowerCampus
    """
    logger = get_run_logger()
    df = read_table('CODE_DAY', )
    logger.debug(f"code_day_table: {df.shape=}")
    return df

@flow(retries=FLOW_RETRIES, retry_delay_seconds=FLOW_RDS)
def demographics_table(year: str, term: str) -> pd.DataFrame:
    """
    returns DEMOGRAPHICS table for year, term from PowerCampus
    """
    logger = get_run_logger()
    df = read_table('DEMOGRAPHICS', where=f"ACADEMIC_YEAR='{year}' AND ACADEMIC_TERM='{term}' AND ACADEMIC_SESSION=''")
    logger.debug(f"demographics_table: {df.shape=}")
    return df

@flow(retries=FLOW_RETRIES, retry_delay_seconds=FLOW_RDS)
def education_table() -> pd.DataFrame:
    """
    returns EDUCATION table from PowerCampus
    """
    logger = get_run_logger()
    df = read_table('EDUCATION', )
    logger.debug(f"education_table: {df.shape=}")
    return df

@flow(retries=FLOW_RETRIES, retry_delay_seconds=FLOW_RDS)
def emailaddress_table() -> pd.DataFrame:
    """
    returns EmailAddress table from PowerCampus
    """
    logger = get_run_logger()
    df = read_table('EmailAddress', )
    logger.debug(f"emailaddress_table: {df.shape=}")
    return df

@flow(retries=FLOW_RETRIES, retry_delay_seconds=FLOW_RDS)
def institution_table() -> pd.DataFrame:
    """
    returns INSTITUTION table from PowerCampus
    """
    logger = get_run_logger()
    df = read_table('INSTITUTION', )
    logger.debug(f"institution_table: {df.shape=}")
    return df

@flow(retries=FLOW_RETRIES, retry_delay_seconds=FLOW_RDS)
def organization_table() -> pd.DataFrame:
    """
    returns ORGANIZATION table from PowerCampus
    """
    logger = get_run_logger()
    df = read_table('ORGANIZATION', )
    logger.debug(f"organization_table: {df.shape=}")
    return df

@flow(retries=FLOW_RETRIES, retry_delay_seconds=FLOW_RDS)
def people_table() -> pd.DataFrame:
    """
    returns PEOPLE table from PowerCampus
    """
    logger = get_run_logger()
    df = read_table('PEOPLE', )
    logger.debug(f"people_table: {df.shape=}")
    return df

@flow(retries=FLOW_RETRIES, retry_delay_seconds=FLOW_RDS)
def residency_table(year: str, term: str) -> pd.DataFrame:
    """
    returns RESIDENCY table for year, term from PowerCampus
    """
    logger = get_run_logger()
    df = read_table('RESIDENCY', where=f"ACADEMIC_YEAR='{year}' AND ACADEMIC_TERM='{term}' AND ACADEMIC_SESSION=''")
    logger.debug(f"residency_table: {df.shape=}")
    return df

@flow(retries=FLOW_RETRIES, retry_delay_seconds=FLOW_RDS)
def sectionper_table(begin_year: str) -> pd.DataFrame:
    """
    returns SECTIONPER table for greater than or equal to begin_year from PowerCampus
    """
    logger = get_run_logger()
    df = read_table('SECTIONPER', where=f"ACADEMIC_YEAR >= '{begin_year}' AND ACADEMIC_TERM IN ('FALL', 'SPRING', 'SUMMER') AND ACADEMIC_SESSION IN ('MAIN', 'CULN', 'EXT', 'FNRR', 'HEOP', 'SLAB', 'BLOCK A', 'BLOCK AB', 'BLOCK B') ")
    logger.debug(f"sectionper_table: {df.shape=}")
    return df

@flow(retries=FLOW_RETRIES, retry_delay_seconds=FLOW_RDS)
def sections_table(begin_year: str) -> pd.DataFrame:
    """
    returns SECTIONS table for greater than or equal to begin_year from PowerCampus
    """
    logger = get_run_logger()
    df = read_table('SECTIONS', where=f"ACADEMIC_YEAR >= '{begin_year}' AND ACADEMIC_TERM IN ('FALL', 'SPRING', 'SUMMER') AND ACADEMIC_SESSION IN ('MAIN', 'CULN', 'EXT', 'FNRR', 'HEOP', 'SLAB', 'BLOCK A', 'BLOCK AB', 'BLOCK B') ")
    logger.debug(f"sections_table: {df.shape=}")
    # logger.debug(f"{df.columns=}")
    return df

@flow(retries=FLOW_RETRIES, retry_delay_seconds=FLOW_RDS)
def sectionschedule_table(begin_year: str) -> pd.DataFrame:
    """
    returns SECTIONSCHEDULE table for greater than or equal to begin_year from PowerCampus
    """
    logger = get_run_logger()
    df = read_table('SECTIONSCHEDULE', where=f"ACADEMIC_YEAR >= '{begin_year}' AND ACADEMIC_TERM IN ('FALL', 'SPRING', 'SUMMER') AND ACADEMIC_SESSION IN ('MAIN', 'CULN', 'EXT', 'FNRR', 'HEOP', 'SLAB', 'BLOCK A', 'BLOCK AB', 'BLOCK B') ")
    logger.debug(f"sectionschedule_table: {df.shape=}")
    # logger.debug(f"{df.columns=}")
    return df

@flow(retries=FLOW_RETRIES, retry_delay_seconds=FLOW_RDS)
def testscores_table() -> pd.DataFrame:
    """
    returns TESTSCORES table for Accuplacer math and English tests from PowerCampus
    """
    logger = get_run_logger()
    df = read_table('TESTSCORES', where=f"TEST_ID = 'ACC' AND ( TEST_TYPE = 'MATH' OR TEST_TYPE = 'ENGL' ) ", parse_dates=['TEST_DATE'])
    logger.debug(f"testscores_table: {df.shape=}")
    # logger.debug(f"{df.columns=}")
    return df

@flow(retries=FLOW_RETRIES, retry_delay_seconds=FLOW_RDS)
def transcriptdetail_table() -> pd.DataFrame:
    """
    returns TRANSCRIPTDETAIL table for greater than or equal to begin_year from PowerCampus
    """
    logger = get_run_logger()
    df = read_table('TRANSCRIPTDETAIL', where=f"CREDIT_TYPE = 'TRAN' ")
    logger.debug(f"transcriptdetail_table: {df.shape=}")
    # logger.debug(f"{df.columns=}")
    return df

@flow(retries=FLOW_RETRIES, retry_delay_seconds=FLOW_RDS)
def transcriptgpa_table(begin_year: str) -> pd.DataFrame:
    """
    returns TRANSCRIPTGPA table for greater than or equal to begin_year from PowerCampus
    """
    logger = get_run_logger()
    df = read_table('TRANSCRIPTGPA', where=f"ACADEMIC_YEAR >= '{begin_year}' AND ACADEMIC_TERM IN ('FALL', 'SPRING', 'SUMMER') ")
    logger.debug(f"transcriptgpa_table: {df.shape=}")
    # logger.debug(f"{df.columns=}")
    return df


# create active student list from 2-year rolling window
@flow(retries=FLOW_RETRIES, retry_delay_seconds=FLOW_RDS)
def active_students(n_years_active_window: int) -> pd.DataFrame:
    """
    returns DataFrame of active student IDs

    Active Students are those that have been enrolled in last two years.
    """

    today = dt.date.today()
    n_years_ago = today.year - n_years_active_window
    df = (
        select(
        'ACADEMIC', 
        ['PEOPLE_CODE_ID'], 
        where=f"ACADEMIC_YEAR > '{n_years_ago}' AND PRIMARY_FLAG = 'Y' AND CURRICULUM NOT IN ('ADVST') AND GRADUATED NOT IN ('G') ", 
        distinct=True 
        )
    )
    return df


# create user list of PEOPLE_CODE_ID's with college email_addresses
@flow(retries=FLOW_RETRIES, retry_delay_seconds=FLOW_RDS)
def with_email_address() -> pd.DataFrame:
    """
    returns DataFrame of PEOPLE_CODE_ID's with non-NULL college email_addresses
    """

    df = (
        select(
        'EmailAddress',
        ['PeopleOrgCodeId'],
        where="IsActive = 1 AND (EmailType='HOME' OR EmailType='MLBX') AND Email LIKE '%@%' ",
        distinct=True 
        ).rename(columns={"PeopleOrgCodeId": "PEOPLE_CODE_ID"})
    )
    return df


@flow(retries=FLOW_RETRIES, retry_delay_seconds=FLOW_RDS)
def apply_active(n_years_active_window: int, in_df: pd.DataFrame) -> pd.DataFrame:
    """
    returns copy of in_df with only records for active students

    in_df is an input DataFrame, must have PEOPLE_CODE_ID field
    """

    # return records for active students
    return pd.merge(in_df, active_students(n_years_active_window), how="inner", on="PEOPLE_CODE_ID")


@flow(retries=FLOW_RETRIES, retry_delay_seconds=FLOW_RDS)
def apply_active_with_email_address(n_years_active_window: int, in_df: pd.DataFrame) -> pd.DataFrame:
    """
    returns copy of in_df with only records for active students with email_address

    in_df is an input DataFrame, must have PEOPLE_CODE_ID field
    """

    # return records for active students with email_address
    return pd.merge(apply_active(n_years_active_window, in_df=in_df), with_email_address(), how="inner", on="PEOPLE_CODE_ID")


@flow(retries=FLOW_RETRIES, retry_delay_seconds=FLOW_RDS)
def read_table(name:str, where:str="", **kwargs1) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info(f"read_table({name=})")
    logger.debug(f"read_table({name=}, {table_fields[name]=}, {where=}, {kwargs1=}, {kwargs1.keys()=})")
    
    return select(name, table_fields[name], where, distinct=True, **kwargs1)


@flow(retries=FLOW_RETRIES, retry_delay_seconds=FLOW_RDS)
def current_yearterm() -> tuple[str, str, pd.Timestamp, pd.Timestamp, str, str]:
    df = current_yearterm_df()
    return (df['year'].iloc[0], df['term'].iloc[0], df['start_of_term'].iloc[0], 
        df['end_of_term'].iloc[0], df['yearterm_sort'].iloc[0], df['yearterm'].iloc[0])

