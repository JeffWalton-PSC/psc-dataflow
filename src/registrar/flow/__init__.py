import datetime as dt
import pandas as pd
from prefect import flow, get_run_logger
from src.powercampus.task import current_yearterm
from src.powercampus.flow import academic_table, address_table, demographics_table, emailaddress_table, people_table


BEGIN_YEAR = "2011"


@flow()
def daily_census_file():
    """
    Daily Census File subflow.
    """
    logger = get_run_logger()
    year, term, start_of_term, end_of_term, yearterm_sort, yearterm =  current_yearterm()
    logger.debug(f"{year=}, {term=}")

    df_academic = academic_table(year, term)
    df_people = people_table()
    df_address = address_table()
    df_email = emailaddress_table()
    df_demographics = demographics_table(year, term)

    print(df_academic.head())




@flow()
def registrar_flow():
    """
    Primary flow that runs all Registrar flows.
    """
    logger = get_run_logger()
    daily_census_file()



# if __name__ == "__main__":
#     registrar_flow()

