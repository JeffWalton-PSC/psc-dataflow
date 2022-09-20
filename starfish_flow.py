import sys
import time
from prefect import flow, get_run_logger
from starfish_task import read_academic_calendar_task, read_sections_table_task, read_people_table_task


BEGIN_YEAR = "2011"


@flow()
def academic_calendar_flow(begin_year: str):
    logger = get_run_logger()
    logger.info(f"academic_calendar_flow({begin_year=})")
    read_academic_calendar_task(begin_year)
    time.sleep(0.5)
    logger.info(f"academic_calendar.txt written.")

@flow()
def users_flow(begin_year: str):
    logger = get_run_logger()
    logger.info(f"read_people_table_task({begin_year=})")
    read_people_table_task(begin_year)
    time.sleep(0.5)
    logger.info(f"users.txt written.")

@flow()
def courses_flow(begin_year: str):
    logger = get_run_logger()
    logger.info(f"read_sections_table_task({begin_year=})")
    read_sections_table_task(begin_year)
    time.sleep(0.5)
    read_people_table_task(begin_year)
    time.sleep(0.5)
    logger.info(f"courses.txt written.")


@flow()
def starfish_flow(academic_year: str, academic_term: str):
    logger = get_run_logger()
    logger.info(f"starfish_flow({academic_year=}, {academic_term=})")
    academic_calendar_flow(BEGIN_YEAR)
    time.sleep(0.5)
    users_flow(BEGIN_YEAR)
    time.sleep(0.5)
    courses_flow(BEGIN_YEAR)
    logger.info(f"starfish_flow completed.")



if __name__ == "__main__":
    academic_year = sys.argv[1]
    academic_term = sys.argv[2]
    starfish_flow(academic_year, academic_term)

