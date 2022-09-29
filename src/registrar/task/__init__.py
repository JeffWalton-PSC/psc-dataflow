import pandas as pd
import powercampus as pc
# import sys
import time
from datetime import timedelta
from prefect import task, get_run_logger
from prefect.tasks import task_input_hash


@task(retries=3, retry_delay_seconds=10,
    cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=4),
    )
def read_academic_calendar_task(begin_year: str):
    logger = get_run_logger()
    logger.info(f"read_academic_calendar_task({begin_year=})")
    time.sleep(0.5)
    df = pd.read_csv('data/academic_calendar.csv')
    logger.debug(f"ACADEMIC_CALENDAR:{df.shape=}")
    return df