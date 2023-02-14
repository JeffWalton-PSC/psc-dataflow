import os
import datetime as dt
import pandas as pd
from prefect import flow, get_run_logger

from src.powercampus.task import current_yearterm
from src.canvas import canvas_path

from canvas_data.api import CanvasDataAPI


@flow()
def canvas_data_download():
    """
    Downloads and processes Canvas Data files.
    """

    logger = get_run_logger()
    logger.info(f"canvas_data_download()")

    data_path = canvas_path / "data"
    downloads_path = data_path / "downloads"
    processed_path = data_path / "processed_data"

    logger.info(f"Start: Canvas Data - All tables")

    try:
        API_KEY = os.environ.get('CANVAS_DATA_API_KEY')
        API_SECRET = os.environ.get('CANVAS_DATA_API_SECRET')
    except KeyError:
        logger.warning("Set CANVAS_DATA_API_KEY and CANVAS_DATA_API_SECRET environment variables.")
        exit()

    cd = CanvasDataAPI(api_key=API_KEY, api_secret=API_SECRET)
    schema = cd.get_schema('latest', key_on_tablenames=True)

    def table_columns(table: str):
        col_names = []
        col_dtypes = {}
        col_datetimes = []
        for i in schema[table]['columns']:
            logger.debug(f"col: {i['name']}, type: {i['type']}")
            col_names.append(i['name'])
            if (i['type'] in ['varchar', 'guid', 'text', 'bigint']):
                col_dtypes[i['name']] = 'string'
            elif (i['type'] in ['boolean']):
                col_dtypes[i['name']] = 'boolean'
            elif (i['type'] in ['timestamp', 'datetime']):
                col_datetimes.append(i['name'])
            elif i['type'] in ['int', 'bigint']:
                col_dtypes[i['name']] = 'Int64'
            else:
                logger.debug( f"Add type '{i['type']}' to table_columns() function for column '{i['name']}'.")
        return col_names, col_dtypes, col_datetimes

    table_list = ['user_dim', 'pseudonym_dim', 'role_dim', 'enrollment_term_dim', 'enrollment_dim', 'course_dim', 'course_section_dim']

    df = {}
    for t in table_list:
        fn_tsv = cd.get_data_for_table(table_name=t, 
                                    dump_id='latest', 
                                    data_directory=data_path,
                                    download_directory=downloads_path,
                                    force=True)
        logger.debug(f"{t}: {fn_tsv}")

        col_names, col_dtypes, col_datetimes = table_columns(t)
        
        df[t] = pd.read_csv(fn_tsv, 
                            sep='\t', 
                            header=None, 
                            names=col_names, 
                            dtype=col_dtypes, 
                            parse_dates=col_datetimes, 
                            na_values=[r'\N']
                        )
        logger.debug(f"df['{t}']: {df[t].shape}")

    table_fields = {
        'user_dim': ['id', 'canvas_id', 'global_canvas_id', 'name', 'sortable_name'],
        'pseudonym_dim': ['id', 'canvas_id', 'user_id', 'sis_user_id', 'unique_name', 'last_request_at', 'last_login_at', 'current_login_at', ],
        'role_dim': ['id', 'canvas_id', 'name', ],
        'enrollment_term_dim': ['id', 'canvas_id', 'name', 'date_start', 'date_end', 'sis_source_id'],
        'course_dim': ['id', 'canvas_id', 'account_id', 'enrollment_term_id', 'name', 'code', 'sis_source_id'],
        'enrollment_dim': ['id', 'canvas_id', 'course_section_id', 'role_id', 'type', 'workflow_state', 'course_id', 'user_id', 'last_activity_at'],
        'course_section_dim': ['id', 'canvas_id', 'course_id', 'enrollment_term_id', 'name', 'sis_source_id'],
    }

    for t in table_list:
        df_t = df[t].loc[:,table_fields[t]]
        ftr_path = processed_path / f"{t}.ftr"
        if df_t.empty:
            logger.debug(f"{t}:{df_t.shape} is empty.")
        else:
            df_t.reset_index(drop=True).to_feather(ftr_path)
            logger.debug(f"{t}:{df_t.shape} data written to {ftr_path}")

    logger.info(f"End: Canvas Data - All tables")


@flow()
def canvas_last_student_activity():
    """
    Combines Canvas Data files to produce daily file of last Canvas activity, canvas_activity_{today}.csv file.
    """

    begin_datetime = '1970-01-01 00:00:00.0'
    today = dt.datetime.now().strftime("%Y%m%d")
    year, term, _, _, _, yearterm = current_yearterm()

    logger = get_run_logger()
    logger.info(f"canvas_last_student_activity()")

    data_path = canvas_path / "data"
    # downloads_path = data_path / "downloads"
    processed_path = data_path / "processed_data"
    output_path = data_path / "activity"

    logger.info(f"Start: Canvas - Student Last Activity")

    df_user = pd.read_feather(processed_path / "user_dim.ftr")
    logger.debug(f"df_user: {df_user.shape=}")

    df_pseudonym = pd.read_feather(processed_path / "pseudonym_dim.ftr")
    logger.debug(f"df_pseudonym: {df_pseudonym.shape=}")

    df_terms = pd.read_feather(processed_path / "enrollment_term_dim.ftr")
    logger.debug(f"df_terms: {df_terms.shape=}")

    df_course = pd.read_feather(processed_path / "course_dim.ftr")
    logger.debug(f"df_course: {df_course.shape=}")

    df_sections = pd.read_feather(processed_path / "course_section_dim.ftr")
    logger.debug(f"df_sections: {df_sections.shape=}")

    df_enrollment = pd.read_feather(processed_path / "enrollment_dim.ftr")
    df_enrollment = df_enrollment.loc[(df_enrollment['workflow_state']=='active'),:]
    logger.debug(f"df_enrollment: {df_enrollment.shape=}")

    df_u1 = df_user.merge(df_pseudonym,
                # how='left',
                left_on=['id'],
                right_on=['user_id'],
                sort=True,
                )
    df_u1 = df_u1[['user_id', 'sis_user_id', 'name', 'sortable_name', 'unique_name',
        'last_request_at', 'last_login_at', 'current_login_at']    
    ]
    logger.debug(f"df_u1: {df_u1.shape=}")

    df_s1 = df_sections.merge(df_terms,
                how='left',
                left_on=['enrollment_term_id'],
                right_on=['id'],
                suffixes=['_section', '_term']
                )
    df_s1 = df_s1.loc[:,[
        'id_section', 'name_section', 'sis_source_id_section',
        'name_term', 'date_start', 'date_end', 'sis_source_id_term'
    ]]
    logger.debug(f"df_s1: {df_s1.shape=}")

    df_e1 = df_enrollment.merge(df_s1,
                how='left',
                left_on=['course_section_id'],
                right_on=['id_section'],
                suffixes=['_enrollment', '_section']
                )
    df_e1 = df_e1.loc[:,[
        'user_id', 'type', 'course_section_id', 'course_id', 'name_section', 'sis_source_id_section',
        'name_term', 'date_start', 'date_end', 'sis_source_id_term', 'last_activity_at',
    ]]
    logger.debug(f"df_e1: {df_e1.shape=}")

    df_e2 = df_e1.merge(df_u1,
                how='left',
                left_on=['user_id'],
                right_on=['user_id'],
                suffixes=['_enrollment', '_user']
                )
    df_e2 = df_e2.loc[:,[
        'user_id', 'type', 'course_section_id', 'course_id', 'name_section', 'sis_source_id_section',
        'name_term', 'date_start', 'date_end', 'sis_source_id_term',
        'sis_user_id', 'name', 'sortable_name', 'unique_name',
        'last_request_at', 'last_login_at', 'current_login_at', 'last_activity_at',
    ]].sort_values(['sortable_name', 'sis_source_id_section'])
    df_e2 = df_e2.loc[(df_e2['type']=='StudentEnrollment'),:]
    df_e2 = df_e2.loc[(df_e2['sis_source_id_term']==yearterm),:]
    df_e2['last_request_at'] = df_e2['last_request_at'].fillna(begin_datetime)
    df_e2['last_login_at'] = df_e2['last_login_at'].fillna(begin_datetime)
    df_e2['current_login_at'] = df_e2['current_login_at'].fillna(begin_datetime)
    df_e2['last_activity_at'] = df_e2['last_activity_at'].fillna(begin_datetime)
    df_e2['login_at'] = df_e2[['current_login_at', 'last_login_at']].max(axis=1)
    logger.debug(f"df_e2: {df_e2.shape=}")

    gb = df_e2.groupby(['sortable_name']).agg(
        {
        'sis_user_id': 'max',
        'last_activity_at': 'max',
        'last_request_at': 'max',
        'login_at': 'max',
        }
    )
    logger.debug(f"gb: {gb.shape=}")

    output_fn = output_path / f"{term}{year}_canvas_activity_{today}.csv"
    gb.to_csv(output_fn)
    logger.debug(f"Canvas - Student Last Activity written to: {output_fn}")
    output_fn2 = output_path / f"canvas_activity.csv"
    gb.to_csv(output_fn2)
    logger.debug(f"Canvas - Student Last Activity written to: {output_fn}")

    logger.info(f"End: Canvas - Student Last Activity")


@flow()
def canvas_data_flow():
    """
    Runs all Canvas Data flows.
    Downloads recent Canvas Data.
    Creates Student last activity spreadsheet.
    """

    logger = get_run_logger()
    logger.info(f"Start: canvas_data_flow()")

    canvas_data_download()
    canvas_last_student_activity()
    
    logger.info(f"End: canvas_data_flow()")


@flow()
def canvas_hourly_flow():
    """
    Main Canvas flow.  Runs all Canvas data feed flows hourly.
    """

    logger = get_run_logger()
    logger.info(f"Start: canvas_hourly_flow()")

    logger.info(f"  NO FLOWS DEFINED")
    df = pd.read_csv(r"F:\Data\exporter_data\attendance.csv")
    logger.debug(f"{df.shape=}")
    
    logger.info(f"End: canvas_hourly_flow()")


if __name__ == "__main__":
    canvas_hourly_flow()

