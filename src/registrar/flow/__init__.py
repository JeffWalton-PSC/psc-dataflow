import datetime as dt
import pandas as pd
from prefect import flow, get_run_logger
from src.powercampus.task import current_yearterm
from src.powercampus.flow import academic_table, address_table, demographics_table, education_table, \
    emailaddress_table, organization_table, institution_table, people_table, residency_table
from src.dataframe.task import filter_rows, keep_columns, merge, rename_columns, write_csv_textfile



BEGIN_YEAR = "2011"


@flow()
def daily_census_file():
    """
    Daily Census File flow.
    """
    logger = get_run_logger()
    year, term, start_of_term, end_of_term, yearterm_sort, yearterm =  current_yearterm()
    logger.debug(f"{year=}, {term=}")

    df_academic = academic_table(year, term)
    df_academic = filter_rows(df_academic, "(primary_flag=='Y') & (academic_session=='') & (curriculum!='ADVST') & (credits>0)")

    df_people = people_table()

    df = merge(df_academic, ['people_code_id'], df_people, ['people_code_id'])

    df_address = address_table()
    df_home_address = filter_rows(df_address, "address_type=='HOME'")
    df = merge(df, ['people_code_id'], df_home_address, ['people_org_code_id'])
    df_campus_address = filter_rows(df_address, "address_type=='MLBX'")
    df_campus_address = keep_columns(df_campus_address, ['people_org_code_id', 'address_line_1'])
    df_campus_address = rename_columns(df_campus_address, {'address_line_1': 'campus_mailbox'})
    df = merge(df, ['people_code_id'], df_campus_address, ['people_org_code_id'])

    df_email = emailaddress_table()
    df_student_psc_email = filter_rows(df_email, "(emailtype=='HOME') & (isactive==True)")
    df_student_psc_email = keep_columns(df_student_psc_email, ['peopleorgcodeid', 'email'])
    df_student_psc_email = rename_columns(df_student_psc_email, {'email': 'email_address'})
    df = merge(df, ['people_code_id'], df_student_psc_email, ['peopleorgcodeid'])

    df_demographics = demographics_table(year, term)
    df_demographics = keep_columns(df_demographics, ['people_code_id','ethnicity', 'gender', 'marital_status', 'veteran', 'citizenship'])
    # df_demographics = rename_columns(df_demographics, {'ethnicity': 'old_ethnicity_code'})
    df = merge(df, ['people_code_id'], df_demographics, ['people_code_id'])

    df_advisor = keep_columns(df_people, ['people_code_id', 'last_name', 'first_name'])
    df_advisor = rename_columns(df_advisor, {'last_name': 'advisor_last_name',
                                            'first_name': 'advisor_first_name',
                                            })
    df = merge(df, ['advisor'], df_advisor, ['people_code_id'])

    df_residency = residency_table(year, term)
    df_residency = keep_columns(df_residency, ['people_code_id', 'resident_commuter', 'dorm_building', 'dorm_room', 'mail_slot'])
    df_residency = rename_columns(df_residency, {'mail_slot': 'campus_mailslot'})
    df = merge(df, ['people_code_id'], df_residency, ['people_code_id'])

    df_education = education_table()
    df_organization = organization_table()
    df_institution = institution_table()

    print(f"{df.shape=}")
    print(df.head())

    




@flow()
def registrar_flow():
    """
    Primary flow that runs all Registrar flows.
    """
    logger = get_run_logger()
    daily_census_file()



# if __name__ == "__main__":
#     registrar_flow()

