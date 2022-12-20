import datetime as dt
import pandas as pd
from pathlib import WindowsPath
from prefect import flow, get_run_logger
from src.powercampus.task import current_yearterm
from src.powercampus.flow import academic_table, academiccalendar_table, address_table, demographics_table, education_table, \
    emailaddress_table, organization_table, institution_table, people_table, residency_table
from src.dataframe.task import deduplicate, filter_rows, keep_columns, merge, rename_columns, sort_rows, write_csv_textfile



BEGIN_YEAR = "2011"
output_path = WindowsPath("F:\Data\Registrar\output")


@flow()
def daily_census_file():
    """
    Daily Census File flow.
    """
    logger = get_run_logger()
    year, term, start_of_term, end_of_term, yearterm_sort, yearterm =  current_yearterm()
    logger.debug(f"{year=}, {term=}")

    df_academic = academic_table(BEGIN_YEAR).rename(columns=str.lower)
    df_academic = filter_rows(df_academic, f"(academic_year=='{year}') & (academic_term=='{term}') & (primary_flag=='Y') & (academic_session=='') & (curriculum!='ADVST') & (credits>0)")

    df_people = people_table().rename(columns=str.lower)

    df = merge(df_academic, ['people_code_id'], df_people, ['people_code_id'])

    df_address = address_table().rename(columns=str.lower)
    df_home_address = filter_rows(df_address, "address_type=='HOME'")
    df = merge(df, ['people_code_id'], df_home_address, ['people_org_code_id'])
    df_campus_address = filter_rows(df_address, "address_type=='MLBX'")
    df_campus_address = keep_columns(df_campus_address, ['people_org_code_id', 'address_line_1'])
    df_campus_address = rename_columns(df_campus_address, {'address_line_1': 'campus_mailbox'})
    df = merge(df, ['people_code_id'], df_campus_address, ['people_org_code_id'])

    df_email = emailaddress_table().rename(columns=str.lower)
    df_student_psc_email = filter_rows(df_email, "(emailtype=='HOME') & (isactive==True)")
    df_student_psc_email = keep_columns(df_student_psc_email, ['peopleorgcodeid', 'email'])
    df_student_psc_email = rename_columns(df_student_psc_email, {'email': 'email_address'})
    df = merge(df, ['people_code_id'], df_student_psc_email, ['peopleorgcodeid'])

    df_demographics = demographics_table(year, term).rename(columns=str.lower)
    df_demographics = keep_columns(df_demographics, ['people_code_id','ethnicity', 'gender', 'marital_status', 'veteran', 'citizenship'])
    # df_demographics = rename_columns(df_demographics, {'ethnicity': 'old_ethnicity_code'})
    df = merge(df, ['people_code_id'], df_demographics, ['people_code_id'])

    df_advisor = keep_columns(df_people, ['people_code_id', 'last_name', 'first_name'])
    df_advisor = rename_columns(df_advisor, {'last_name': 'advisor_last_name',
                                            'first_name': 'advisor_first_name',
                                            })
    df = merge(df, ['advisor'], df_advisor, ['people_code_id'])

    df_residency = residency_table(year, term).rename(columns=str.lower)
    df_residency = keep_columns(df_residency, ['people_code_id', 'resident_commuter', 'dorm_building', 'dorm_room', 'mail_slot'])
    df_residency = rename_columns(df_residency, {'mail_slot': 'campus_mailslot'})
    df = merge(df, ['people_code_id'], df_residency, ['people_code_id'])

    df_education = education_table().rename(columns=str.lower)
    df_organization = organization_table().rename(columns=str.lower)
    df_institution = institution_table().rename(columns=str.lower)
    df_highschool = merge(df_organization, 'org_code_id', df_institution, 'org_code_id')
    df_highschool = filter_rows(df_highschool, "institute_type=='HS'")
    df_education = merge(df_education, 'org_code_id', df_highschool, 'org_code_id', how='inner')
    df_education = rename_columns(df_education, {'mail_slot': 'campus_mailslot',
                                                'org_code_id': 'hs_org_code_id',
                                                'org_name_1': 'hs_name',
                                                'weighted_gpa': 'hs_gpa',
                                                'degree': 'hs_degree',
                                                'end_date': 'hs_grad_date',
                                                'class_rank': 'hs_rank',
                                                'class_size': 'hs_class_size',
                                                'weighted_gpa_scale': 'hs_weighted_gpa_scale',
                                                })
    df_education['hs_rank%'] = df_education['hs_rank'] / df_education['hs_class_size'] *100.0
    df_education = sort_rows(df_education, ['people_code_id', 'hs_grad_date'])
    df_education = deduplicate(df_education, ['people_code_id'])
    df = merge(df, ['people_code_id'], df_education, ['people_code_id'])

    out_fn = output_path / f"{year}{term}_census_{dt.datetime.now().strftime('%Y%m%d')}.csv"
    write_csv_textfile(df, out_fn)
    logger.debug(df.columns)
    logger.debug(f"{df.shape=}")
    




@flow()
def registrar_flow():
    """
    Primary flow that runs all Registrar flows.
    """
    logger = get_run_logger()
    daily_census_file()



# if __name__ == "__main__":
#     registrar_flow()

