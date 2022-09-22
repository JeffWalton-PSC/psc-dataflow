import pandas as pd
import powercampus as pc
from datetime import timedelta
from prefect import task, get_run_logger
from prefect.tasks import task_input_hash


table_fields = {
        'ACADEMIC': 
            [
                # 'PEOPLE_CODE'
                'PEOPLE_ID'
                ,'PEOPLE_CODE_ID'
                ,'ACADEMIC_YEAR'
                ,'ACADEMIC_TERM'
                ,'ACADEMIC_SESSION'
                ,'PROGRAM'
                ,'DEGREE'
                ,'CURRICULUM'
                ,'COLLEGE'
                ,'DEPARTMENT'
                ,'CLASS_LEVEL'
                # ,'NONTRAD_PROGRAM'
                # ,'POPULATION'
                ,'ADVISOR'
                ,'ADMIT_YEAR'
                ,'ADMIT_TERM'
                # ,'ADMIT_SESSION'
                ,'ADMIT_DATE'
                ,'MATRIC'
                ,'MATRIC_YEAR'
                ,'MATRIC_TERM'
                # ,'MATRIC_SESSION'
                ,'MATRIC_DATE'
                ,'FULL_PART'
                # ,'ACADEMIC_STANDING'
                # ,'REGISTER_LIMIT'
                # ,'EXPECT_GRAD_MM'
                # ,'EXPECT_GRAD_YYYY'
                ,'ENROLL_SEPARATION'
                ,'SEPARATION_DATE'
                ,'CREDITS'
                # ,'CREATE_DATE'
                # ,'CREATE_TIME'
                # ,'CREATE_OPID'
                # ,'CREATE_TERMINAL'
                # ,'REVISION_DATE'
                # ,'REVISION_TIME'
                # ,'REVISION_OPID'
                # ,'REVISION_TERMINAL'
                # ,'ABT_JOIN'
                # ,'PREREG_VALIDATE'
                # ,'PREREG_VAL_WHO'
                # ,'PREREG_VAL_DATE'
                # ,'REG_VALIDATE'
                # ,'REG_VAL_WHO'
                # ,'REG_VAL_DATE'
                ,'GRADUATED'
                ,'GRADUATED_YEAR'
                ,'GRADUATED_TERM'
                # ,'GRADUATED_SESSION'
                # ,'ORG_CODE_ID'
                ,'ACADEMIC_FLAG'
                # ,'APPLICATION_FLAG'
                ,'APP_STATUS'
                ,'APP_STATUS_DATE'
                ,'APP_DECISION'
                ,'APP_DECISION_DATE'
                # ,'COUNSELOR'
                ,'COLLEGE_ATTEND'
                # ,'ACADEMIC_RATING'
                # ,'ADVOCATE'
                # ,'ACA_PLAN_SETUP'
                # ,'STATUS'
                # ,'TRANSCRIPT_SEQ'
                # ,'LAST_ACTIVITY'
                # ,'CURRENT_ACTIVITY'
                # ,'INQUIRY_FLAG'
                # ,'INQUIRY_DATE'
                # ,'FIN_AID_CANDIDATE'
                # ,'EXTRA_CURRICULAR'
                # ,'DEGREE_CANDIDATE'
                # ,'INTEREST_LEVEL'
                # ,'INQ_STATUS'
                # ,'INQ_STATUS_DATE'
                # ,'APPLICATION_DATE'
                ,'PRIMARY_FLAG'
                ,'PROGRAM_START_DATE'
                ,'PROGRAM_END_DATE'
                # ,'MET_ENGLISH_REQUIREMENT'
                # ,'DROP_BELOW_FULL_CODE'
                # ,'AUTH_START_DATE'
                # ,'AUTH_END_DATE'
                # ,'PROTECT_COUNSELOR'
                ,'ENROLLMENT_STATUS_DATE'
                # ,'INACTIVE_STATUS_DATE'
                # ,'FULL_PART_NON_WITHDRAWN'
                # ,'FULL_PART_DISP_WITHDRAWN'
            ],
        'EmailAddress':
            [
            'EmailAddressId'
            # ,'PeopleOrgCode'
            # ,'PeopleOrgId'
            ,'PeopleOrgCodeId'
            ,'EmailType'
            ,'Email'
            ,'IsActive'
            # ,'Note'
            # ,'CREATE_DATE'
            # ,'CREATE_TIME'
            # ,'CREATE_OPID'
            # ,'CREATE_TERMINAL'
            # ,'REVISION_DATE'
            # ,'REVISION_TIME'
            # ,'REVISION_OPID'
            # ,'REVISION_TERMINAL'
            ],
        'PEOPLE':
            [
                # 'PEOPLE_CODE'
                # ,'PEOPLE_ID'
                'PEOPLE_CODE_ID'
                # ,'PREVIOUS_ID'
                ,'GOVERNMENT_ID'
                # ,'PREV_GOV_ID'
                # ,'PREFIX'
                ,'FIRST_NAME'
                ,'MIDDLE_NAME'
                ,'LAST_NAME'
                # ,'SUFFIX'
                # ,'NICKNAME'
                # ,'PREFERRED_ADD'
                ,'BIRTH_DATE'
                # ,'BIRTH_CITY'
                # ,'BIRTH_STATE'
                # ,'BIRTH_ZIP_CODE'
                # ,'BIRTH_COUNTRY'
                # ,'BIRTH_COUNTY'
                ,'DECEASED_DATE'
                ,'DECEASED_FLAG'
                # ,'RELEASE_INFO'
                # ,'CREATE_DATE'
                # ,'CREATE_TIME'
                # ,'CREATE_OPID'
                # ,'CREATE_TERMINAL'
                # ,'REVISION_DATE'
                # ,'REVISION_TIME'
                # ,'REVISION_OPID'
                # ,'REVISION_TERMINAL'
                # ,'ABT_JOIN'
                # ,'TAX_ID'
                ,'PersonId'
                ,'PrimaryPhoneId'
                # ,'Last_Name_Prefix'
                # ,'LegalName'
                ,'DisplayName'
                ,'GenderIdentity'
                ,'Pronoun'
                ,'PrimaryEmailId'
            ],
    }


@task(retries=3, retry_delay_seconds=10,
    cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=1),
    )
def current_yearterm() -> tuple[str, str, pd.Timestamp, pd.Timestamp, str, str]:
    logger = get_run_logger()
    logger.debug(f"current_yearterm()")

    df = pc.current_yearterm()
    return (df['year'].iloc[0], df['term'].iloc[0], df['start_of_term'].iloc[0], 
        df['end_of_term'].iloc[0], df['yearterm_sort'].iloc[0], df['yearterm'].iloc[0])


@task(retries=3, retry_delay_seconds=10,
    cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=10),
    )
def read_table(name:str, where:str="") -> pd.DataFrame:
    logger = get_run_logger()
    logger.debug(f"read_table({name=}, {where=})")

    return pc.select(name, table_fields[name], where, distinct=True)


