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
        'ADDRESS':
            [
                # 'PEOPLE_ORG_CODE'
                # ,'PEOPLE_ORG_ID'
                'PEOPLE_ORG_CODE_ID'
                ,'ADDRESS_TYPE'
                ,'ADDRESS_LINE_1'
                ,'ADDRESS_LINE_2'
                # ,'ADDRESS_LINE_3'
                ,'CITY'
                ,'STATE'
                ,'ZIP_CODE'
                ,'COUNTRY'
                ,'COUNTY'
                # ,'DAY_PHONE'
                # ,'EVENING_PHONE'
                # ,'MAIN_FAX'
                # ,'ALTERNATE_FAX'
                # ,'NO_MAIL'
                # ,'NO_CALL_DAY_PHONE'
                # ,'NO_CALL_EVE_PHONE'
                # ,'NOT_VALID_BEG_MM'
                # ,'NOT_VALID_BEG_YY'
                # ,'NOT_VALID_END_MM'
                # ,'NOT_VALID_END_YY'
                # ,'ALTERNATE_ADD_TYPE'
                # ,'CREATE_DATE'
                # ,'CREATE_TIME'
                # ,'CREATE_OPID'
                # ,'CREATE_TERMINAL'
                # ,'REVISION_DATE'
                # ,'REVISION_TIME'
                # ,'REVISION_OPID'
                # ,'REVISION_TERMINAL'
                # ,'ABT_JOIN'
                # ,'CITY_PREFIX'
                # ,'CITY_SUFFIX'
                # ,'EMAIL_ADDRESS'
                # ,'ADDRESS_LINE_4'
                # ,'HOUSE_NUMBER'
            ],
        'ASSOCIATION':
            [
                # 'PEOPLE_ORG_CODE'
                # ,'PEOPLE_ORG_ID'
                'PEOPLE_ORG_CODE_ID'
                ,'ASSOCIATION'
                ,'ACADEMIC_YEAR'
                ,'ACADEMIC_TERM'
                ,'ACADEMIC_SESSION'
                # ,'OFFICE_HELD'
                # ,'CREATE_DATE'
                # ,'CREATE_TIME'
                # ,'CREATE_OPID'
                # ,'CREATE_TERMINAL'
                # ,'REVISION_DATE'
                # ,'REVISION_TIME'
                # ,'REVISION_OPID'
                # ,'REVISION_TERMINAL'
                # ,'ABT_JOIN'
            ],
        'CODE_COUNTY':
            [
                # 'CODE_VALUE_KEY'
                'CODE_VALUE'
                ,'SHORT_DESC'
                ,'MEDIUM_DESC'
                ,'LONG_DESC'
                ,'STATUS'
                # ,'CREATE_DATE'
                # ,'CREATE_TIME'
                # ,'CREATE_OPID'
                # ,'CREATE_TERMINAL'
                # ,'REVISION_DATE'
                # ,'REVISION_TIME'
                # ,'REVISION_OPID'
                # ,'REVISION_TERMINAL'
                # ,'CODE_XVAL'
                # ,'CODE_XDESC'
                # ,'ABT_JOIN'
                # ,'CountyId'
            ],
        'CODE_DAY':
            [
                # 'CODE_VALUE_KEY'
                'CODE_VALUE'
                ,'SHORT_DESC'
                ,'MEDIUM_DESC'
                ,'LONG_DESC'
                ,'STATUS'
                # ,'CREATE_DATE'
                # ,'CREATE_TIME'
                # ,'CREATE_OPID'
                # ,'CREATE_TERMINAL'
                # ,'REVISION_DATE'
                # ,'REVISION_TIME'
                # ,'REVISION_OPID'
                # ,'REVISION_TERMINAL'
                ,'DAY_SUNDAY'
                ,'DAY_MONDAY'
                ,'DAY_TUESDAY'
                ,'DAY_WEDNESDAY'
                ,'DAY_THURSDAY'
                ,'DAY_FRIDAY'
                ,'DAY_SATURDAY'
                ,'DAY_SORT'
                # ,'CODE_XVAL'
                # ,'CODE_XDESC'
                # ,'ABT_JOIN'
                ,'BINARY_DAY_FIELD'
            ],
        'DEMOGRAPHICS':
            [
                # 'PEOPLE_CODE'
                # ,'PEOPLE_ID'
                'PEOPLE_CODE_ID'
                ,'ACADEMIC_YEAR'
                ,'ACADEMIC_TERM'
                ,'ACADEMIC_SESSION'
                ,'ETHNICITY'
                ,'GENDER'
                ,'MARITAL_STATUS'
                # ,'RELIGION'
                ,'VETERAN'
                ,'CITIZENSHIP'
                # ,'VISA'
                # ,'RETIRED'
                # ,'CREATE_DATE'
                # ,'CREATE_TIME'
                # ,'CREATE_OPID'
                # ,'CREATE_TERMINAL'
                # ,'REVISION_DATE'
                # ,'REVISION_TIME'
                # ,'REVISION_OPID'
                # ,'REVISION_TERMINAL'
                # ,'ABT_JOIN'
                # ,'LAST_ACTIVITY'
                # ,'CURRENT_ACTIVITY'
                # ,'PRIMARY_LANGUAGE'
                # ,'HOME_LANGUAGE'
                # ,'MONTHS_IN_COUNTRY'
                # ,'DUAL_CITIZENSHIP'
                # ,'PERMANENT_HOME'
                # ,'LEGAL_RESIDENCE'
            ],
        'EDUCATION':
            [
                # 'PEOPLE_CODE'
                # ,'PEOPLE_ID'
                'PEOPLE_CODE_ID'
                ,'ORG_CODE_ID'
                ,'DEGREE'
                # ,'CURRICULUM'
                ,'GRADEPOINT_AVERAGE'
                # ,'START_DATE'
                ,'END_DATE'
                # ,'HONORS'
                # ,'TRANSCRIPT_DATE'
                ,'CLASS_RANK'
                ,'CLASS_SIZE'
                # ,'TRANSFER_CREDITS'
                # ,'FIN_AID_AMOUNT'
                # ,'CREATE_DATE'
                # ,'CREATE_TIME'
                # ,'CREATE_OPID'
                # ,'CREATE_TERMINAL'
                # ,'REVISION_DATE'
                # ,'REVISION_TIME'
                # ,'REVISION_OPID'
                # ,'REVISION_TERMINAL'
                # ,'ABT_JOIN'
                # ,'UNWEIGHTED_GPA'
                # ,'UNWEIGHTED_GPA_SCALE'
                ,'WEIGHTED_GPA'
                ,'WEIGHTED_GPA_SCALE'
                # ,'QUARTILE'
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
        'EVENT':
            [
                'EVENT_ID'
                ,'EVENT_MED_NAME'
                ,'EVENT_LONG_NAME'
                ,'EVENT_TYPE'
                ,'ORG_CODE_ID'
                ,'PROGRAM'
                ,'COLLEGE'
                ,'DEPARTMENT'
                ,'CURRICULUM'
                ,'CLASS_LEVEL'
                ,'NONTRAD_PROGRAM'
                ,'POPULATION'
                ,'EVENT_STATUS'
                ,'CIP_CODE'
                ,'SPEEDE_CODE'
                ,'SERIAL_ID'
                ,'CREDIT_TYPE'
                ,'CREDITS'
                ,'CEU'
                ,'CREATE_DATE'
                ,'CREATE_TIME'
                ,'CREATE_OPID'
                ,'CREATE_TERMINAL'
                ,'REVISION_DATE'
                ,'REVISION_TIME'
                ,'REVISION_OPID'
                ,'REVISION_TERMINAL'
                ,'DESCRIPTION'
                ,'ABT_JOIN'
                ,'GENERAL_ED'
                ,'SCHEDULE_PRIORITY'
                ,'PUBLICATION_NAME_1'
                ,'PUBLICATION_NAME_2'
                ,'REPEATABLE'
                ,'EventId'
                ,'HideOnlineSearch'
            ],
        'ORGANIZATION':
            [
                # 'ORG_CODE'
                # ,'ORG_ID'
                'ORG_CODE_ID'
                ,'ORG_NAME_1'
                ,'ORG_NAME_2'
                ,'ORG_SORT_NAME'
                # ,'PREFERRED_ADD'
                # ,'MAIN_SATELLITE'
                ,'ETS_CODE'
                # ,'ORG_IDENTIFIER'
                # ,'PARENT_CODE_ID'
                # ,'FEDERAL_EIN'
                # ,'STATE_EIN'
                # ,'FED_TAX_EXEMPT'
                # ,'STATE_TAX_EXEMPT'
                # ,'CREATE_DATE'
                # ,'CREATE_TIME'
                # ,'CREATE_OPID'
                # ,'CREATE_TERMINAL'
                # ,'REVISION_DATE'
                # ,'REVISION_TIME'
                # ,'REVISION_OPID'
                # ,'REVISION_TERMINAL'
                # ,'ABT_JOIN'
                # ,'FICE_CODE'
                # ,'SEVIS_SCHOOL_CODE'
                # ,'TRANSMITTER_CODE'
                # ,'OrganizationId'
                # ,'PrimaryPhoneId'
                # ,'Code'
                # ,'PrimaryEmailId'
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
        'RESIDENCY':
            [
                # 'PEOPLE_CODE'
                # ,'PEOPLE_ID'
                'PEOPLE_CODE_ID'
                ,'ACADEMIC_YEAR'
                ,'ACADEMIC_TERM'
                ,'ACADEMIC_SESSION'
                ,'RESIDENT_COMMUTER'
                # ,'FOOD_PLAN'
                # ,'DORM_PLAN'
                # ,'DORM_CAMPUS'
                ,'DORM_BUILDING'
                ,'DORM_ROOM'
                # ,'DORM_KEY'
                # ,'LOCKER'
                ,'MAIL_SLOT'
                # ,'CREATE_DATE'
                # ,'CREATE_TIME'
                # ,'CREATE_OPID'
                # ,'CREATE_TERMINAL'
                # ,'REVISION_DATE'
                # ,'REVISION_TIME'
                # ,'REVISION_OPID'
                # ,'REVISION_TERMINAL'
                # ,'ABT_JOIN'
                # ,'LAST_ACTIVITY'
                # ,'CURRENT_ACTIVITY'
            ],
        'SECTIONPER':
            [
                'ACADEMIC_YEAR'
                ,'ACADEMIC_TERM'
                ,'ACADEMIC_SESSION'
                ,'EVENT_ID'
                ,'EVENT_SUB_TYPE'
                ,'SECTION'
                ,'PERSON_CODE_ID'
                # ,'PERCENTAGE'
                # ,'CREATE_DATE'
                # ,'CREATE_TIME'
                # ,'CREATE_OPID'
                # ,'CREATE_TERMINAL'
                # ,'REVISION_DATE'
                # ,'REVISION_TIME'
                # ,'REVISION_OPID'
                # ,'REVISION_TERMINAL'
                # ,'ABT_JOIN'
            ],
        'SECTIONS':
            [
                'ACADEMIC_YEAR'
                ,'ACADEMIC_TERM'
                ,'ACADEMIC_SESSION'
                ,'EVENT_ID'
                ,'EVENT_SUB_TYPE'
                ,'SECTION'
                ,'EVENT_MED_NAME'
                ,'EVENT_LONG_NAME'
                ,'EVENT_TYPE'
                # ,'ORG_CODE_ID'
                # ,'PROGRAM'
                ,'COLLEGE'
                # ,'DEPARTMENT'
                # ,'CURRICULUM'
                # ,'CLASS_LEVEL'
                # ,'NONTRAD_PROGRAM'
                # ,'POPULATION'
                ,'EVENT_STATUS'
                # ,'CIP_CODE'
                # ,'SPEEDE_CODE'
                # ,'SERIAL_ID'
                # ,'ROOM_TYPE'
                # ,'CREDIT_TYPE'
                ,'CREDITS'
                # ,'CEU'
                # ,'MINUTES_WEEK'
                # ,'CONTACT_HOURS'
                # ,'REPORT_CARD_PRINT'
                # ,'TRANSCRIPT_PRINT'
                # ,'MIN_PARTICIPANT'
                # ,'TARGET_PARTICIPANT'
                # ,'MAX_PARTICIPANT'
                # ,'OTHER_ORG'
                # ,'OTHER_ORG_PART'
                # ,'OTHER_PROGRAM'
                # ,'OTHER_PROGRAM_PART'
                # ,'OTHER_COLLEGE'
                # ,'OTHER_COLLEGE_PART'
                # ,'OTHER_DEPARTMENT'
                # ,'OTHER_DEPT_PART'
                # ,'OTHER_CURRICULUM'
                # ,'OTHER_CURRIC_PART'
                # ,'OTHER_CLASS_LEVEL'
                # ,'OTHER_CLEVEL_PART'
                # ,'OTHER_NONTRAD'
                # ,'OTHER_NONTRAD_PART'
                # ,'OTHER_POPULATION'
                # ,'OTHER_POP_PART'
                ,'ADDS'
                ,'DROPS'
                ,'WAIT_LIST'
                # ,'OTHER_ORG_ADD'
                # ,'OTHER_ORG_DROP'
                # ,'OTHER_ORG_WAIT'
                # ,'OTHER_PROGRAM_ADD'
                # ,'OTHER_PROGRAM_DROP'
                # ,'OTHER_PROGRAM_WAIT'
                # ,'OTHER_COLLEGE_ADD'
                # ,'OTHER_COLLEGE_DROP'
                # ,'OTHER_COLLEGE_WAIT'
                # ,'OTHER_DEPT_ADD'
                # ,'OTHER_DEPT_DROP'
                # ,'OTHER_DEPT_WAIT'
                # ,'OTHER_CURR_ADD'
                # ,'OTHER_CURR_DROP'
                # ,'OTHER_CURR_WAIT'
                # ,'OTHER_CLEVEL_ADD'
                # ,'OTHER_CLEVEL_DROP'
                # ,'OTHER_CLEVEL_WAIT'
                # ,'OTHER_NONTRAD_ADD'
                # ,'OTHER_NONTRAD_DROP'
                # ,'OTHER_NONTRAD_WAIT'
                # ,'OTHER_POP_ADD'
                # ,'OTHER_POP_DROP'
                # ,'OTHER_POP_WAIT'
                # ,'WEEK_NUMBER'
                ,'START_DATE'
                ,'END_DATE'
                # ,'DESCRIPTION'
                # ,'SEC_ENROLL_STATUS'
                # ,'CREATE_DATE'
                # ,'CREATE_TIME'
                # ,'CREATE_OPID'
                # ,'CREATE_TERMINAL'
                # ,'REVISION_DATE'
                # ,'REVISION_TIME'
                # ,'REVISION_OPID'
                # ,'REVISION_TERMINAL'
                # ,'ABT_JOIN'
                # ,'GENERAL_ED'
                # ,'CONTACT_HR_SESSION'
                ,'MID_GRD_RECEIVED'
                ,'FINAL_GRD_RECEIVED'
                # ,'SCHEDULE_PRIORITY'
                # ,'PUBLICATION_NAME_1'
                # ,'PUBLICATION_NAME_2'
                # ,'LATE_REG_FEE_DATE'
                # ,'REQUESTED_MEETINGS'
                # ,'SCHEDULED_MEETINGS'
                # ,'ANONYMOUS_GRADING'
                # ,'CANCEL_REASON'
                # ,'LAST_REFUND_DATE'
                # ,'ADJUSTMENT_POLICY_ID'
                # ,'REPEATABLE'
                # ,'ACTIVITY_TYPE_GRADING'
                # ,'REGISTRATION_TYPE'
                # ,'SectionId'
                # ,'AssignmentWeightingMethod'
                # ,'UseWeightedAssignmentTypes'
                # ,'RequiresGradeApproval'
                # ,'HideOnlineSearch'
            ],
        'SECTIONSCHEDULE':
            [
                'ACADEMIC_YEAR'
                ,'ACADEMIC_TERM'
                ,'ACADEMIC_SESSION'
                ,'EVENT_ID'
                ,'EVENT_SUB_TYPE'
                ,'SECTION'
                ,'DAY'
                ,'START_TIME'
                ,'END_TIME'
                # ,'ORG_CODE_ID'
                ,'BUILDING_CODE'
                ,'ROOM_ID'
                # ,'CREATE_DATE'
                # ,'CREATE_TIME'
                # ,'CREATE_OPID'
                # ,'CREATE_TERMINAL'
                # ,'REVISION_DATE'
                # ,'REVISION_TIME'
                # ,'REVISION_OPID'
                # ,'REVISION_TERMINAL'
                # ,'ABT_JOIN'
                # ,'CALENDARDET_EVENT_KEY'
                # ,'SECTIONSCHEDULE_ID'
            ],
        'TESTSCORES':
            [
                # 'PEOPLE_CODE'
                # ,'PEOPLE_ID'
                'PEOPLE_CODE_ID'
                ,'TEST_ID'
                ,'TEST_TYPE'
                ,'TEST_DATE'
                ,'RAW_SCORE'
                ,'CONVERSION_FACTOR'
                ,'CONVERTED_SCORE'
                # ,'TRANSCRIPT_PRINT'
                # ,'CREATE_DATE'
                # ,'CREATE_TIME'
                # ,'CREATE_OPID'
                # ,'CREATE_TERMINAL'
                # ,'REVISION_DATE'
                # ,'REVISION_TIME'
                # ,'REVISION_OPID'
                # ,'REVISION_TERMINAL'
                # ,'ABT_JOIN'
                ,'ALPHA_SCORE'
                ,'ALPHA_SCORE_1'
                ,'ALPHA_SCORE_2'
                ,'ALPHA_SCORE_3'
            ],
        'TRANSCRIPTDETAIL':
            [
                # 'PEOPLE_CODE'
                # ,'PEOPLE_ID'
                'PEOPLE_CODE_ID'
                ,'ACADEMIC_YEAR'
                ,'ACADEMIC_TERM'
                ,'ACADEMIC_SESSION'
                ,'EVENT_ID'
                ,'EVENT_SUB_TYPE'
                ,'SECTION'
                # ,'TRANSCRIPT_SEQ'
                # ,'ORG_CODE_ID'
                # ,'WEEK_NUMBER'
                ,'START_DATE'
                ,'END_DATE'
                ,'EVENT_MED_NAME'
                ,'EVENT_LONG_NAME'
                ,'EVENT_TYPE'
                # ,'CIP_CODE'
                # ,'SPEEDE_CODE'
                # ,'SERIAL_ID'
                ,'CREDIT_TYPE'
                ,'CREDIT'
                # ,'CEU'
                # ,'MINUTES_WEEK'
                # ,'CONTACT_HOURS'
                # ,'CREDIT_GRADE'
                # ,'CREDIT_BILLING'
                # ,'CREDIT_FTE'
                # ,'CREDIT_WORKLOAD'
                # ,'CREDIT_FED_FA'
                # ,'CREDIT_ST_FA'
                # ,'CREDIT_CNTY_FA'
                # ,'CREDIT_FED_RPT'
                # ,'CREDIT_ST_RPT'
                # ,'CREDIT_CNTY_RPT'
                ,'MID_GRADE'
                ,'FINAL_GRADE'
                # ,'FINAL_QUALITY_PNTS'
                ,'REPEATED'
                # ,'REPEATED_YEAR'
                # ,'REPEATED_TERM'
                # ,'REPEATED_SESSION'
                # ,'REPEATED_ID'
                # ,'REPEATED_SUB_TYPE'
                # ,'REPEATED_SECTION'
                # ,'COURSE_PRINT_RC'
                # ,'COURSE_PRINT_TRAN'
                ,'ADD_DROP_WAIT'
                ,'STATUS_DATE'
                # ,'PREREG_REG_STATUS'
                # ,'CREATE_DATE'
                # ,'CREATE_TIME'
                # ,'CREATE_OPID'
                # ,'CREATE_TERMINAL'
                # ,'REVISION_DATE'
                # ,'REVISION_TIME'
                # ,'REVISION_OPID'
                # ,'REVISION_TERMINAL'
                # ,'ABT_JOIN'
                # ,'CLASS_LEVEL_CREDITS'
                # ,'CONTACT_HR_SESSION'
                # ,'HONORS'
                # ,'REFERENCE_EVENT_ID'
                # ,'REFERENCE_SUB_TYPE'
                # ,'ATTEND_STATUS'
                # ,'LAST_ATTEND_DATE'
                # ,'COMMENT_EXIST'
                # ,'SPONSOR_CODE_ID'
                # ,'AGREEMENT_NUMBER'
                # ,'REFERENCE_NUMBER_1'
                # ,'REFERENCE_NUMBER_2'
                # ,'STATUS_TIME'
                # ,'PROTECT_AGREEMENT_FLAG'
                # ,'MILITARY_REFERENCE_CODE'
                # ,'TranscriptDetailId'
            ],
        'TRANSCRIPTGPA':
            [
                # 'PEOPLE_CODE'
                # ,'PEOPLE_ID'
                'PEOPLE_CODE_ID'
                ,'ACADEMIC_YEAR'
                ,'ACADEMIC_TERM'
                ,'ACADEMIC_SESSION'
                # ,'TRANSCRIPT_SEQ'
                ,'RECORD_TYPE'
                ,'PROGRAM'
                ,'DEGREE'
                ,'CURRICULUM'
                ,'ATTEMPTED_CREDITS'
                ,'EARNED_CREDITS'
                ,'TOTAL_CREDITS'
                ,'TRANSFER_CREDITS'
                ,'GPA_CREDITS'
                ,'QUALITY_POINTS'
                ,'GPA'
                # ,'CLASS_RANK'
                # ,'CLASS_SIZE'
                # ,'NUMBER_REPEATS'
                # ,'CONTACT_HOURS'
                # ,'SUPPORTING_DETAIL'
                # ,'CREATE_DATE'
                # ,'CREATE_TIME'
                # ,'CREATE_OPID'
                # ,'CREATE_TERMINAL'
                # ,'REVISION_DATE'
                # ,'REVISION_TIME'
                # ,'REVISION_OPID'
                # ,'REVISION_TERMINAL'
                # ,'ABT_JOIN'
                # ,'CL_LEVEL_FOR_RANK'
                # ,'PROGRAM_FOR_RANK'
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


