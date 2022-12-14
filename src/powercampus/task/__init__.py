import datetime as dt
import pandas as pd
from datetime import timedelta
from prefect import task, get_run_logger
from prefect.tasks import task_input_hash
from src.powercampus import START_ACADEMIC_YEAR, TASK_CEM, TASK_RETRIES, TASK_RDS
import local_db


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
        'ACADEMICCALENDAR':
            [
                'ACADEMIC_YEAR'
                ,'ACADEMIC_TERM'
                ,'ACADEMIC_SESSION'
                ,'START_DATE'
                ,'END_DATE'
                ,'FISCAL_YEAR'
                ,'ACADEMIC_WEEKS'
                ,'ACADEMIC_MONTHS'
                ,'NUMBER_COURSES'
                ,'PRE_REG_DATE'
                ,'REG_DATE'
                ,'LAST_REG_DATE'
                ,'GRADE_WTHDRWL_DATE'
                ,'GRADE_PENALTY_DATE'
                # ,'CREATE_DATE'
                # ,'CREATE_TIME'
                # ,'CREATE_OPID'
                # ,'CREATE_TERMINAL'
                # ,'REVISION_DATE'
                # ,'REVISION_TIME'
                # ,'REVISION_OPID'
                # ,'REVISION_TERMINAL'
                # ,'ABT_JOIN'
                ,'TRUE_ACADEMIC_YEAR'
                ,'FIN_AID_YEAR'
                ,'FIN_AID_TERM'
                ,'MID_START_DATE'
                ,'MID_END_DATE'
                ,'FINAL_START_DATE'
                ,'FINAL_END_DATE'
                # ,'SessionPeriodId'
                # ,'FinAidNonTerm'
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
        'BUILDING':
            [
                # 'ORG_CODE'
                # ,'ORG_ID'
                # ,'ORG_CODE_ID'
                'BUILDING_CODE'
                ,'BUILD_NAME_1'
                # ,'BUILD_NAME_2'
                # ,'OWNER_CODE'
                # ,'OWNER_ID'
                # ,'OWNERSHIP_TYPE'
                # ,'CONSTRUCTED_DATE'
                # ,'CONSTRUCTION_TYPE'
                # ,'RENOVATE_DATE'
                # ,'ZONING'
                # ,'CONDITION'
                # ,'POWER'
                # ,'AIR_CONDITION'
                # ,'TOTAL_SQ_FT'
                # ,'USABLE_SQ_FT'
                # ,'SURROUND_SQ_FT'
                # ,'NUMBER_FLOORS'
                # ,'SMOKE_NONSMOKE'
                # ,'HANDICAP_PARKING'
                # ,'RAMP_ACCESS'
                # ,'HANDICAP_RESTROOMS'
                # ,'CREATE_DATE'
                # ,'CREATE_TIME'
                # ,'CREATE_OPID'
                # ,'CREATE_TERMINAL'
                # ,'REVISION_DATE'
                # ,'REVISION_TIME'
                # ,'REVISION_OPID'
                # ,'REVISION_TERMINAL'
                # ,'ABT_JOIN'
                # ,'BuildingId'
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
        'INSTITUTION':
            [
                # 'ORG_CODE'
                # ,'ORG_ID'
                'ORG_CODE_ID'
                ,'INSTITUTE_TYPE'
                # ,'REL_AFFILIATE'
                # ,'ENROLLMENT'
                # ,'TUITION_RANGE'
                # ,'CATALOG_RCVD'
                # ,'CATALOG_RCVD_DATE'
                # ,'CREATE_DATE'
                # ,'CREATE_TIME'
                # ,'CREATE_OPID'
                # ,'CREATE_TERMINAL'
                # ,'REVISION_DATE'
                # ,'REVISION_TIME'
                # ,'REVISION_OPID'
                # ,'REVISION_TERMINAL'
                # ,'ABT_JOIN'
                # ,'CONTROL_TYPE'
                # ,'RESP_COUNSELOR'
                # ,'TRANSFER_POLICY'
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
                ,'CIP_CODE'
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
                ,'MAX_PARTICIPANT'
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
                ,'REVISION_DATE'
                ,'REVISION_TIME'
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
                # ,'ALPHA_SCORE'
                # ,'ALPHA_SCORE_1'
                # ,'ALPHA_SCORE_2'
                # ,'ALPHA_SCORE_3'
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
                ,'ORG_CODE_ID'
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
                ,'REFERENCE_EVENT_ID'
                ,'REFERENCE_SUB_TYPE'
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


# create active student list from 2-year rolling window
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


def apply_active(n_years_active_window: int, in_df: pd.DataFrame) -> pd.DataFrame:
    """
    returns copy of in_df with only records for active students

    in_df is an input DataFrame, must have PEOPLE_CODE_ID field
    """

    # return records for active students
    return pd.merge(in_df, active_students(n_years_active_window), how="inner", on="PEOPLE_CODE_ID")


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RDS,
    cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=TASK_CEM),
    )
def apply_active_with_email_address(n_years_active_window: int, in_df: pd.DataFrame) -> pd.DataFrame:
    """
    returns copy of in_df with only records for active students with email_address

    in_df is an input DataFrame, must have PEOPLE_CODE_ID field
    """

    # return records for active students with email_address
    return pd.merge(apply_active(n_years_active_window, in_df=in_df), with_email_address(), how="inner", on="PEOPLE_CODE_ID")


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
def read_table(name:str, where:str="", **kwargs1) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info(f"read_table({name=})")
    logger.debug(f"read_table({name=}, {table_fields[name]=}, {where=}, {kwargs1=}, {kwargs1.keys()=})")
    
    return select(name, table_fields[name], where, distinct=True, **kwargs1)


# @task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RDS,
#     cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=5),
#     )
def current_yearterm() -> tuple[str, str, pd.Timestamp, pd.Timestamp, str, str]:
    df = current_yearterm_df()
    return (df['year'].iloc[0], df['term'].iloc[0], df['start_of_term'].iloc[0], 
        df['end_of_term'].iloc[0], df['yearterm_sort'].iloc[0], df['yearterm'].iloc[0])


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RDS,
    cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=TASK_CEM),
    )
def current_yearterm_df() -> pd.DataFrame:
    """
    Function returns current year/term information based on today's date.

    Returns dataframe containing:
        term - string,
        year - string,
        yearterm - string,
        start_of_term - datetime,
        end_of_term - datetime,
        yearterm_sort - string
    """

    logger = get_run_logger()
    logger.debug(f"current_yearterm_df()")

    df_cal = ( select("ACADEMICCALENDAR", 
                    fields=['ACADEMIC_YEAR', 'ACADEMIC_TERM', 'ACADEMIC_SESSION', 
                            'START_DATE', 'END_DATE', 'FINAL_END_DATE'
                            ], 
                    where=f"ACADEMIC_YEAR>='{START_ACADEMIC_YEAR}' AND ACADEMIC_TERM IN ('FALL', 'SPRING', 'SUMMER')", 
                    distinct=True
                    )
              .groupby(['ACADEMIC_YEAR', 'ACADEMIC_TERM']).agg(
                  {'START_DATE': ['min'],
                   'END_DATE': ['max'],
                   'FINAL_END_DATE': ['max']
                  }
              ).reset_index()
             )
    df_cal.columns = df_cal.columns.droplevel(1)
    
    yearterm_sort = ( lambda r:
        r['ACADEMIC_YEAR'] + '01' if r['ACADEMIC_TERM']=='SPRING' else
        (r['ACADEMIC_YEAR'] + '02' if r['ACADEMIC_TERM']=='SUMMER' else
        (r['ACADEMIC_YEAR'] + '03' if r['ACADEMIC_TERM']=='FALL' else
        r['ACADEMIC_YEAR'] + '00'))
    )
    df_cal['yearterm_sort'] = df_cal.apply(yearterm_sort, axis=1)

    df_cal['yearterm'] = df_cal['ACADEMIC_YEAR'] + '.' +  df_cal['ACADEMIC_TERM'].str.title()

    df_cal = ( 
        df_cal.drop(
            columns=[
                'END_DATE'
                ]
            )
        .rename(
            columns={
                'ACADEMIC_YEAR': 'year', 
                'ACADEMIC_TERM': 'term', 
                'START_DATE': 'start_of_term', 
                'FINAL_END_DATE': 'end_of_term', 
                }
            )
        )

    return df_cal.loc[(df_cal['end_of_term'] >= dt.datetime.today())].sort_values(['end_of_term']).iloc[[0]]


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