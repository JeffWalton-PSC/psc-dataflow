import datetime as dt
import numpy as np
import pandas as pd
import sys
from prefect import flow, get_run_logger

from src.dataframe.task import deduplicate, filter_rows, keep_columns, rename_columns, sort_rows, write_csv_textfile
from src.powercampus.flow import academic_table, building_table, code_day_table, sectionper_table, \
     sections_table, sectionschedule_table, testscores_table, transcriptdetail_table, transcriptgpa_table, \
     apply_active_with_email_address
from src.powercampus.task import latest_year_term
from src.starfish import BEGIN_YEAR, CATALOG_YEAR, N_YEARS_ACTIVE_WINDOW, starfish_workingfiles_path, starfish_prod_sisdatafiles_path
from src.starfish.task import copy_to_starfish_sisdatafiles


@flow()
def higherthan20gpa_student_prereq_groups(begin_year: str):
    """
    Outputs prereq group file for students with greater than a 2.0 GPA
    """

    logger = get_run_logger()
    logger.info(f"higherthan20gpa_student_prereq_groups({begin_year=})")

    output_path = starfish_workingfiles_path / "student_prereq_groups"
    fn_output = output_path / "higherthan20gpa_student_prereq_groups.txt"

    df = transcriptgpa_table(begin_year)
    df = filter_rows(df, filter="(RECORD_TYPE == 'O') & (TOTAL_CREDITS >= 0) ")

    # keep records for active students with email_address
    df = apply_active_with_email_address(N_YEARS_ACTIVE_WINDOW, in_df=df)

    # filter results to only have cumulative GPA's equal to or above a 2.0,
    #                   and Fall, Spring or Summer term
    df = df[(~df["GPA"].isnull())]
    df = df[(df["GPA"] >= 2)]
    df = df[df["ACADEMIC_TERM"].isin(["SPRING", "SUMMER", "FALL"])]

    # find the latest year
    df = latest_year_term(df)

    # create prereq group identifier
    df["prereq_group_identifier"] = "GPA_GT_2.0"

    df = rename_columns(df, name_map={"PEOPLE_CODE_ID": "student_integration_id"})
    df = keep_columns(df, keep_cols=["student_integration_id", "prereq_group_identifier"])
    df = sort_rows(df, sort_by=["student_integration_id", "prereq_group_identifier"])
    df = deduplicate(df, subset=["student_integration_id", "prereq_group_identifier"], keep="last")

    write_csv_textfile(df, fn_output)

    logger.info(f"End: higherthan20gpa_student_prereq_groups()")


@flow()
def classlevel_student_prereq_groups(begin_year: str):
    """
    Data file for Starfish's DegreePlanner.
    Creates pre-requisite groups based on class level (Freshman, Sophomore, Junior, Senior, Grad).
    """

    logger = get_run_logger()
    logger.info(f"classlevel_student_prereq_groups({begin_year=})")

    output_path = starfish_workingfiles_path / "student_prereq_groups"
    fn_output = output_path / "class-level_student_prereq_groups.txt"

    df_aca = academic_table(begin_year)
    df_aca = filter_rows(df_aca, filter="ACADEMIC_SESSION == '' & PRIMARY_FLAG == 'Y' & CREDITS > 0 ")
    df_aca = keep_columns(df_aca, keep_cols=[
        "PEOPLE_CODE_ID",
        "ACADEMIC_YEAR",
        "ACADEMIC_TERM",
        "ACADEMIC_SESSION",
        "CREDITS",
        "PRIMARY_FLAG",
        "CLASS_LEVEL",
        ]
    )
    df_aca = latest_year_term(df_aca)

    df_tgpa = transcriptgpa_table(begin_year)
    df_tgpa = filter_rows(df_tgpa, filter="(RECORD_TYPE == 'O') & (TOTAL_CREDITS >= 0) ")
    df_tgpa = latest_year_term(df_tgpa)

    df = pd.merge(df_aca, df_tgpa, on=["PEOPLE_CODE_ID"], how="left")

    # keep records for active students with email_address
    df = apply_active_with_email_address(N_YEARS_ACTIVE_WINDOW, in_df=df)
    
    df.loc[:, "prereq_group_identifier"] = "FRESHMAN"
    df.loc[(df["TOTAL_CREDITS"] >= 30), "prereq_group_identifier"] = "SOPHOMORE"
    df.loc[(df["TOTAL_CREDITS"] >= 60), "prereq_group_identifier"] = "JUNIOR"
    df.loc[(df["TOTAL_CREDITS"] >= 90), "prereq_group_identifier"] = "SENIOR"
    df.loc[(df["CLASS_LEVEL"] == 'GRAD'), "prereq_group_identifier"] = "GRAD"

    df = rename_columns(df, name_map={"PEOPLE_CODE_ID": "student_integration_id"})
    df = keep_columns(df, keep_cols=["student_integration_id", "prereq_group_identifier"])
    df = sort_rows(df, sort_by=["student_integration_id", "prereq_group_identifier"])
    df = deduplicate(df, subset=["student_integration_id", "prereq_group_identifier"], keep="last")

    write_csv_textfile(df, fn_output)
    logger.info(f"{fn_output} written.")

    logger.info(f"End: classlevel_student_prereq_groups()")


@flow()
def gened_student_prereq_groups(begin_year: str, catalog_year: str):
    """
    Data file for Starfish's DegreePlanner.
    Creates pre-requisite groups based on geneds (SC-I, WC-F, etc.).
    """
    logger = get_run_logger()
    logger.info(f"gened_student_prereq_groups({begin_year=})")

    output_path = starfish_workingfiles_path / "student_prereq_groups"
    fn_output = output_path / "gened_student_prereq_groups.txt"

    #create a passing grades dataframe
    passing_grades = ['A','A+','B','B+','C','C+','D','D+','P','TR']
    gen_ed_codes = ['WC-F','QP-F','AR-F','RE-F','SC-F','WC-R','QP-R','AR-R','RE-R','SC-R','WC-I','QP-I','AR-I','RE-I','SC-I']

    #readd in the gened courses for special topics
    st_gened_courses_fn = starfish_workingfiles_path / "student_prereq_groups\ST_GENED_COURSES.csv"
    st_gened_courses = pd.read_csv(st_gened_courses_fn)

    sections_fn = starfish_prod_sisdatafiles_path / "sections.txt"
    sections_df = pd.read_csv(sections_fn)
    sections_df = sections_df[['course_section_id','course_integration_id']]

    outcomes_fn = starfish_prod_sisdatafiles_path / "course_outcomes.txt"
    outcomes_df = pd.read_csv(outcomes_fn)

    outcomes_section_df = pd.merge(outcomes_df, sections_df, left_on='course_section_integration_id', right_on='course_section_id', how='left')
    outcomes_section_df = outcomes_section_df[['user_integration_id','course_section_integration_id','final_grade','course_integration_id']]

    req_course_sets_fn = starfish_prod_sisdatafiles_path / f"{catalog_year}_requirement_course_sets.txt"
    req_course_sets_df = pd.read_csv(req_course_sets_fn)

    student_transfer_records_fn = starfish_prod_sisdatafiles_path / "student_transfer_records.txt"
    student_transfer_records_df = pd.read_csv(student_transfer_records_fn)

    course_catalog_fn = starfish_prod_sisdatafiles_path / "course_catalog.txt"
    course_catalog_df = pd.read_csv(course_catalog_fn)

    student_transfer_df = pd.merge(student_transfer_records_df, 
                                course_catalog_df, 
                                left_on='transfer_course_number', 
                                right_on='course_id', 
                                how='left')
    student_transfer_df = student_transfer_df.rename(columns={"student_integration_id": "user_integration_id",
                                                            "transfer_course_section_number": "course_section_integration_id",
                                                            "ag_grade": "final_grade",
                                                            "integration_id": "course_integration_id",
                                                        }
                                                )
    # student_transfer_df = student_transfer_df[['user_integration_id', 
    #                                         'transfer_course_number', 
    #                                         'course_id', 
    #                                         'course_section_integration_id', 
    #                                         'final_grade', 
    #                                         'course_integration_id']]
    student_transfer_df = student_transfer_df[['user_integration_id', 
                                            'course_section_integration_id', 
                                            'final_grade', 
                                            'course_integration_id']]

    outcomes_section_df = pd.concat([outcomes_section_df, student_transfer_df], ignore_index=True, sort=True)

    outcomes_sections_sets_pd = pd.merge(outcomes_section_df, req_course_sets_df, on='course_integration_id', how='inner')
    outcomes_sections_sets_pd = outcomes_sections_sets_pd[['user_integration_id','set_abbreviation','final_grade']]

    #only keep passing grades and gened codes
    result_pd = outcomes_sections_sets_pd.query("final_grade in @passing_grades").query("set_abbreviation in @gen_ed_codes")

    #takes the st special topics courses and prepared to merge them with the outcomes file
    outcomes_sections_sets_pd_st = pd.merge(outcomes_section_df, st_gened_courses, on='course_section_integration_id', how='inner')

    #in st dataframe, only keep passing grades and gened codes
    result_pd_st = outcomes_sections_sets_pd_st.query("final_grade in @passing_grades").query("set_abbreviation in @gen_ed_codes")

    #append the st and standard data frames
    result_pd = pd.concat([result_pd, result_pd_st], ignore_index=True, sort=True)

    #remove the grade and ID column
    result_pd = result_pd[['user_integration_id','set_abbreviation']]
    result_pd = result_pd.rename(
        columns={
            "user_integration_id": "student_integration_id",
            "set_abbreviation": "prereq_group_identifier"
            }
        )

    #incorporate test scores for math scores over 199 to have met qp-f
    #student_integration_id,test_id,numeric_score,date_taken
    #P000024201,ACCUPLACER_ENGLISH,100,2010-07-09
    test_scores_fn = starfish_prod_sisdatafiles_path / "student_test_results.txt"
    test_scores = pd.read_csv(test_scores_fn)
    test_scores = test_scores[test_scores.test_id == 'ACCUPLACER_MATH']
    test_scores = test_scores[test_scores.numeric_score > 199]
    #Remove all of the columns except the student integration id and add a new column for qp-f
    test_scores = test_scores[['student_integration_id']]
    test_scores['prereq_group_identifier'] = 'QP-F'

    #appending test_scores to results_pd
    result_pd = pd.concat([result_pd, test_scores])

    #rename the headers to match the file requirements
    result_pd = result_pd.drop_duplicates(subset=['student_integration_id','prereq_group_identifier'])
    result_pd = result_pd.sort_values(['student_integration_id','prereq_group_identifier'])

    write_csv_textfile(result_pd, fn_output)

    logger.info(f"End: gened_student_prereq_groups()")


@flow()
def student_prereq_groups(begin_year: str, catalog_year: str):
    """
    Data file for Starfish's DegreePlanner.
    Creates pre-requisite groups based on geneds (SC-I, WC-F, etc.).
    """
    import os

    logger = get_run_logger()
    logger.info(f"student_prereq_groups()")

    higherthan20gpa_student_prereq_groups(begin_year)
    classlevel_student_prereq_groups(begin_year)
    gened_student_prereq_groups(begin_year, catalog_year)

    prereq_files_path = starfish_workingfiles_path / "student_prereq_groups"
    prereq_fn_output = prereq_files_path / "student_prereq_groups.txt"

    #find files that end with "student_prereq_groups.txt"
    prereq_input_files = []
    for root, dirs, files in os.walk(prereq_files_path):
        logger.info(f"{files=}")
        for file in files:
            if file.endswith("student_prereq_groups.txt") and file != "student_prereq_groups.txt":
                prereq_input_files.append(os.path.join(root, file))

    logger.info(f"{prereq_input_files=}")
    #combine them
    combined_csv = pd.concat( [ pd.read_csv(f) for f in prereq_input_files ] )
    #output to a new file
    write_csv_textfile(combined_csv, prereq_fn_output)
    logger.info(f"{prereq_fn_output} written.")

    # copy files to Starfish sisdatafiles directory
    copy_to_starfish_sisdatafiles(prereq_fn_output)
    
    logger.info(f"End: student_prereq_groups()")


@flow()
def student_test_results():
    """
    Data file for Starfish.
    Creates placement test scores file, student_test_results.txt, from PowerCampus.
    """

    logger = get_run_logger()
    logger.info(f"student_test_results()")

    output_path = starfish_workingfiles_path / "student_test_results"
    fn_output = output_path / "student_test_results.txt"

    df = testscores_table()

    df = df[df["TEST_DATE"].notnull()]
    df.loc[df["TEST_TYPE"]=="MATH", "test_id"] = "ACCUPLACER_MATH"
    df.loc[df["TEST_TYPE"]=="ENGL", "test_id"] = "ACCUPLACER_ENGLISH"
    df = df.loc[(df["CONVERTED_SCORE"].notna()),:]
    df["numeric_score"] = df["CONVERTED_SCORE"].apply(np.int64)
    df["date_taken"] = df["TEST_DATE"].dt.strftime("%Y-%m-%d")

    # keep records for active students with email_address
    df = apply_active_with_email_address(N_YEARS_ACTIVE_WINDOW, in_df=df)

    df = rename_columns(df, name_map={"PEOPLE_CODE_ID": "student_integration_id"})
    df = keep_columns(df, keep_cols=["student_integration_id", "test_id", "numeric_score", "date_taken"])

    df = sort_rows(df, sort_by=["student_integration_id", "test_id", "numeric_score"])
    df = deduplicate(df, subset=["student_integration_id", "test_id"], keep="last")

    #output to a new file
    write_csv_textfile(df, fn_output)
    logger.info(f"{fn_output} written.")

    # copy files to Starfish sisdatafiles directory
    copy_to_starfish_sisdatafiles(fn_output)
    
    logger.info(f"End: student_test_results()")


@flow()
def student_transfer_records():
    """
    Data file for Starfish.
    Creates student transfer records file, student_transfer_records.txt, from PowerCampus.
    """

    logger = get_run_logger()
    logger.info(f"student_transfer_records()")

    output_path = starfish_workingfiles_path / "student_transfer_records"
    fn_output = output_path / "student_transfer_records.txt"

    df = transcriptdetail_table()

    # keep records for active students with email_address
    df = apply_active_with_email_address(N_YEARS_ACTIVE_WINDOW, in_df=df)

    crs_id = (
        lambda c: (str(c["EVENT_ID"]).replace(" ", "") + str(c["EVENT_SUB_TYPE"]).upper())
        if ((c["EVENT_SUB_TYPE"] == "LAB") | (c["EVENT_SUB_TYPE"] == "SI"))
        else (str(c["EVENT_ID"]).replace(" ", ""))
    )
    df.loc[:, "transfer_course_number"] = df.apply(crs_id, axis=1)

    tr_section_id = (
        lambda c: (c["EVENT_ID"] + "." + c["EVENT_SUB_TYPE"] + ".Transfer")
        if ((c["ACADEMIC_YEAR"] == "1999") | (c["ACADEMIC_YEAR"] == "2004"))
        else (
            c["EVENT_ID"]
            + "."
            + c["EVENT_SUB_TYPE"]
            + "."
            + c["ACADEMIC_YEAR"]
            + "."
            + c["ACADEMIC_TERM"].title()
            + ".TR"
        )
    )
    df.loc[:, "transfer_course_section_number"] = df.apply(tr_section_id, axis=1)
    df.loc[:, "ag_grading_type"] = "P/F"
    df.loc[:, "ag_status"] = "TRANSFER"

    df = rename_columns(df, name_map={
            "PEOPLE_CODE_ID": "student_integration_id",
            "CREDIT": "credits",
            "EVENT_MED_NAME": "course_title",
            "ACADEMIC_YEAR": "term_year",
            "ACADEMIC_TERM": "term_season",
        }
    )

    tr_grade = lambda c: "P" if (c["FINAL_GRADE"] == "TR") else "NG"
    df.loc[:, "ag_grade"] = df.apply(tr_grade, axis=1)
    df = df[~df["ag_grade"].isnull()]

    df = keep_columns(df, keep_cols=[
        "student_integration_id",
        "transfer_course_number",
        "transfer_course_section_number",
        "ag_grade",
        "ag_grading_type",
        "ag_status",
        "credits",
        "course_title",
    ],
    )

    df = sort_rows(df, sort_by=["student_integration_id", "transfer_course_section_number"])
    df = deduplicate(df, subset=["student_integration_id", "transfer_course_section_number"], keep="last")

    #output to a new file
    write_csv_textfile(df, fn_output)
    logger.info(f"{fn_output} written.")

    # copy files to Starfish sisdatafiles directory
    copy_to_starfish_sisdatafiles(fn_output)
    
    logger.info(f"End: student_transfer_records()")


@flow()
def requirement_course_sets(catalog_year: str):
    """
    For Starfish Degree Planner - creates requirement_course_sets file.txt for ingestion into Starfish
    Reads import_csv_file and outputs output_csv_file - import_csv_file is created by registrar.
    genedcourses.csv should be derived from genedcourses.xlsx and catalog years should be kept seperated in the excel sheet to keep track
    New set titles can be added by adding a new column to the CSV file and adding the label and title to the first 2 rows 

    import_csv_file Example:
    COURSE,COURSE COMP,DEGREE_APPLICABL,LIBARTS,WC-F,QP-F,AR-F,RE-F,SC-F,WC-R,QP-R,AR-R,RE-R,SC-R,WC-I,QP-I,AR-I,RE-I,SC-I,BIOG_UD_BIO,ENVS_UD_SCIENCE,HHAE_ECO,HHAE_POLICY,COMM_COMMMETHODS,COMM_DIVERSITY,SCWL_SUSTAINABLE,SCWL_SOCIETY,RECR_RECREATION,RECR_DIVERSITY,RECR_EXPERIENTIA,RECR_NATURAL,PACM_HUMAN,PACM_NATURAL,NRCM_CULTURAL,NRCM_ECOSYSTEM,NRCM_NEGOTIATION,NRCM_ORGANISMS,NRCM_PRACTITIONE,NRCM_SOCIETY,HRTM_CUSTOMER,HRTM_DIVERSITY,HRTM_MANAGEMENT,FWSW_ZOOLOGY,FWSW_BOTANY,FWSW_ECOLOGY,FWSW_PHYSICAL,FWSW_POLICY,FWSW_WILDLIFE,FWSF_BIOLOGICAL,FWSF_HUMAN,FBIO_BIOLOGY,FSMT_CUSTOMER,FSMT_DIVERSITY,FSMT_MANAGEMENT,ENST_ENV_HUMAN,ENST_ENV_SOCIETY,ENST_ENV_SCIENCE,ENST_PRACTITIONE,ENST_SOCIETY,ECOR_HUMAN,BASM_MANAGEMENT,FORT_FORT,IS_BASM,IS_BASM_ONE,IS_BIOG,IS_COMM,IS_CASM,IS_CASM_ONE,IS_ECOR,IS_EBSB,IS_ENVS,IS_ENST,IS_FOR,IS_FW,IS_FSMT,IS_HRTM,IS_HRTM_ONE,IS_MNGT,IS_NRCM,IS_PACM,IS_PSYCH,IS_RECR,IS_SCWL,IS_AALM,IS_SURV,HRTM_SPAN,HRTM_FREN,HRTM_ITAL,SCM_PRAC,SCM_CLUSTER,CAPSTONE,MNGT_CORE,MNGT_ENTR,MNGT_SPORTS,BIO_MNR_UDELEC,BOT_MNR_ELEC,CBM_MNR_ELEC,EBM_MNR_ELEC,ECM_MNR_EI,ECM_MNR_C,ESM_MNR_ELEC,FRM_MNR_F,GIS_MNR_ELEC,MPP_MNR_ELEC,SCM_MNR_SP,SCM_MNR_ELEC
    COURSE,COURSE COMP,Degree Applicable,Liberal Arts and Science,Written Communication - Foundation,Quantitative Problem Solving - Foundation,Analytical Reasoning & Scientific Inquiry - Foundation,Responsibility & Expression - Foundation,Social & Cultural Engagement - Foundation,Written Communication - Reinforcing,Quantitative Problem Solving - Reinforcing,Analytical Reasoning & Scientific Inquiry - Reinforcing,Responsibility & Expression - Reinforcing,Social & Cultural Engagement - Reinforcing,Written Communication - Integrated,Quantitative Problem Solving - Integrated,Analytical Reasoning & Scientific Inquiry - Integrated,Responsibility & Expression - Integrated,Social & Cultural Engagement - Integrated,BIOG: Upper Division Biology Electives,ENVS: Upper Division Science Electives,Human Health - Ecosystem Processes Cluster,Human Health - Policy Cluster,COMM: Communication Methods Cluster,COMM: Diversity Cluster,SCWL: Sustainable Practitioner Cluster,SCWL: Society and Natural World Foundation Course,RECR: Recreation Management Cluster,RECR: Diversity Cluster,RECR: Experiential Cluster,RECR: Natural World Cluster,PACM: Human Dimension Cluster,PACM: Natural World Cluster,NRCM: Cultural Perspective Cluster,NRCM: Ecosystem Management Cluster,NRCM: Negotiation/Planning Cluster,NRCM: Organisms/Habitats Cluster,NRCM: Practitioner Skills Cluster,NRCM: Society and Natural World Foundation Course,HRTM: Customer Relations Cluster,HRTM: Diversity Cluster,HRTM: Management Cluster,FWSW - Wildlife Concentration: Zoology Elective,FWSW - Wildlife Concentration: Botany Elective,FWSW - Wildlife Concentration: Ecology Elective,FWSW - Wildlife Concentration: Physical Science Elective,"FWSW - Wildlife Concentration: Policy, Admin & Law Elective","FWSW - Wildlife Concentration: Policy, Admin & Law Elective",FWSW - Fisheries Concentration: Biological Science Elective,FWSW - Fisheries Concentration: Human Dimension Elective,Forestry - Biology Concentration: Biology Cluster,FSMT: Customer Relations Cluster,FSMT: Diversity Cluster,FSMT: Management Cluster,ENST: Environment & Human Expression Cluster,ENST: Environment & Society Cluster,ENST: Environment & Science Cluster,ENST: Practitioner Skills Cluster,ENST: Society and Natural World Foundation Course,ECOR: Human System Cluster,BASM: Management Cluster,FORT: Forest Technology Cluster,Integrative Studies: BASM Program Options,Integrative Studies: BASM Select One Program Options,Integrative Studies: BIOG Program Options,Integrative Studies: COMM Program Options,Integrative Studies: CASM Program Options,Integrative Studies: CASM Select One Program Options,Integrative Studies: ECOR Program Options,Integrative Studies: EBSB Program Options,Integrative Studies: ENVS Program Options,Integrative Studies: ENST Program Options,Integrative Studies: FOR Program Options,Integrative Studies: FW Program Options,Integrative Studies: FSMT Program Options,Integrative Studies: HRTM Program Options,Integrative Studies: HRTM Select One Program Options,Integrative Studies: MNGT Program Options,Integrative Studies: NRCM Program Options,Integrative Studies: PACM Program Options,Integrative Studies: PSYCH Program Options,Integrative Studies: RECR Program Options,Integrative Studies: SCWL Program Options,Integrative Studies: AALM Program Options,Integrative Studies: SURV Program Options,Language Sequence - Spanish,Language Sequence - French,Language Sequence - Italian,SCM: Sustainable Communities Minor Cluster,SCM: Sustainable Communities Minor - Practitioner Courses,Capstone Courses,Management Core,Entrepreneurship Core,Sports and Event Management Core,Biology Minor UD Electives,Botany Minor Electives,Craft Beer Studies Electives,Entrepreneurial Business Minor Electives,Environmental Communications Minor Electives,Environmental Studies Minor Communication Methods Electives,Environmental Studies Minor Electives,Forestry Minor Electives,Geographic Information Systems Minor Electives,Maple Production & Products Electives,Sustainable Communities Minor Electives,Sustainable Communities Minor Practitioner Electives
    Financial Accounting,ACC101.LEC.2010,X,,,,,,,,X,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,X,,,,X,,X,,,,X,,X,,,,,X,,X,X,,,,,,,,,,,,,,,,,,,,,,,,,,,,
    Managerial Accounting,ACC102.LEC.2010,X,,,,,,,,X,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,X,,,,,,,,,,,,X,,,,,,,,X,,,,,,,,,,,,,,,,,,,,X,,,,,,,,
    Small Business Accounting,ACC301.LEC.2011,X,,,,,,,,,,,,,,,,,,,,,,,,,X,,,,,,,,,,,,,,,,,,,,,,,,,,X,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,X,X,,,,,X,,,,,,,,
    Analytical Reasoning Foundational,AR100.2011,X,X,,,X,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,

    Description needs to be less than 64 characters
    Abreviation needs be less than 17
    """
    import csv

    logger = get_run_logger()
    logger.info(f"requirement_course_sets({catalog_year=})")

    # catalog_year uses catalog_year in the naming convention of the files to keep them segregated
    path = starfish_workingfiles_path / 'requirement_course_sets'
    # import_csv_file - this file is produced by exporting the appropriate csv from genedcourses.xlsx
    import_csv_file = path / 'requirements_course_sets_input.csv'
    # output_csv_file - this file is what is produced to be provided to starfish
    output_csv_file = path / f'{catalog_year}_requirement_course_sets.txt'

    with open (import_csv_file) as csvfile:
        freader = csv.reader(csvfile, delimiter=',', quotechar = '"')
        count = 0
        lines = []
        abbreviation_names = []
        title_names = []

        for row in freader:
            if count == 0:
                for x in range(0, len(row)):
                    abbreviation_names.append(row[x])
                #print(abbreviation_names)
            # populate set titles
            if count == 1:
                for x in range(0, len(row)):
                    title_names.append(row[x])
            else:
                #CourseID = row[1].replace(' ','')
                CourseID = row[1]
                for x in range(0, len(row)):
                    if row[x] == 'X':
                        lines.append([CourseID,catalog_year,abbreviation_names[x],title_names[x]])
            count += 1

    headers = ['course_integration_id','catalog_year','set_abbreviation','set_title']
    df = pd.DataFrame.from_records(lines, columns=headers)
    sorteddf = df.sort_values(by=['set_abbreviation','set_title'])
    sorteddf.to_csv(output_csv_file, sep=',', index=False)

    # copy files to Starfish sisdatafiles directory
    copy_to_starfish_sisdatafiles(output_csv_file)
    
    logger.info(f"End: requirement_course_sets()")


@flow()
def section_schedules(sections_begin_year: str) -> pd.DataFrame:
    """
    Creates the section_schedules.txt file for Starfish.
    """

    logger = get_run_logger()
    logger.info(f"Start: section_schedules({sections_begin_year=})")

    output_path = starfish_workingfiles_path / "section_schedules"
    fn_output = output_path / 'section_schedules.txt'

    df = sectionschedule_table(sections_begin_year)
    df = df[~(df['EVENT_ID'].str.contains('REG', case=False))]
    df = df[~(df['EVENT_ID'].str.contains('STDY', case=False))]
    df = (df.loc[(~df['EVENT_SUB_TYPE'].isin(['ACE', 'EXT', 'ONLN'])) &
                (~df['DAY'].isin(['TBD', 'ONLN', 'CANC'])) &
    #             (~df['BUILDING_CODE'].isin(['ONLINE'])) &
                (~df['BUILDING_CODE'].isnull())
                ]
        )
    df.loc[:, 'section_integration_id'] = (df['EVENT_ID'] + '.' +
                                        df['EVENT_SUB_TYPE'] + '.' +
                                        df['ACADEMIC_YEAR'] + '.' +
                                        df['ACADEMIC_TERM'].str.title() + '.' +
                                        df['SECTION']
                                        )

    # building codes
    building_codes = building_table()
    df = pd.merge(df, building_codes, on=['BUILDING_CODE'], how='left')
    df = df.rename(columns={
                            'BUILD_NAME_1': 'building',
                            'ROOM_ID': 'room',
                        })
    df['start_time'] = df.START_TIME.dt.strftime('%I:%M%p')
    df['end_time'] = df.END_TIME.dt.strftime('%I:%M%p')

    # day codes
    day_codes = code_day_table()
    day_func = (lambda c: (str(c['DAY_SORT']).replace('1', 'M')
                                            .replace('2', 'T')
                                            .replace('3', 'W')
                                            .replace('4', 'R')
                                            .replace('5', 'F')
                                            .replace('6', 'A')
                                            .replace('7', 'S')
                        )
            )
    day_codes.loc[:, 'meeting_days'] = day_codes.apply(day_func, axis=1)
    df = pd.merge(df, day_codes, left_on=['DAY'], right_on=['CODE_VALUE'], how='left')
    df = df.loc[:, ['section_integration_id', 'meeting_days',
                    'start_time', 'end_time',
                    'building', 'room', 
                ]]
    df = (df.sort_values(['section_integration_id', 
                        'meeting_days', 'start_time'])
            .drop_duplicates(['section_integration_id', 
                            'meeting_days', 'start_time'],
                            keep='last')
        )

    write_csv_textfile(df, fn_output)
    logger.info(f"{fn_output} written.")

    # copy files to Starfish sisdatafiles directory
    copy_to_starfish_sisdatafiles(fn_output)

    logger.info(f"End: section_schedules()")


@flow()
def sections(sections_begin_year: str) -> pd.DataFrame:
    """
    Creates the sections.txt file for Starfish.
    """
    # Modifications:
    #   20190530 (JTW): remove SESSION from term_id

    logger = get_run_logger()
    logger.info(f"Start: sections({sections_begin_year=})")

    output_path = starfish_workingfiles_path / "sections"
    sfn_output = output_path / "sections.txt"
    catalog_path = starfish_workingfiles_path / "course_catalog"
    catalog_fn = catalog_path / "course_catalog.txt"

    df = sections_table(sections_begin_year)
    df = df[~(df["EVENT_ID"].str.contains("REG", case=False))]
    df = df[~(df["EVENT_ID"].str.contains("STDY", case=False))]
    df = rename_columns(df, 
        name_map={
            "EVENT_MED_NAME": "course_section_name",
            "CREDITS": "credit_hours",
            "MAX_PARTICIPANT": "maximum_enrollment_count",
            "START_DATE": "start_dt",
            "END_DATE": "end_dt",
            "CIP_CODE": "course_cip_code",
        }
    )
    crs_id = (
        lambda c: (str(c["EVENT_ID"]).replace(" ", "") + str(c["EVENT_SUB_TYPE"]).upper())
        if ((c["EVENT_SUB_TYPE"] == "LAB") | (c["EVENT_SUB_TYPE"] == "SI"))
        else (str(c["EVENT_ID"]).replace(" ", ""))
    )
    df.loc[:, "course_id"] = df.apply(crs_id, axis=1)
    df.loc[:, "course_section_id"] = (
        df["EVENT_ID"]
        + "."
        + df["EVENT_SUB_TYPE"]
        + "."
        + df["ACADEMIC_YEAR"]
        + "."
        + df["ACADEMIC_TERM"].str.title()
        + "."
        + df["SECTION"]
    )
    df.loc[:, "integration_id"] = df.loc[:, "course_section_id"]
    term_id = (
        lambda c: (c["ACADEMIC_YEAR"] + "." + str(c["ACADEMIC_TERM"]).title())
        # if (c["ACADEMIC_SESSION"] == "MAIN")
        # else (
        #     c["ACADEMIC_YEAR"]
        #     + "."
        #     + str(c["ACADEMIC_TERM"]).title()
        #     + "."
        #     + c["ACADEMIC_SESSION"]
        # )
    )
    df.loc[:, "term_id"] = df.apply(term_id, axis=1)
    # temporarily use academic year as catalog year
    df["AY"] = (
        pd.to_numeric(df["ACADEMIC_YEAR"], errors="coerce")
        .fillna(sections_begin_year)
        .astype(np.int64)
    )
    cat_yr = lambda c: c["AY"] if (c["ACADEMIC_TERM"] == "FALL") else (c["AY"] - 1)
    df.loc[:, "catalog_year"] = df.apply(cat_yr, axis=1)
    crs_sect_delv = (
        lambda c: "03"
        if str(c["SECTION"])[:2] == "HY"
        else ("02" if str(c["SECTION"])[:2] == "ON" else "01")
    )
    df.loc[:, "course_section_delivery"] = df.apply(crs_sect_delv, axis=1)
    crs_integ_id = (
        lambda c: (c["EVENT_ID"] + "." + str(c["catalog_year"]))
        if (c["EVENT_SUB_TYPE"] == "")
        else (c["EVENT_ID"] + "." + c["EVENT_SUB_TYPE"] + "." + str(c["catalog_year"]))
    )
    df.loc[:, "course_integration_id"] = df.apply(crs_integ_id, axis=1)
    # read course_catalog.txt to find the correct catalog year
    dfcat = pd.read_csv(catalog_fn)
    dfcat = dfcat[["course_id", "integration_id"]].rename(
        {"integration_id": "cat_integ_id"}, axis="columns"
    )
    df = pd.merge(df, dfcat, on=["course_id"], how="left")
    # keep catalog_year before course year
    df = df.loc[(df["course_integration_id"] >= df["cat_integ_id"])]
    df = df.sort_values(
        ["course_section_id", "course_integration_id"], ascending=[True, True]
    ).drop_duplicates(["course_section_id"], keep="last")
    df.loc[:, "course_integration_id"] = df.loc[:, "cat_integ_id"]
    # save sections for teaching.txt below
    dfs = df.copy()
    df = keep_columns(df,
        keep_cols= [
            "integration_id",
            "course_section_name",
            "course_section_id",
            "start_dt",
            "end_dt",
            "term_id",
            "course_integration_id",
            "course_section_delivery",
            "maximum_enrollment_count",
            "credit_hours",
        ]    )
    df = sort_rows(df, sort_by=["integration_id"])
    write_csv_textfile(df, sfn_output)
    logger.info(f"{sfn_output} written.")

    # copy files to Starfish sisdatafiles directory
    copy_to_starfish_sisdatafiles(sfn_output)

    logger.info(f"End: sections()")

    return dfs


@flow()
def teaching(sections_begin_year: str):
    """
    Creates the teaching.txt file for Starfish.
    """

    logger = get_run_logger()
    logger.info(f"Start: teaching({sections_begin_year=}, sections_df)")

    output_path = starfish_workingfiles_path / "sections"
    tfn_output = output_path / "teaching.txt"

    sections_df = sections(sections_begin_year)
    sp = sectionper_table(sections_begin_year)
    dft = pd.merge(
        sections_df,
        sp,
        on=[
            "ACADEMIC_YEAR",
            "ACADEMIC_TERM",
            "ACADEMIC_SESSION",
            "EVENT_ID",
            "EVENT_SUB_TYPE",
            "SECTION",
        ],
        how="left",
    )
    dft = dft[~dft["PERSON_CODE_ID"].isnull()]
    dft = keep_columns(dft, keep_cols=["course_section_id", "PERSON_CODE_ID"])
    dft = rename_columns(dft, 
        name_map={
            "course_section_id": "course_section_integration_id",
            "PERSON_CODE_ID": "user_integration_id",
        }   )
    dft.loc[:, "user_role"] = "INSTRUCTOR"
    dft.loc[:, "available_ind"] = "1"
    dft = sort_rows(dft, sort_by=["course_section_integration_id", "user_integration_id"])
    write_csv_textfile(dft, tfn_output)
    logger.info(f"{tfn_output} written.")

    # copy files to Starfish sisdatafiles directory
    copy_to_starfish_sisdatafiles(tfn_output)

    logger.info(f"End: teaching()")



@flow()
def starfish_flow(academic_year: str, academic_term: str):


    logger = get_run_logger()
    logger.info(f"Start: starfish_flow({academic_year=}, {academic_term=})")

    # requirement_course_sets(CATALOG_YEAR)
    # teaching(BEGIN_YEAR)
    # student_test_results()
    # student_transfer_records()
    # section_schedules(BEGIN_YEAR)
    # student_prereq_groups(BEGIN_YEAR, CATALOG_YEAR)

    logger.info(f"End: starfish_flow()")


if __name__ == "__main__":
    academic_year = sys.argv[1]
    academic_term = sys.argv[2]
    starfish_flow(academic_year, academic_term)

