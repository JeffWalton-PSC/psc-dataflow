import pandas as pd
from prefect import task, get_run_logger


@task
def deduplicate(df: pd.DataFrame, subset: list[str], keep: str ='first') -> pd.DataFrame:
    """
    Returns dataframe with rows deduplicated based on the subset of columns.
    """
    logger = get_run_logger()
    logger.debug(f"deduplicate({subset=}, {keep=})")
    return df.drop_duplicates(subset=subset, keep=keep)


@task
def filter_rows(df: pd.DataFrame, filter: str) -> pd.DataFrame:
    """
    Returns dataframe with rows that meet the conditions of filter.
    Refer to dataframe columns by their column names in the filter string.
    """
    logger = get_run_logger()
    logger.debug(f"filter_rows({filter=})")
    return df.query(filter)


@task
def keep_columns(df: pd.DataFrame, keep_cols: list[str]) -> pd.DataFrame:
    """
    Returns dataframe with only columns in keep_cols.
    """
    logger = get_run_logger()
    logger.debug(f"keep_columns({keep_cols=})")
    return df[keep_cols]


@task
def merge(df1: pd.DataFrame, on1: list[str], df2: pd.DataFrame, on2: list[str], how: str ='left', suffixes: list[str, str] =[None, '_y']) -> pd.DataFrame:
    """
    Returns dataframe two dataframes df1 and df2 joined by columns on1 and on2 with a 'how' type of merge.
    See pandas.DataFrame.merge in pandas docs.
    """
    logger = get_run_logger()
    logger.debug(f"merge({on1=}, {on2=})")
    return df1.merge(df2, how=how, left_on=on1, right_on=on2, suffixes=suffixes)


@task
def rename_columns(df: pd.DataFrame, name_map: dict[str, str]) -> pd.DataFrame:
    """
    Returns dataframe with columns renamed based on the mapping given in name_map.
    """
    logger = get_run_logger()
    logger.debug(f"rename_columns({name_map=})")
    return df.rename(columns=name_map)


@task
def sort_rows(df: pd.DataFrame, sort_by: list[str], asc: list[bool]=[]) -> pd.DataFrame:
    """
    Returns dataframe with rows sorted by the sort_by columns in order defined by asc.
    """
    logger = get_run_logger()
    logger.debug(f"sort_rows({sort_by=}, {asc=})")
    if not asc:
        asc = [ True for s in sort_by]
    return df.sort_values(sort_by, ascending=asc)


@task
def transform(df: pd.DataFrame, transform: dict[str, str]) -> pd.DataFrame:
    """
    Returns dataframe with new column (transform key) created from expression (transform value).
    Refer to dataframe columns by their column names in the transform value string.
    See pandas.DataFrame.eval in pandas docs.
    """
    logger = get_run_logger()
    logger.debug(f"transform({transform=})")
    for k, v in transform:
        df[k] = df.eval(df[v])
    return df


@task
def write_csv_textfile(df: pd.DataFrame, fn:str, ):
    """
    Writes dataframe to a csv text file with name fn.
    See pandas.DataFrame.to_csv in pandas docs.
    """
    logger = get_run_logger()
    logger.debug(f"write_csv_textfile({fn=})")
    df.to_csv(fn, index=False)
    return


