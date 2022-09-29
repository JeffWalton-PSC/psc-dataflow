import pandas as pd
from prefect import task, get_run_logger


@task
def keep_columns(df: pd.DataFrame, keep_cols: list[str]):
    """
    Returns dataframe with only columns in keep_cols.
    """
    return df[keep_cols]


@task
def rename_columns(df: pd.DataFrame, name_map: dict[str, str]):
    """
    Returns dataframe with columns renamed based on the mapping given in name_map.
    """
    return df.rename(columns=name_map)


@task
def sort_rows(df: pd.DataFrame, sort_by: list[str], asc: list[bool]):
    """
    Returns dataframe with rows sorted by the sort_by columns in order defined by asc.
    """
    if not asc:
        asc = [ True for s in sort_by]
    return df.sort_values(sort_by, ascending=asc)


@task
def filter_rows(df: pd.DataFrame, filter: str):
    """
    Returns dataframe with rows that meet the conditions of filter.
    Refer to the dataframe as df[] in the filter string.
    """
    return df.loc[filter,:]


@task
def transform(df: pd.DataFrame, transform: dict[str, str]):
    """
    Returns dataframe with new column (transform key) created from expression (transform value).
    Refer to other dataframe columns by their column names in the transform value string.
    See pandas.DataFrame.eval in pandas docs.
    """
    for k, v in transform:
        df[k] = df.eval(df[v])
    return df


@task
def merge(df1: pd.DataFrame, on1: str, df2: pd.DataFrame, on2: str, how: str ='left'):
    """
    Returns dataframe two dataframes df1 and df2 joined by columns on1 and on2 with a 'how' type of merge.
    See pandas.DataFrame.merge in pandas docs.
    """
    return df1.merge(df2, how=how, left_on=on1, right_on=on2)


@task
def write_csv_textfile(df: pd.DataFrame, fn:str, ):
    """
    Writes dataframe to a csv text file with name fn.
    See pandas.DataFrame.to_csv in pandas docs.
    """
    return df.to_csv(fn)


