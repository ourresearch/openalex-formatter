import csv
import json
import tempfile
from io import StringIO
from itertools import chain

from formats.util import paginate, get_nested_value, get_first_page, \
    unravel_index, truncate_format_row, build_dataframes, WORKS_DF_KEY, \
    join_lists


def build_single_df(export):
    dfs = build_dataframes(export)
    for k in dfs.keys():
        if k == WORKS_DF_KEY:
            dfs[k] = dfs[k].applymap(join_lists)
            continue
        dfs[k] = dfs[k].applymap(str).groupby('work_id').agg(
            lambda x: '|'.join(x)).rename(
            columns=lambda x: f'{k}.{x}' if x != 'work_id' else x)
        dfs[WORKS_DF_KEY] = dfs[WORKS_DF_KEY].merge(dfs[k],
                                                    how='left',
                                                    left_on='id',
                                                    right_on='work_id')
    return dfs[WORKS_DF_KEY]


def export_csv(export):
    csv_filename = tempfile.mkstemp(suffix='.csv')[1]
    df = build_single_df(export)
    with open(csv_filename, 'w') as csv_file:
        df.to_csv(csv_file, index=False)
    return csv_filename


def instant_export(export):
    buffer = StringIO()
    df = build_single_df(export)
    df.to_csv(buffer, index=False)
    return buffer.getvalue()
