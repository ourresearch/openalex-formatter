import io
import itertools
import tempfile
import zipfile

from formats.util import paginate
import pandas as pd


def join_lists(cell):
    if isinstance(cell, list) and all(
            isinstance(i, (str, int, float)) for i in cell):
        return '|'.join(cell)
    return cell


def create_csv_zip_buffer(fnames_df_map):
    zip_buffer = io.BytesIO()

    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for name, df in fnames_df_map.items():
            df = df.applymap(join_lists)
            csv_buffer = io.StringIO()
            fname = f'{name}.csv'
            df.to_csv(csv_buffer, index=False)
            zip_file.writestr(fname, csv_buffer.getvalue())

    zip_buffer.seek(0)
    return zip_buffer


def object_columns_select(export_columns):
    m = {}
    for column in export_columns:
        split = column.split('.', maxsplit=1)
        if len(split) == 2:
            m[split[0]] = split[1]
        else:
            m[split[0]] = ''
    return m


def set_work_ids(col_list, df):
    for i, _list in enumerate(col_list):
        for obj in _list:
            obj['work_id'] = df['id'].iloc[i]


def set_column_order(sub_df):
    front_cols = ['work_id']
    if 'id' in sub_df.columns:
        front_cols.insert(0, 'id')
    end_cols = [col for col in sub_df.columns if col not in front_cols]
    df = sub_df[front_cols + end_cols]
    return df


def write_dataframes(export):
    dfs = dict()
    works_csv_key = 'works'
    raw_columns = []
    columns_map = {}
    for page in paginate(export):
        df = pd.json_normalize(page)
        drop_columns = [col for col in df.columns if
                        'abstract_inverted' in col]
        df.drop(columns=drop_columns, inplace=True)
        if export.columns:
            raw_columns = export.columns.split(',')
            columns_map = object_columns_select(raw_columns)
            drop_columns = [col for col in df.columns if
                            col not in list(
                                columns_map.keys()) + raw_columns + ['id']]
            df.drop(columns=drop_columns, inplace=True)
        if works_csv_key not in dfs:
            dfs[works_csv_key] = df
        else:
            dfs[works_csv_key] = pd.concat([dfs[works_csv_key], df],
                                           axis=0).reset_index(drop=True)
        for col in df.columns:
            filtered_series = df[col].dropna().apply(
                lambda x: x if isinstance(x, list) else [])
            non_empty_lists = filtered_series[filtered_series.map(len) > 0]
            if not non_empty_lists.empty and isinstance(
                    non_empty_lists.iloc[0][0], dict):
                col_list_form = df[col].tolist()
                set_work_ids(col_list_form, df)
                sub_df = pd.json_normalize(
                    list(itertools.chain(*col_list_form)))
                sub_df = set_column_order(sub_df)
                if raw_columns:
                    drop_columns = [column for column in sub_df.columns if
                                    column not in [columns_map.get(col, [])] + [
                                        'work_id']]
                    sub_df.drop(columns=drop_columns, inplace=True)
                dfs[works_csv_key].drop(columns=[col], inplace=True)
                if col in dfs:
                    dfs[col] = pd.concat([dfs[col], sub_df],
                                         axis=0).reset_index(drop=True)
                else:
                    dfs[col] = sub_df
        drop_columns = [key for key in dfs.keys() if
                        key in dfs[works_csv_key].columns]
        if drop_columns:
            dfs[works_csv_key].drop(columns=drop_columns, inplace=True)
    if raw_columns:
        drop_columns = [col for col in dfs[works_csv_key].columns if
                        col not in raw_columns]
        dfs[works_csv_key].drop(columns=drop_columns, inplace=True)
    return create_csv_zip_buffer(dfs)


def export_zip(export):
    zip_buffer = write_dataframes(export)
    zip_filename = tempfile.mkstemp(suffix='.zip')[1]
    with open(zip_filename, 'wb') as zip_file:
        zip_file.write(zip_buffer.getvalue())
    return zip_filename
