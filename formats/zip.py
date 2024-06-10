import io
import itertools
import tempfile
import zipfile

from formats.util import paginate
import pandas as pd


def join_lists(cell):
    if isinstance(cell, list) and all(isinstance(i, (str, int, float)) for i in cell):
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


def write_dataframes(export):
    dfs = dict()
    works_csv_key = 'works'
    for page in paginate(export):
        df = pd.json_normalize(page)
        df.drop(
            columns=[col for col in df.columns if 'abstract_inverted' in col],
            inplace=True)
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
                sub_df = pd.json_normalize(
                    list(itertools.chain(*df[col].tolist())))
                sub_df.insert(0, 'work_id', df['id'])
                if col in dfs:
                    dfs[col] = pd.concat([dfs[col], sub_df],
                                         axis=0).reset_index(drop=True)
                else:
                    dfs[col] = sub_df
        df.drop([col for col in dfs.keys() if col in df.columns], axis=1, inplace=True)
    return create_csv_zip_buffer(dfs)


def export_zip(export):
    zip_buffer = write_dataframes(export)
    zip_filename = tempfile.mkstemp(suffix='.zip')[1]
    with open(zip_filename, 'wb') as zip_file:
        zip_file.write(zip_buffer.getvalue())
    return zip_filename
