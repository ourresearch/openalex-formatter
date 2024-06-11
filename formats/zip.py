import io
import tempfile
import zipfile

from formats.util import build_dataframes, join_lists


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


def export_zip(export):
    dfs = build_dataframes(export)
    zip_buffer = create_csv_zip_buffer(dfs)
    zip_filename = tempfile.mkstemp(suffix='.zip')[1]
    with open(zip_filename, 'wb') as zip_file:
        zip_file.write(zip_buffer.getvalue())
    return zip_filename
