import datetime
import itertools
from math import ceil
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import pandas as pd
import requests
from requests import JSONDecodeError

from app import db, logger, openalex_api_key

TRUNCATE_MAX_CHARS = 30_000

WORKS_DF_KEY = 'works'


def update_export_progress(export, progress):
    export.progress = progress
    export.progress_updated = datetime.datetime.utcnow()
    db.session.merge(export)
    db.session.commit()


def construct_query_url(cursor, export, per_page):
    parsed_query_url = urlparse(export.query_url)
    query_args = parse_qs(parsed_query_url.query)
    query_args['cursor'] = cursor
    query_args['per_page'] = per_page
    query_args['api-key'] = openalex_api_key

    parsed_query_url = parsed_query_url._replace(
        query=urlencode(query_args, doseq=True)
    )
    query_url = urlunparse(parsed_query_url)
    return query_url


def paginate(export, fname=None, max_results=200 * 250):
    page = 1
    cursor = '*'
    per_page = 200
    results_count = 0

    s = requests.session()

    while results_count <= max_results and cursor is not None:
        query_url = construct_query_url(cursor, export, per_page)
        try:
            r = s.get(query_url)
            j = r.json()
        except JSONDecodeError:
            per_page = ceil(per_page / 2)
            continue
        per_page = min(200, per_page * 2)
        total_count = j['meta']['count']
        cursor = j['meta']['next_cursor']
        results = j['results']
        results_count += len(results)

        yield results

        if not export.args.get('is_async'):
            update_export_progress(export, 1)
            break

        # Update progress after every page for best user experience
        percent_complete = results_count / total_count if total_count > 0 else 1
        update_export_progress(export, percent_complete)
        if fname:
            logger.info(f'wrote {results_count}/{total_count} to {fname}')
        page += 1


def get_nested_value(work, *keys):
    for key in keys:
        if work is None or not isinstance(work, dict):
            return None
        work = work.get(key)
    return work


def parse_bool(s):
    if s.lower() in ["true", "yes", "t", "on", "1"]:
        return True
    elif s.lower() in ["false", "no", "f", "off", "0"]:
        return False
    else:
        raise ValueError("Invalid boolean value: {}".format(s))


def unravel_index(inverted_index):
    unraveled = {}
    for key, values in inverted_index.items():
        for value in values:
            unraveled[value] = key

    sorted_unraveled = dict(sorted(unraveled.items()))
    result = " ".join(sorted_unraveled.values()).replace("\n", "")
    return result


def get_first_page(export):
    params = {
        'page': '1',
        'per-page': '200',
        'api-key': openalex_api_key,

    }
    return requests.get(export.query_url, params=params).json()


def truncate_format_str(cell_str):
    cell_str = cell_str[: TRUNCATE_MAX_CHARS - 3]
    if len(cell_str) >= TRUNCATE_MAX_CHARS - 3:
        cell_str += '...'
    return cell_str


def truncate_format_row(row):
    for k in row.keys():
        row[k] = truncate_format_str(row[k]) if isinstance(row[k], str) else \
            row[k]
    return row


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


def truncate_string(x, max_len=TRUNCATE_MAX_CHARS):
    return x[:max_len] if isinstance(x, str) else x


def join_lists(cell):
    if isinstance(cell, list) and all(
            isinstance(i, (str, int, float)) for i in cell):
        return '|'.join(cell)
    return cell


def reconstruct_abstract(row, inverted_columns):
    word_positions = {}
    for col in inverted_columns:
        word = col.split('.', maxsplit=1)[-1]
        indexes = row[col]
        if not isinstance(indexes, list):
            continue
        for index in indexes:
            word_positions[index] = word
    # Create a list of words in correct order
    max_index = max(word_positions.keys(), default=-1)
    abstract = [word_positions.get(i, '') for i in range(max_index + 1)]
    return ' '.join(abstract)


def build_dataframes(export):
    dfs = dict()
    raw_columns = ['id']
    columns_map = {}
    for page in paginate(export):
        df = pd.json_normalize(page)
        drop_columns = [col for col in df.columns if
                        'abstract_inverted' in col]
        if 'open_access.is_oa' in df.columns:
            df['abstract'] = df.apply(reconstruct_abstract,
                                      inverted_columns=drop_columns,
                                      axis=1)
            df.loc[~df['open_access.is_oa'], 'abstract'] = ''
        df.drop(columns=drop_columns, inplace=True)
        export_cols = export.args.get('columns')
        if export_cols:
            raw_columns.extend(export_cols.split(','))
            columns_map = object_columns_select(raw_columns)
            drop_columns = [col for col in df.columns if
                            col not in list(
                                columns_map.keys()) + raw_columns]
            df.drop(columns=drop_columns, inplace=True)
        if WORKS_DF_KEY not in dfs:
            dfs[WORKS_DF_KEY] = df
        else:
            dfs[WORKS_DF_KEY] = pd.concat([dfs[WORKS_DF_KEY], df],
                                          axis=0).reset_index(drop=True)
        for col in df.columns:
            filtered_series = df[col].dropna().apply(
                lambda x: x if isinstance(x, list) else [])
            non_empty_lists = filtered_series[filtered_series.map(len) > 0]
            if not non_empty_lists.empty and isinstance(
                    non_empty_lists.iloc[0][0], dict):
                col_list_form = df[col].apply(lambda x: x if isinstance(x, list) else []).tolist()
                set_work_ids(col_list_form, df)
                sub_df = pd.json_normalize(
                    list(itertools.chain(*col_list_form)))
                sub_df = set_column_order(sub_df)
                if export.args.get('columns'):
                    drop_columns = [column for column in sub_df.columns if
                                    column not in [columns_map.get(col, [])] + [
                                        'work_id']]
                    sub_df.drop(columns=drop_columns, inplace=True)
                dfs[WORKS_DF_KEY].drop(columns=[col], inplace=True)
                if col in dfs:
                    dfs[col] = pd.concat([dfs[col], sub_df],
                                         axis=0).reset_index(drop=True)
                else:
                    dfs[col] = sub_df
        drop_columns = [key for key in dfs.keys() if
                        key in dfs[WORKS_DF_KEY].columns]
        if drop_columns:
            dfs[WORKS_DF_KEY].drop(columns=drop_columns, inplace=True)
    if export.args.get('columns'):
        drop_columns = [col for col in dfs[WORKS_DF_KEY].columns if
                        col not in raw_columns]
        dfs[WORKS_DF_KEY].drop(columns=drop_columns, inplace=True)
    if export.args.get('truncate'):
        for k in dfs.keys():
            dfs[k] = dfs[k].applymap(truncate_string)
    return dfs
