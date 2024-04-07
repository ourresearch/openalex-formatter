import datetime
from math import ceil
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import requests

from app import db, logger

TRUNCATE_MAX_CHARS = 32_740


def update_export_progress(export, max_page, page):
    export.progress = page / max_page
    export.progress_updated = datetime.datetime.utcnow()
    db.session.merge(export)
    db.session.commit()


def construct_query_url(cursor, export, per_page):
    parsed_query_url = urlparse(export.query_url)
    query_args = parse_qs(parsed_query_url.query)
    query_args['cursor'] = cursor
    query_args['per_page'] = per_page
    parsed_query_url = parsed_query_url._replace(
        query=urlencode(query_args, doseq=True)
    )
    query_url = urlunparse(parsed_query_url)
    return query_url


def paginate(export, fname, max_page=250):
    page = 1
    cursor = '*'
    per_page = 200

    s = requests.session()

    while page <= max_page and cursor is not None:
        query_url = construct_query_url(cursor, export, per_page)
        response = s.get(query_url).json()
        max_page = min(ceil(response['meta']['count'] / per_page), max_page)
        cursor = response['meta']['next_cursor']

        yield response['results']

        update_export_progress(export, max_page, page)
        logger.info(f'wrote page {page} of {max_page} to {fname}')
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
        'per-page': '200'
    }
    return requests.get(export.query_url, params=params).json()


def truncate_format_str(cell_str):
    cell_str = cell_str[: TRUNCATE_MAX_CHARS - 3]
    if len(cell_str) >= TRUNCATE_MAX_CHARS - 3:
        cell_str += '...'
    return cell_str


def truncate_format_row(row):
    for k in row.keys():
        row[k] = truncate_format_str(row[k]) if isinstance(row[k], str) else row[k]
    return row
