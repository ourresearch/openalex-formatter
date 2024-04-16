import datetime
from math import ceil
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import requests
from requests import JSONDecodeError

from app import db, logger

TRUNCATE_MAX_CHARS = 32_740


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
    parsed_query_url = parsed_query_url._replace(
        query=urlencode(query_args, doseq=True)
    )
    query_url = urlunparse(parsed_query_url)
    return query_url


def paginate(export, fname, max_results=200*250):
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

        update_export_progress(export, results_count/total_count)
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
