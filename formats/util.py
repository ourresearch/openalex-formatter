import datetime
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from app import db


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
