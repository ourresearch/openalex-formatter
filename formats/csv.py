import csv
import datetime
import tempfile
from math import ceil
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import requests

from app import db, logger

CSV_FIELDS = [
    'id',
    'display_name',
    'publication_date',
    'relevance_score',
    'primary_location_id',
    'primary_location_display_name',
    'primary_location_host_organization',
    'primary_location_issns',
    'primary_location_issn_l',
    'primary_location_type',
    'primary_location_landing_page_url',
    'primary_location_pdf_url',
    'primary_location_is_oa',
    'primary_location_version',
    'primary_location_license',
    'author_ids',
    'author_names',
    'author_orcids',
    'author_institution_ids',
    'author_institution_names',
    'is_oa',
    'oa_status',
    'oa_url',
    'cited_by_count',
    'doi',
    'mag',
    'pmid',
    'pmcid',
    'publication_year',
    'cited_by_api_url',
    'type',
    'is_paratext',
    'is_retracted',
    'biblio_issue',
    'biblio_first_page',
    'biblio_volume',
    'biblio_last_page',
    'referenced_works',
    'related_works',
    'concept_ids',
]


def get_nested_value(work, *keys):
    for key in keys:
        if work is None or not isinstance(work, dict):
            return None
        work = work.get(key)
    return work


def get_nested_issns(work):
    issns = get_nested_value(work, 'primary_location', 'source', 'issn')
    return '|'.join(issns) if issns else ""


def row_dict(work):
    primary_location = get_nested_value(work, 'primary_location')
    primary_location_source = get_nested_value(primary_location, 'source')
    return {
        'id': work.get('id'),
        'display_name': work.get('display_name'),
        'publication_date': work.get('publication_date'),
        'relevance_score': work.get('relevance_score'),
        'primary_location_id': get_nested_value(primary_location_source, 'id'),
        'primary_location_display_name': get_nested_value(primary_location_source, 'display_name'),
        'primary_location_host_organization': get_nested_value(primary_location_source, 'host_organization_name'),
        'primary_location_issns': get_nested_issns(work),
        'primary_location_issn_l': get_nested_value(primary_location_source, 'issn_l'),
        'primary_location_type': get_nested_value(primary_location_source, 'type'),
        'primary_location_landing_page_url': get_nested_value(primary_location, 'landing_page_url'),
        'primary_location_pdf_url': get_nested_value(primary_location, 'pdf_url'),
        'primary_location_is_oa': get_nested_value(primary_location, 'is_oa'),
        'primary_location_version': get_nested_value(primary_location, 'version'),
        'primary_location_license': get_nested_value(primary_location, 'license'),
        'author_ids': authors_pipe_string(work, 'id'),
        'author_names': authors_pipe_string(work, 'display_name'),
        'author_orcids': authors_pipe_string(work, 'orcid'),
        'author_institution_ids': institutions_pipe_string(work, 'id'),
        'author_institution_names': institutions_pipe_string(work, 'display_name'),
        'is_oa': (work.get('open_access') or {}).get('is_oa'),
        'oa_status': (work.get('open_access') or {}).get('oa_status'),
        'oa_url': (work.get('open_access') or {}).get('oa_url'),
        'cited_by_count': work.get('cited_by_count'),
        'doi': (work.get('ids') or {}).get('doi'),
        'mag': (work.get('ids') or {}).get('mag'),
        'pmid': (work.get('ids') or {}).get('pmid'),
        'pmcid': (work.get('ids') or {}).get('pmcid'),
        'publication_year': work.get('publication_year'),
        'cited_by_api_url': work.get('cited_by_api_url'),
        'type': work.get('type'),
        'is_paratext': work.get('is_paratext'),
        'is_retracted': work.get('is_retracted'),
        'biblio_issue': (work.get('biblio') or {}).get('issue'),
        'biblio_first_page': (work.get('biblio') or {}).get('first_page'),
        'biblio_volume': (work.get('biblio') or {}).get('volume'),
        'biblio_last_page': (work.get('biblio') or {}).get('last_page'),
        'referenced_works': '|'.join(work.get('referenced_works') or []),
        'related_works': '|'.join(work.get('related_works') or []),
        'concept_ids': '|'.join([(c.get('id') or '') for c in (work.get('concepts') or [])]),
    }


def authors_pipe_string(work, field_name):
    return '|'.join([
        ((a.get('author') or {}).get(field_name) or '').replace('|', '')
        for a in (work.get('authorships') or [])
    ])


def institutions_pipe_string(work, field_name):
    return '|'.join([
        ((a.get('institutions') or [{}])[0].get(field_name) or '').replace('|', '')
        for a in (work.get('authorships') or [])
    ])


def export_csv(export):
    csv_filename = tempfile.mkstemp(suffix='.csv')[1]
    with open('myfile.csv', 'w') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDS)
        writer.writeheader()

        page = 1
        cursor = '*'
        per_page = 200
        max_page = 500

        while page <= max_page and cursor is not None:
            parsed_query_url = urlparse(export.query_url)
            query_args = parse_qs(parsed_query_url.query)
            query_args['cursor'] = cursor
            query_args['per_page'] = per_page
            parsed_query_url = parsed_query_url._replace(
                query=urlencode(query_args, doseq=True)
            )

            query_url = urlunparse(parsed_query_url)

            result = requests.get(query_url).json()
            max_page = min(ceil(result['meta']['count'] / per_page), max_page)
            cursor = result['meta']['next_cursor']

            export.progress = page / max_page
            export.progress_updated = datetime.datetime.utcnow()
            db.session.merge(export)
            db.session.commit()

            for work in result['results']:
                writer.writerow(row_dict(work))

            logger.info(f'wrote page {page} of {max_page} to {csv_filename}')
            page += 1

    return csv_filename