import csv
import datetime
import tempfile
from math import ceil
from time import sleep, time

import boto3
import requests
from sqlalchemy import text

from app import app_url
from app import db, logger
from csv_export import CsvExport
from util import elapsed

CSV_FIELDS = [
    'id',
    'display_name',
    'publication_date',
    'relevance_score',
    'host_venue_id',
    'host_venue_display_name',
    'host_venue_publisher',
    'host_venue_issns',
    'host_venue_issn_l',
    'host_venue_type',
    'host_venue_url',
    'host_venue_is_oa',
    'host_venue_version',
    'host_venue_license',
    'author_ids'
    'author_names',
    'author_orcids',
    'author_institutions',
    'concept_ids',
    'alternate_host_venue_ids',
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
    'referenced_works',
    'related_works',
    'biblio_issue',
    'biblio_first_page',
    'biblio_volume',
    'biblio_last_page',
]


def worker_run():
    while True:
        if export_id := fetch_export_id():
            if not (export := CsvExport.query.get(export_id)):
                # not sure how this happened, but not much we can do
                continue

            csv_filename = export_csv(export)
            s3_client = boto3.client('s3')
            s3_client.upload_file(csv_filename, 'openalex-query-exports', f'{export_id}.csv')
            s3_object_name = f's3://openalex-query-exports/{export_id}.csv'

            logger.info(f'uploaded {csv_filename} to {s3_object_name}')
            export.result_url = f'{app_url}/export/{export.id}/download'
            export.status = 'finished'
            export.progress_updated = datetime.datetime.utcnow()
            db.session.merge(export)
            db.session.commit()
        else:
            sleep(1)


def export_csv(export):
    csv_filename = tempfile.mkstemp(suffix='.csv')[1]
    with open(csv_filename, 'w') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDS)
        writer.writeheader()

        page = 1
        per_page = 50
        max_page = 200

        while page <= max_page:
            query_url = f'{export.query_url}&page={page}&per_page={per_page}'
            result = requests.get(query_url).json()
            max_page = min(ceil(result['meta']['count'] / per_page), 200)

            export.progress = page / max_page
            export.progress_updated = datetime.datetime.utcnow()
            db.session.merge(export)
            db.session.commit()

            for work in result['results']:
                writer.writerow(row_dict(work))

            logger.info(f'wrote page {page} of {max_page} to {csv_filename}')
            page += 1

    return csv_filename


def row_dict(work):
    return {
        'id': work.get('id'),
        'display_name': work.get('display_name'),
        'publication_date': work.get('publication_date'),
        'relevance_score': work.get('relevance_score'),
        'host_venue_id': work.get('host_venue', {}).get('id'),
        'host_venue_display_name': work.get('host_venue', {}).get('display_name'),
        'host_venue_publisher': work.get('host_venue', {}).get('publisher'),
        'host_venue_issns': '|'.join(work.get('host_venue', {}).get('issn') or []),
        'host_venue_issn_l': work.get('host_venue', {}).get('issn_l'),
        'host_venue_type': work.get('host_venue', {}).get('type'),
        'host_venue_url': work.get('host_venue', {}).get('url'),
        'host_venue_is_oa': work.get('host_venue', {}).get('is_oa'),
        'host_venue_version': work.get('host_venue', {}).get('version'),
        'host_venue_license': work.get('host_venue', {}).get('license'),
    }


def fetch_export_id():
    logger.info("looking for new jobs")

    fetch_query = text("""
        with fetched_export as (
            select id
            from csv_export
            where status = 'submitted'
            order by submitted
            limit 1
            for update skip locked
        )
        update csv_export
        set status = 'running', progress_updated = now()
        from fetched_export
        where csv_export.id = fetched_export.id
        returning fetched_export.id;
    """)

    job_time = time()
    export_id = db.engine.execute(fetch_query.execution_options(autocommit=True)).scalar()
    logger.info(f'fetched export {export_id}, took {elapsed(job_time)} seconds')
    return export_id


if __name__ == "__main__":
    worker_run()