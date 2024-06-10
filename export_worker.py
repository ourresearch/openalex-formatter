import datetime
import os
from time import sleep, time

import boto3
import sentry_sdk
from sqlalchemy import text

from app import app, supported_formats
from app import app_url
from app import db, logger
from formats.csv import export_csv
from formats.group_bys import export_group_bys_csv
from formats.ris import export_ris
from formats.wos_plaintext import export_wos
from formats.zip import export_zip
from models import Export

from util import elapsed

sentry_sdk.init(dsn=os.environ.get('SENTRY_DSN'),)


def worker_run():
    while True:
        if export_id := fetch_export_id():
            if not (export := Export.query.get(export_id)):
                # not sure how this happened, but not much we can do
                continue

            if export.format == 'csv':
                filename = export_csv(export)
            elif export.format == 'wos-plaintext':
                filename = export_wos(export)
            elif export.format == "group-bys-csv":
                filename = export_group_bys_csv(export)
            elif export.format == 'ris':
                filename = export_ris(export)
            elif export.format == "zip":
                filename = export_zip(export)
            else:
                raise ValueError(f'unknown format {export.format}')

            file_format = supported_formats[export.format]
            s3_client = boto3.client('s3')
            s3_client.upload_file(filename, 'openalex-query-exports', f'{export_id}.{file_format}')
            s3_object_name = f's3://openalex-query-exports/{export_id}.{file_format}'

            logger.info(f'uploaded {filename} to {s3_object_name}')
            export.result_url = f'{app_url}/export/{export.id}/download'
            export.status = 'finished'
            export.progress = 1
            export.progress_updated = datetime.datetime.utcnow()
            db.session.merge(export)
            db.session.commit()
        else:
            sleep(1)


def fetch_export_id():
    logger.info("looking for new jobs")

    fetch_query = text("""
        with fetched_export as (
            select id
            from export
            where status = 'submitted'
            order by submitted
            limit 1
            for update skip locked
        )
        update export
        set status = 'running', progress_updated = now()
        from fetched_export
        where export.id = fetched_export.id
        returning fetched_export.id;
    """)

    job_time = time()
    with db.engine.connect() as connection:
        result = connection.execute(fetch_query.execution_options(autocommit=True))
        export_id = result.scalar()
        connection.commit()
    logger.info(f'fetched export {export_id}, took {elapsed(job_time)} seconds')
    return export_id


if __name__ == "__main__":
    with app.app_context():
        worker_run()

