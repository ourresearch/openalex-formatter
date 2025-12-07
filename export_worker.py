import datetime
import os
from time import sleep, time

import boto3
import sentry_sdk
from sqlalchemy import text

from app import app, supported_formats
from app import app_url
from app import db, logger
from app import EXPORT_TABLE
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
        export_id = None
        job_start_time = None
        try:
            if export_id := fetch_export_id():
                job_start_time = time()

                if not (export := Export.query.get(export_id)):
                    # not sure how this happened, but not much we can do
                    logger.error(f'export {export_id} not found in database')
                    continue

                logger.info(f'processing export {export_id} (format: {export.format})')

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

                if not filename.startswith('s3://'):
                    file_format = supported_formats[export.format]
                    s3_client = boto3.client('s3')
                    s3_client.upload_file(filename, 'openalex-query-exports', f'{export_id}.{file_format}')
                    s3_object_name = f's3://openalex-query-exports/{export_id}.{file_format}'

                    # Clean up temp file after upload
                    try:
                        os.remove(filename)
                        logger.info(f'cleaned up temp file {filename}')
                    except Exception as cleanup_error:
                        logger.warning(f'failed to clean up temp file {filename}: {cleanup_error}')
                else:
                    s3_object_name = filename

                logger.info(f'uploaded {filename} to {s3_object_name}')
                export.result_url = f'{app_url}/export/{export.id}/download'
                export.status = 'finished'
                export.progress = 1
                export.progress_updated = datetime.datetime.utcnow()
                db.session.merge(export)
                db.session.commit()

                # Log successful completion with timing
                total_time = elapsed(job_start_time) if job_start_time else 'unknown'
                logger.info(f'successfully completed export {export_id} in {total_time} seconds')
            else:
                sleep(1)
        except Exception as e:
            logger.error(f'error processing export {export_id}: {e}', exc_info=True)
            sentry_sdk.capture_exception(e)

            # Mark the job as failed so it doesn't stay stuck in 'running'
            if export_id:
                try:
                    export = Export.query.get(export_id)
                    if export:
                        export.status = 'failed'
                        export.progress_updated = datetime.datetime.utcnow()
                        db.session.merge(export)
                        db.session.commit()
                        logger.info(f'marked export {export_id} as failed')
                except Exception as inner_e:
                    logger.error(f'error marking export {export_id} as failed: {inner_e}')
                    sentry_sdk.capture_exception(inner_e)

            # Continue processing other jobs instead of crashing
            sleep(1)

last_log_time = 0
def fetch_export_id():
    global last_log_time
    last_log_time = time() if time() - last_log_time >= 60 and logger.info("looking for jobs to process") is None else last_log_time

    fetch_query = text(f"""
        with fetched_export as (
            select id
            from """ + EXPORT_TABLE + """
            where status = 'submitted'
            order by submitted
            limit 1
            for update skip locked
        )
        update """ + EXPORT_TABLE + """
        set status = 'running', progress_updated = now()
        from fetched_export
        where """ + EXPORT_TABLE + """.id = fetched_export.id
        returning fetched_export.id;
    """)

    job_time = time()
    with db.engine.begin() as connection:
        result = connection.execute(fetch_query)
        export_id = result.scalar()

    if export_id:
        logger.info(f'fetched export {export_id}, took {elapsed(job_time)} seconds')

    return export_id


if __name__ == "__main__":
    with app.app_context():
        worker_run()
