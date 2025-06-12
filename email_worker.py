import datetime
import os
from time import sleep, time

import sentry_sdk
from sqlalchemy import text

from app import app
from app import db, logger, EXPORT_EMAIL_TABLE, EXPORT_TABLE
from models import Export, ExportEmail
from emailer import send_email
from util import elapsed

sentry_sdk.init(dsn=os.environ.get('SENTRY_DSN'),)


def worker_run():
    while True:
        if email_request_id := fetch_email_request_id():
            if not (email_request := ExportEmail.query.get(email_request_id)):
                # not sure how this happened, but not much we can do
                continue

            if not (csv_export := Export.query.get(email_request.export_id)):
                # same thing
                continue

            email_result_link(csv_export, email_request.requester_email)
            email_request.sent_at = datetime.datetime.utcnow()
            db.session.merge(email_request)
            db.session.commit()
        else:
            sleep(5)


def email_result_link(export, email):
    send_email(
        email,
        "Your OpenAlex Works download is ready",
        "csv_export_ready",
        {
            "data": {
                "result_url": export.result_url,
                "query_url": export.query_url,
            }
        },
        for_real=True
    )


last_log_time = 0
def fetch_email_request_id():
    global last_log_time
    last_log_time = time() if time() - last_log_time >= 60 and logger.info("looking for results that are ready to send") is None else last_log_time

    fetch_query = text(f"""
        with fetched_request as (
            select """ + EXPORT_EMAIL_TABLE + """.id
            from 
                """ + EXPORT_EMAIL_TABLE + """
                join """ + EXPORT_TABLE + """ on """ + EXPORT_EMAIL_TABLE + """.export_id = """ + EXPORT_TABLE + """.id
            where 
                """ + EXPORT_TABLE + """.status = 'finished'
                and """ + EXPORT_EMAIL_TABLE + """.send_started is null
            order by """ + EXPORT_EMAIL_TABLE + """.requested_at
            limit 1
            for update skip locked
        )
        update """ + EXPORT_EMAIL_TABLE + """
        set send_started = now()
        from fetched_request
        where """ + EXPORT_EMAIL_TABLE + """.id = fetched_request.id
        returning fetched_request.id;
    """)

    job_time = time()
    with db.engine.connect() as connection:
        trans = connection.begin()
        export_request_id = connection.execute(fetch_query).scalar()
        trans.commit()
        
    if export_request_id:
        logger.info(f'fetched export email request {export_request_id}, took {elapsed(job_time)} seconds')
    
    return export_request_id


if __name__ == "__main__":
    with app.app_context():
        worker_run()
