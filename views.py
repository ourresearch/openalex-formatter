import datetime
import json
import os
import re
from urllib.parse import urlencode

import boto3
import requests
from flask import abort, jsonify, make_response, redirect, request
import sentry_sdk

from app import app, supported_formats
from app import db
from bibtex import dump_bibtex
from models import Export, ExportEmail


sentry_sdk.init(dsn=os.environ.get('SENTRY_DSN'),)


def abort_json(status_code, msg):
    body_dict = {
        "HTTP_status_code": status_code,
        "message": msg,
        "error": True
    }
    resp_string = json.dumps(body_dict, sort_keys=True, indent=4)
    resp = make_response(resp_string, status_code)
    resp.mimetype = "application/json"
    abort(resp)


@app.after_request
def after_request(response):
    # support CORS
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS, PUT, DELETE, PATCH"
    response.headers["Access-Control-Allow-Headers"] = "Origin, X-Requested-With, Content-Type, Accept, Authorization, Cache-Control"
    response.headers["Access-Control-Expose-Headers"] = "Authorization, Cache-Control"
    response.headers["Access-Control-Allow-Credentials"] = "true"

    # make not cacheable because the GETs change after parameter change posts!
    response.cache_control.max_age = 0
    response.cache_control.no_cache = True

    return response


@app.route('/works', strict_slashes=False, methods=["GET"])
def init_export_works():
    export_format = request.args.get('format')
    email = request.args.get('email')
    export_format = export_format and export_format.strip().lower()

    if email:
        email = email.strip()
        if not re.match(r'^.+@.+\..+$', email):
            abort_json(400, f"email argument {email} doesn't look like an email address")

    if not export_format:
        abort_json(400, '"format" argument is required')
    if export_format in supported_formats:
        query_url = 'https://api.openalex.org/works'
        query_args = {}

        if query_filter := request.args.get('filter'):
            query_args['filter'] = query_filter

        if query_sort := request.args.get('sort'):
            query_args['sort'] = query_sort

        if query_search := request.args.get('search'):
            query_args['search'] = query_search

        if query_group_bys_fields := request.args.get('group-bys'):
            query_args['group_bys'] = query_group_bys_fields

        if query_args:
            query_string = urlencode(query_args)
            query_url = f'{query_url}?{query_string}'

        export = Export.query.filter(
            Export.format == export_format,
            Export.query_url == query_url,
            Export.progress_updated > datetime.datetime.utcnow() - datetime.timedelta(minutes=15)
        ).first()

        if not export:
            if export_format != 'group-bys-csv':
                try:
                    test_query_response = requests.get(query_url)

                    if not test_query_response.status_code == 200:
                        return make_response(test_query_response.content, test_query_response.status_code)

                    if not (response_json := test_query_response.json()):
                        raise requests.exceptions.RequestException

                    if not response_json.get('meta', {}).get('page'):
                        raise requests.exceptions.RequestException

                except requests.exceptions.RequestException:
                    abort_json(500, f"There was an error submitting your request to {query_url}.")

            export = Export(query_url=query_url, format=export_format)
            db.session.merge(export)

        if email:
            export_email = ExportEmail(export_id=export.id, requester_email=email)
            db.session.merge(export_email)

        db.session.commit()
        return jsonify(export.to_dict())
    else:
        abort_json(422, f'supported formats are: {",".join(supported_formats)}')


@app.route('/export/<export_id>', methods=["GET"])
def lookup_export(export_id):
    if not (export := Export.query.get(export_id)):
        abort_json(404, f'Export {export_id} does not exist.')

    return jsonify(export.to_dict())


@app.route('/export/<export_id>/download', methods=["GET"])
def download_export(export_id):
    if not (export := Export.query.get(export_id)):
        abort_json(404, f'Export {export_id} does not exist.')

    if export.format not in supported_formats:
        abort_json(422, f'Export {export_id} is not a supported format.')

    file_format = supported_formats[export.format]

    if not export.status == 'finished':
        abort_json(422, f'Export {export_id} is not finished.')

    if export.submitted:
        filename = f'works-{export.submitted.strftime("%Y-%m-%dT%H-%M-%S")}.{file_format}'
    else:
        filename = f'{export_id}.{file_format}'

    s3_client = boto3.client('s3')
    presigned_url = s3_client.generate_presigned_url(
        'get_object',
        Params={
            'Bucket': 'openalex-query-exports',
            'Key': f'{export.id}.{file_format}',
            'ResponseContentDisposition': f'attachment; filename={filename}',
            'ResponseContentType': 'text/csv'
        },
        ExpiresIn=300
    )

    return redirect(presigned_url, 302)


@app.route('/works/<work_id>.<export_format>', strict_slashes=False, methods=["GET"])
def format_single_work(work_id, export_format):
    export_format = export_format and export_format.strip().lower()

    if not export_format:
        abort_json(400, '"format" argument is required')
    if export_format == 'bib':
        query_url = f'https://api.openalex.org/works/{work_id}'
        response_json = {}

        try:
            query_response = requests.get(query_url)

            if not query_response.status_code == 200:
                return make_response(query_response.content, query_response.status_code)

            if not (response_json := query_response.json()):
                raise requests.exceptions.RequestException
        except requests.exceptions.RequestException:
            abort_json(500, f"There was an error submitting your request to {query_url}.")

        response = make_response(dump_bibtex(response_json))
        response.headers['Content-Type'] = 'application/x-bibtex; charset=utf-8'
        return response
    else:
        abort_json(422, 'supported formats are: "bib"')


@app.route('/', methods=["GET", "POST"])
def base_endpoint():
    return jsonify({
        "version": "0.0.1",
        "msg": "Don't panic"
    })


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    app.run(host='0.0.0.0', port=port, debug=True, threaded=True)

