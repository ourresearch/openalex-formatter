import datetime
import json
import os
import re
from urllib.parse import urlencode

import boto3
import requests
import shortuuid
from flask import abort, jsonify, make_response, redirect, request
import sentry_sdk

from app import app, supported_formats, s3_key_formats
from app import db
from bibtex import dump_bibtex
from formats.util import parse_bool
from models import Export, ExportEmail
from formats.csv import instant_export as csv_instant_export
from formats.ris import instant_export as ris_instant_export
from formats.wos_plaintext import instant_export as wos_instant_export

sentry_sdk.init(dsn=os.environ.get('SENTRY_DSN'), )


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
    response.headers[
        "Access-Control-Allow-Methods"] = "POST, GET, OPTIONS, PUT, DELETE, PATCH"
    response.headers[
        "Access-Control-Allow-Headers"] = "Origin, X-Requested-With, Content-Type, Accept, Authorization, Cache-Control"
    response.headers[
        "Access-Control-Expose-Headers"] = "Authorization, Cache-Control"
    response.headers["Access-Control-Allow-Credentials"] = "true"

    # make not cacheable because the GETs change after parameter change posts!
    response.cache_control.max_age = 0
    response.cache_control.no_cache = True

    return response


def instant_export_response(export):
    if export.format == "csv":
        file_str = csv_instant_export(export)
        content_type = 'text/csv'
    elif export.format == "ris":
        file_str = ris_instant_export(export)
        content_type = 'text/x-ris'
    elif export.format == "wos-plaintext":
        file_str = wos_instant_export(export)
        content_type = 'text/x-wos'
    else:
        raise Exception('Invalid export format: {}'.format(export.format))
    output = make_response(file_str)
    output.headers[
        "Content-Disposition"] = f"attachment; filename={export.id}.{export.format}"
    output.headers["Content-type"] = content_type
    return output


@app.route('/works', strict_slashes=False, methods=["GET"])
def init_export_works():
    export_format = request.args.get('format')
    email = request.args.get('email')
    export_format = export_format and export_format.strip().lower()

    if email:
        email = email.strip()
        if not re.match(r'^.+@.+\..+$', email):
            abort_json(400,
                       f"email argument {email} doesn't look like an email address")

    if not export_format:
        abort_json(400, '"format" argument is required')

    if export_format in supported_formats and export_format != 'mega-csv': # Use mega-csv endpoint for mega-csv format (redshift)
        query_url = 'https://api.openalex.org/works'
        query_args = {}

        # Build query parameters
        if query_filter := request.args.get('filter'):
            query_args['filter'] = query_filter

        if query_sort := request.args.get('sort'):
            query_args['sort'] = query_sort

        if query_search := request.args.get('search'):
            query_args['search'] = query_search

        if query_group_bys_fields := request.args.get('group-bys'):
            query_args['group_bys'] = query_group_bys_fields

        # Build args dictionary for JSON column
        export_args = {
            'is_async': parse_bool(request.args.get('async', 'true')),
            'truncate': parse_bool(request.args.get('truncate', 'false')),
            'select': None,
            'columns': request.args.get('columns')
        }

        # Handle select parameter
        if select := request.args.get('select'):
            select = select.strip(',')
            if 'id' not in select:
                select += ',id'
            export_args['select'] = select
            query_args['select'] = select

        if query_args:
            query_string = urlencode(query_args)
            query_url = f'{query_url}?{query_string}'

        # Query for existing export
        export = Export.query.filter(
            Export.format == export_format,
            Export.query_url == query_url,
            Export.args == export_args,  # JSON comparison
            Export.progress_updated > datetime.datetime.utcnow() - datetime.timedelta(
                minutes=15)
        ).first()

        if not export:
            if export_format != 'group-bys-csv':
                try:
                    test_query_response = requests.get(query_url)

                    if not test_query_response.status_code == 200:
                        return make_response(test_query_response.content,
                                             test_query_response.status_code)

                    if not (response_json := test_query_response.json()):
                        raise requests.exceptions.RequestException

                    if not response_json.get('meta', {}).get('page'):
                        raise requests.exceptions.RequestException

                except requests.exceptions.RequestException:
                    abort_json(500,
                               f"There was an error submitting your request to {query_url}.")

            # Create new export with args as JSON
            export = Export(
                id=f'works-{export_format}-{shortuuid.uuid()}',
                query_url=query_url,
                format=export_format,
                args=export_args
            )
            db.session.merge(export)

        if email:
            export_email = ExportEmail(
                export_id=export.id,
                requester_email=email
            )
            db.session.merge(export_email)

        db.session.commit()

        if not export_args['is_async'] and export_format != 'group-bys-csv':
            return instant_export_response(export)

        return jsonify(export.to_dict())
    else:
        abort_json(422,
                   f'supported formats are: {",".join(supported_formats.keys())}')


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

    if not export.status == 'finished':
        abort_json(422, f'Export {export_id} is not finished.')

    s3_client = boto3.client('s3')
    obj = s3_client.list_objects(Bucket='openalex-query-exports', Prefix=export_id)['Contents'][0]
    extension = obj['Key'].split('.')[-1]
    extension = extension if '00' not in extension else 'csv'

    if export.submitted:
        entity = export.args.get('entity', 'works')
        filename = f'{entity}-{export.submitted.strftime("%Y-%m-%dT%H-%M-%S")}.{extension}'
    else:
        filename = f'{export_id}.{extension}'

    presigned_url = s3_client.generate_presigned_url(
        'get_object',
        Params={
            'Bucket': 'openalex-query-exports',
            'Key': obj['Key'],
            'ResponseContentDisposition': f'attachment; filename={filename}',
            'ResponseContentType': 'text/csv'
        },
        ExpiresIn=300
    )

    return redirect(presigned_url, 302)


@app.route('/works/<work_id>.<export_format>', strict_slashes=False,
           methods=["GET"])
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
                return make_response(query_response.content,
                                     query_response.status_code)

            if not (response_json := query_response.json()):
                raise requests.exceptions.RequestException
        except requests.exceptions.RequestException:
            abort_json(500,
                       f"There was an error submitting your request to {query_url}.")

        response = make_response(dump_bibtex(response_json))
        response.headers['Content-Type'] = 'application/x-bibtex; charset=utf-8'
        return response
    else:
        abort_json(422, 'supported formats are: "bib"')

@app.route('/mega-csv', methods=['POST'])
def mega_csv_export():
    '''
        entity: str,
        filter_works: list,
        filter_aggs: list,
        show_columns: list,
        sort_by_column: Optional[str] = None,
        sort_by_order: Optional[str] = None,
    '''
    email = request.args.get('email')
    if email:
        email = email.strip()
        if not re.match(r'^.+@.+\..+$', email):
            abort_json(400,
                       f"email argument {email} doesn't look like an email address")

    request_json = request.json.copy()
    if request_json.get('get_rows'):
        request_json['entity'] = request_json['get_rows']
        request_json.pop('get_rows')
    required_args = {'entity', 'filter_works', 'filter_aggs', 'show_columns'}
    if any(arg not in request_json for arg in required_args):
        abort_json(400, f'arguments must be specified - {", ".join(required_args)}')
        return
    all_valid_args = required_args.union({'sort_by_column', 'sort_by_order'})
    export_args = {k: v for k, v in request_json.items() if k in all_valid_args}
    export = Export.query.filter(
        Export.format == 'mega-csv',
        Export.args == export_args,  # JSON comparison
        Export.progress_updated > datetime.datetime.utcnow() - datetime.timedelta(
            minutes=15)
    ).first()
    entity = request_json.get('entity')
    if not export:
        export = Export(
            id=f'{entity}-mega-csv-{shortuuid.uuid()}',
            query_url=None,
            format='mega-csv',
            args=export_args
        )

        db.session.merge(export)
        if email:
            export_email = ExportEmail(
                export_id=export.id,
                requester_email=email
            )
            db.session.merge(export_email)
        db.session.commit()

    return jsonify(export.to_dict())


@app.route('/', methods=["GET", "POST"])
def base_endpoint():
    return jsonify({
        "version": "0.0.1",
        "msg": "Don't panic"
    })


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    app.run(host='0.0.0.0', port=port, debug=True, threaded=True)
