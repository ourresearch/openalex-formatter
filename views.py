import json
import os
from urllib.parse import urlencode

import boto3
import requests
from flask import abort, jsonify, make_response, redirect, request

from app import app
from app import db
from bibtex import dump_bibtex
from csv_export import CsvExport


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
    export_format = export_format and export_format.strip().lower()

    if not export_format:
        abort_json(400, '"format" argument is required')
    if export_format == 'csv':
        query_url = 'https://api.openalex.org/works'
        query_filter = request.args.get('filter')

        if query_filter:
            query_string = urlencode({'filter': query_filter})
            query_url = f'{query_url}?{query_string}'

        response_json = {}
        try:
            query_response = requests.get(query_url)

            if not query_response.status_code == 200:
                return make_response(query_response.content, query_response.status_code)

            if not (response_json := query_response.json()):
                raise requests.exceptions.RequestException

            if not response_json.get('meta', {}).get('page'):
                raise requests.exceptions.RequestException

            new_export = CsvExport(query_url=query_url)
            db.session.merge(new_export)
            db.session.commit()
            return jsonify(new_export.to_dict())
        except requests.exceptions.RequestException:
            abort_json(500, f"There was an error submitting your request to {query_url}.")

        return jsonify(response_json)
    else:
        abort_json(422, 'supported formats are: "csv"')


@app.route('/export/<export_id>', methods=["GET"])
def lookup_export(export_id):
    if not (export := CsvExport.query.get(export_id)):
        abort_json(404, f'Export {export_id} does not exist.')

    return jsonify(export.to_dict())


@app.route('/export/<export_id>/download', methods=["GET"])
def download_export(export_id):
    if not (export := CsvExport.query.get(export_id)):
        abort_json(404, f'Export {export_id} does not exist.')

    if not export.status == 'finished':
        abort_json(422, f'Export {export_id} is not finished.')

    if export.submitted:
        filename = f'works-{export.submitted.strftime("%Y-%m-%dT%H-%M-%S")}.csv'
    else:
        filename = f'{export_id}.csv'

    s3_client = boto3.client('s3')
    presigned_url = s3_client.generate_presigned_url(
        'get_object',
        Params={
            'Bucket': 'openalex-query-exports',
            'Key': f'{export.id}.csv',
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
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True, threaded=True)

