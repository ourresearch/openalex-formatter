import datetime

import shortuuid
from sqlalchemy import Sequence

from app import db, app_url


class Export(db.Model):
    id = db.Column(db.Text, primary_key=True)
    query_url = db.Column(db.Text)
    format = db.Column(db.Text)
    status = db.Column(db.Text)
    progress = db.Column(db.Float)
    result_url = db.Column(db.Text)
    submitted = db.Column(db.DateTime)
    progress_updated = db.Column(db.DateTime)
    progress_url = db.Column(db.Text)
    is_async = db.Column(db.Boolean)

    def __init__(self, **kwargs):
        if 'format' in kwargs:
            self.format = kwargs['format']
        self.id = f'works-{self.format}-{shortuuid.uuid()}'
        self.status = "submitted"
        self.progress = 0
        self.submitted = datetime.datetime.utcnow()
        self.progress_updated = self.submitted
        self.progress_url = f'{app_url}/export/{self.id}'
        super().__init__(**kwargs)

    def to_dict(self):
        return {
            'id': self.id,
            'query_url': self.query_url,
            'status': self.status,
            'format': self.format,
            'progress': self.progress,
            'result_url': self.result_url,
            'submitted': self.submitted and self.submitted.isoformat(),
            'progress_updated': self.progress_updated and self.progress_updated.isoformat(),
            'progress_url': self.progress_url
        }

    def __repr__(self):
        return f'<Export ({self.id}, {self.query_url}, {self.status})>'


class ExportEmail(db.Model):
    id = db.Column(db.Integer, Sequence('export_email_id_seq', start=1, increment=1), primary_key=True)
    export_id = db.Column(db.Text, db.ForeignKey('export.id'))
    requester_email = db.Column(db.Text)
    requested_at = db.Column(db.DateTime)
    sent_at = db.Column(db.DateTime)

    def __init__(self, **kwargs):
        self.requested_at = datetime.datetime.utcnow()
        super().__init__(**kwargs)

    def to_dict(self):
        return {
            'export_id': self.export_id,
            'requester_email': self.requester_email,
            'requested_at': self.requested_at and self.requested_at.isoformat(),
            'sent_at': self.sent_at and self.sent_at.isoformat(),
        }

    def __repr__(self):
        return f'<CsvExportEmail ({self.export_id}, {self.requester_email}): {self.sent_at}>'
