import datetime

import shortuuid

from app import app_url, db


class CsvExport(db.Model):
    id = db.Column(db.Text, primary_key=True)
    query_url = db.Column(db.Text)
    status = db.Column(db.Text)
    progress = db.Column(db.Float)
    result_url = db.Column(db.Text)
    submitted = db.Column(db.DateTime)
    progress_updated = db.Column(db.DateTime)
    progress_url = db.Column(db.Text)
    requester_email = db.Column(db.Text, nullable=False)

    def __init__(self, **kwargs):
        self.id = f'works-csv-{shortuuid.uuid()}'
        self.status = "submitted"
        self.progress = 0
        self.submitted = datetime.datetime.utcnow()
        self.progress_updated = self.submitted
        self.progress_url = f'{app_url}/export/{self.id}'
        super().__init__(**kwargs)

    def to_dict(self):
        return {
            'id': self.id,
            'requester_email': self.requester_email,
            'query_url': self.query_url,
            'status': self.status,
            'progress': self.progress,
            'result_url': self.result_url,
            'submitted': self.submitted and self.submitted.isoformat(),
            'progress_updated': self.progress_updated and self.progress_updated.isoformat(),
            'progress_url': self.progress_url
        }

    def __repr__(self):
        return f'<CsvExport ({self.id}, {self.requester_email}, {self.query_url}, {self.status})>'
