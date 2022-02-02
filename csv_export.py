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

    def __init__(self, **kwargs):
        self.id = shortuuid.uuid()
        self.status = "submitted"
        self.progress = 0
        self.submitted = datetime.datetime.utcnow()
        self.progress_updated = self.submitted
        self.progress_url = f'{app_url}/export/{self.id}/status'
        super().__init__(**kwargs)

    def to_dict(self):
        return {
            'id': self.id,
            'query_url': self.query_url,
            'status': self.status,
            'progress': self.progress,
            'result_url': self.result_url,
            'submitted': self.submitted and self.submitted.isoformat(),
            'progress_updated': self.progress_updated and self.progress_updated.isoformat(),
            'progress_url': self.progress_url
        }

    def __repr__(self):
        return f'<CsvExport ({self.id}, {self.entity_type}, {self.query})>'