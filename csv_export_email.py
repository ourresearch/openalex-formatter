import datetime

from sqlalchemy import Sequence

from app import db


class CsvExportEmail(db.Model):
    id = db.Column(db.Integer, Sequence('csv_export_email_id_seq', start=1, increment=1), primary_key=True)
    export_id = db.Column(db.Text, db.ForeignKey('csv_export.id'))
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
