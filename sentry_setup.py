import os
import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration
from sentry_sdk.integrations.logging import LoggingIntegration
from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration

def init_sentry(traces_sample_rate=0.0, use_flask=True):
    integrations = [
        LoggingIntegration(level=None, event_level='ERROR'),
        SqlalchemyIntegration()
    ]
    if use_flask:
        integrations.append(FlaskIntegration())

    sentry_sdk.init(
        dsn=os.environ.get("SENTRY_DSN"),
        integrations=integrations,
        traces_sample_rate=traces_sample_rate
    )