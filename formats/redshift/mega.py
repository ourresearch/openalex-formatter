import os
import re
from typing import Optional
from datetime import datetime

import boto3
from sqlalchemy import text

from models import Export
from .query_handler import RedshiftQueryHandler, get_entity_class

aws_key, aws_secret = os.getenv('AWS_ACCESS_KEY_ID'), os.getenv('AWS_SECRET_ACCESS_KEY')
s3_client = boto3.client('s3')
exports_bucket = 'openalex-query-exports'

def export_mega_csv(export: Export):
    return export_query_to_csv(**export.args, s3_path=f's3://{exports_bucket}/{export.id}.csv')



def export_query_to_csv(
        entity: str,
        filter_works: list,
        filter_aggs: list,
        show_columns: list,
        s3_path: str,
        sort_by_column: Optional[str] = None,
        sort_by_order: Optional[str] = None,
) -> str:
    """
    Exports a RedshiftQueryHandler query to CSV using Redshift's UNLOAD command.

    Args:
        entity: The entity type to query (e.g., "works", "authors", etc.)
        filter_works: List of work filters
        filter_aggs: List of aggregation filters
        show_columns: List of columns to include
        sort_by_column: Column to sort by
        sort_by_order: Sort order ("asc" or "desc")

    Returns:
        str: The S3 path where the CSV was exported
    """
    # Initialize query handler
    handler = RedshiftQueryHandler(
        entity=entity,
        filter_works=filter_works,
        filter_aggs=filter_aggs,
        show_columns=show_columns,
        sort_by_column=sort_by_column,
        sort_by_order=sort_by_order,
        valid_columns=show_columns
    )

    # Build the query using the handler
    entity_class = get_entity_class(entity)
    query = handler.build_joins(entity_class)
    query = handler.set_columns(query, entity_class)
    query = handler.apply_work_filters(query)
    query = handler.apply_entity_filters(query, entity_class)
    query = handler.apply_sort(query, entity_class)
    query = handler.apply_stats(query, entity_class)

    # Convert SQLAlchemy query to raw SQL
    sql_query = str(query.statement.compile(
        compile_kwargs={"literal_binds": True}
    ))

    # Escape single quotes in the SQL query
    sql_query = sql_query.replace("'", "''")
    # Create UNLOAD command and wrap it in text()
    unload_command = text(f"""
    UNLOAD ('{sql_query}')
    TO '{s3_path}'
    CREDENTIALS 'aws_access_key_id={aws_key};aws_secret_access_key={aws_secret}'
    DELIMITER ','
    HEADER
    ALLOWOVERWRITE
    PARALLEL OFF
    """)

    handler.session.execute(unload_command)
    handler.session.commit()

    return s3_path


# Example usage:
"""
filter_works = [
    {"column_id": "year", "value": 2020, "operator": "is"}
]
show_columns = ["paper_id", "original_title", "year"]

export_path = export_query_to_csv(
    entity="works",
    filter_works=filter_works,
    filter_aggs=[],
    show_columns=show_columns,
    aws_access_key_id="your_key",
    aws_secret_access_key="your_secret"
)
print(f"Export completed: {export_path}")
"""