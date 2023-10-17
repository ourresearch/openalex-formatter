import csv
import tempfile
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

GROUP_LIMIT = 15000  # max number of groups for a single group_by


def export_group_bys_csv(export):
    group_bys, query = parse_query_url(export.query_url)
    csv_filename = tempfile.mkstemp(suffix=".csv")[1]
    csv_data = [
        [f"Your query: {export.query_url}"],
        [],
        [f"Number of results: {get_total_count(query)}"],
        [],
    ]

    column_pointer = 0
    for group_by in group_bys:
        groups = fetch_group_data(query, group_by)
        csv_data, column_pointer = append_group_to_csv_data(
            csv_data, column_pointer, group_by, groups
        )

    with open(csv_filename, "w") as csv_file:
        writer = csv.writer(csv_file)
        for row in csv_data:
            writer.writerow(row)
    return csv_filename


def parse_query_url(query_url):
    parsed_url = urlparse(query_url)
    parsed_query = parse_qs(parsed_url.query)

    group_bys = parsed_query.pop("group_bys", [None])[0]
    if group_bys:
        group_bys = group_bys.split(",")
    else:
        group_bys = []

    new_query_str = urlencode(parsed_query, doseq=True)
    rebuilt_url = urlunparse(
        (
            parsed_url.scheme,
            parsed_url.netloc,
            parsed_url.path,
            parsed_url.params,
            new_query_str,
            parsed_url.fragment,
        )
    )

    return group_bys, rebuilt_url


def get_total_count(query):
    r = requests.get(query)
    return r.json()["meta"]["count"]


def fetch_group_data(query, group_by, per_page=50):
    groups = []
    cursor = "*"
    while cursor is not None and len(groups) < GROUP_LIMIT:
        url = f"{query}&group_by={group_by}&per_page={per_page}&cursor={cursor}&mailto=team@ourresearch.org"
        result = get_request(url)
        cursor = result["meta"]["next_cursor"]
        for group in result["group_by"]:
            groups.append(group)
            if len(groups) >= GROUP_LIMIT:
                break

    return sorted(groups, key=lambda x: x["count"], reverse=True)


@retry(wait=wait_exponential(multiplier=1, min=1, max=5), stop=stop_after_attempt(3))
def get_request(url):
    result = requests.get(url).json()
    return result


def append_group_to_csv_data(csv_data, column_pointer, group_by, groups):
    limit_hit = len(groups) >= GROUP_LIMIT

    # header
    while len(csv_data) <= 4:
        csv_data.append([""] * column_pointer)
    csv_data[4].extend([group_by, "", ""])

    # group data
    for idx, group in enumerate(groups):
        while len(csv_data) <= idx + 5:
            csv_data.append([""] * column_pointer)
        csv_data[idx + 5].extend([group["key_display_name"], group["count"], ""])

    # optional truncation message
    if limit_hit:
        message_row_idx = 5 + len(groups)

        # ensure the row exists
        while len(csv_data) <= message_row_idx:
            csv_data.append([""] * (column_pointer + 3))

        # ensure the column exists
        while len(csv_data[message_row_idx]) <= column_pointer:
            csv_data[message_row_idx].append("")

        csv_data[message_row_idx][
            column_pointer
        ] = f"Results truncated, groups limited to {GROUP_LIMIT} results per group."

    return csv_data, column_pointer + 3
