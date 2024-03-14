import tempfile
from io import StringIO

from formats.util import paginate, get_nested_value, get_first_page, \
    unravel_index

RIS_CONTENT_TYPE = 'text/x-ris'

type_map = {
    "article": "JOUR",
    "book-chapter": "CHAP",
    "dissertation": "THES",
    "book": "BOOK",
    "dataset": "DATA",
    "paratext": "GEN",
    "other": "GEN",
    "reference-entry": "ENTRY",
    "report": "RPRT",
    "peer-review": "JOUR",
    "standard": "STAND",
    "editorial": "JOUR",
    "erratum": "ERRT",
    "grant": "GEN",
    "letter": "LETTER"
}


def build_ris_entry(work):
    ris_entry = [f"TY  - {type_map[work['type']]}",
                 f"TI  - {work['title']}",
                 f"PY  - {work['publication_year']}"]

    if get_nested_value(work, 'primary_location', 'source',
                        'host_organization_name'):
        ris_entry.append(
            f"PB  - {work['primary_location']['source']['host_organization_name']}")

    if get_nested_value(work, 'primary_location', 'source', 'issn_l'):
        ris_entry.append(
            f"SN  - {work['primary_location']['source']['issn_l']}")

    if get_nested_value(work, 'primary_location', 'source', 'display_name'):
        ris_entry.append(
            f"T2  - {work['primary_location']['source']['display_name']}")

    if work['ids'].get('doi'):
        doi = work['ids']['doi'].split('.org/')[-1]
        ris_entry.append(f"DO  - {doi}")
        ris_entry.append(f"DOI - {doi}")
        ris_entry.append(f"DI  - {doi}")
        ris_entry.append(f"URL  - {work['ids']['doi']}")

    if work['publication_date']:
        ris_entry.append(f"DA  - {work['publication_date']}")

    # Authors
    for authorship in work['authorships']:
        author_name = authorship['author']['display_name']
        ris_entry.append(f"AU  - {author_name}")
        if authorship['raw_affiliation_strings']:
            for affiliation in authorship['raw_affiliation_strings']:
                ris_entry.append(f"C1  - {affiliation}")

    # Additional optional fields
    if work['language']:
        ris_entry.append(f"LA  - {work['language']}")
    if work['keywords']:
        for kw in work['keywords']:
            ris_entry.append(f"KW  - {kw['keyword']}")
    if work['biblio'].get('volume'):
        ris_entry.append(f"VL  - {work['biblio']['volume']}")
    if work['biblio'].get('issue'):
        ris_entry.append(f"IS  - {work['biblio']['issue']}")
    if work['biblio'].get('first_page'):
        ris_entry.append(f"SP  - {work['biblio']['first_page']}")
    if work['biblio'].get('last_page'):
        ris_entry.append(f"EP  - {work['biblio']['last_page']}")

    if work.get('abstract_inverted_index') and (work.get('open_access') or {}).get('is_oa'):
        ris_entry.append(f"AB - {unravel_index(work['abstract_inverted_index'])}")

    ris_entry.append('ER  -\n')

    return "\n".join(ris_entry)


def export_ris(export):
    fname = tempfile.mkstemp(suffix='.ris')[1]
    with open(fname, 'w') as f:
        for page in paginate(export, fname):
            for work in page:
                ris_entry = build_ris_entry(work)
                f.write(ris_entry)
    return fname


def instant_export(export):
    first_page = get_first_page(export)
    buffer = StringIO()
    for work in first_page['results']:
        ris_entry = build_ris_entry(work)
        buffer.write(ris_entry)
    return buffer.getvalue()
