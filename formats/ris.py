import tempfile

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
                 f"PY  - {work['publication_year']}",
                 f"PB  - {work['primary_location']['source']['host_organization_name']}"]

    if work['ids'].get('doi'):
        doi = work['ids']['doi'].split('.org/')[-1]
        ris_entry.append(f"DO  - {doi}")
        ris_entry.append(f"DOI  - {doi}")
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
    if work['ids'].get('issn'):
        ris_entry.append(f"SN  - {work['ids']['issn']}")
    if work['biblio'].get('volume'):
        ris_entry.append(f"VL  - {work['biblio']['volume']}")
    if work['biblio'].get('issue'):
        ris_entry.append(f"IS  - {work['biblio']['issue']}")
    if work['biblio'].get('first_page'):
        ris_entry.append(f"SP  - {work['biblio']['first_page']}")
    if work['biblio'].get('last_page'):
        ris_entry.append(f"EP  - {work['biblio']['last_page']}")

    ris_entry.append('ER -\n')

    return "\n".join(ris_entry)


def export_ris(works):
    fname = tempfile.mkstemp(suffix='.ris')[1]
    with open(fname, 'w') as f:
        for work in works:
            ris_entry = build_ris_entry(work)
            f.write(ris_entry)
    return fname