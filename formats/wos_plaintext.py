import datetime
import tempfile
from io import StringIO

import pycountry

from formats.util import paginate, get_first_page

HEADER = [
    'FN OpenAlex',
    'VR 1.0'
]


def export_wos(export):
    wos_filename = tempfile.mkstemp(suffix='.txt')[1]
    with open(wos_filename, 'w') as file:
        file.write('\n'.join(HEADER))
        file.write('\n')
        for page in paginate(export, wos_filename):
            lines = []
            for work in page:
                for key, processor in WOS_PROCESSORS.items():
                    line = processor(work)
                    # stripe None values
                    if line and line[0].split(' ')[1] == 'None':
                        line[0] = line[0].split(' ')[0]
                    lines.extend(line)
                # write to file and add a blank line
                file.write('\n'.join(lines))
                file.write('\nER\n\n')

    return wos_filename


def instant_export(export):
    first_page = get_first_page(export)
    buffer = StringIO()
    buffer.write('\n'.join(HEADER))
    buffer.write('\n')
    lines = []
    for work in first_page['results']:
        for key, processor in WOS_PROCESSORS.items():
            line = processor(work)
            if line and line[0].split(' ')[1] == 'None':
                line[0] = line[0].split(' ')[0]
            lines.extend(line)
        # write to file and add a blank line
        buffer.write('\n'.join(lines))
        buffer.write('\nER\n\n')
        
    return buffer.getvalue()


def process_pub_type(work):
    pub_type = get_pub_type(work.get('type'))
    return [f'PT {pub_type}']


def process_authors_au(work):
    author_names = [authorship.get('author').get('display_name') for authorship
                    in work.get('authorships')]
    if author_names:
        return [f'AU {author_names[0]}'] + [f'   {name}' for name in
                                            author_names[1:]]
    else:
        return [f'AU ']


def process_authors_af(work):
    author_names = [authorship.get('author').get('display_name') for authorship
                    in work.get('authorships')]
    if author_names:
        return [f'AF {author_names[0]}'] + [f'   {name}' for name in
                                            author_names[1:]]
    else:
        return [f'AF ']


def process_source_name(work):
    if work.get('primary_location') and work.get('primary_location').get(
            'source'):
        source_name = work.get("primary_location", {}).get("source", {}).get(
            "display_name")
    else:
        source_name = None
    return [f'SO {source_name}']


def process_language(work):
    language = get_full_language_name(work.get('language'))
    return [f'LA {language}']


def process_document_type(work):
    doc_type = work.get('type').capitalize()
    return [f'DT {doc_type}']


def process_author_addresses(work):
    lines = []
    first_line_set = False
    for author in work.get('authorships'):
        author_name = author.get('author').get('display_name')
        addresses = get_author_addresses(author)
        if addresses:
            prefix = f'   [{author_name}]' if first_line_set else f'C1 [{author_name}]'
            lines.append(f'{prefix} {addresses[0]}')
            lines.extend(
                [f'   [{author_name}] {address}' for address in addresses[1:]])
            first_line_set = True
        else:
            prefix = 'C1' if not first_line_set else '  '
            lines.append(f'{prefix} [{author_name}]')
            first_line_set = True
    return lines


def process_affiliations(work):
    affiliations = [address for author in work.get('authorships') for address in
                    get_author_addresses(author)]
    return [f'C3 {"; ".join(set(affiliations))}']


def process_corresponding_author(work):
    for author in work.get('authorships'):
        if author.get('is_corresponding'):
            author_name = author.get('author').get('display_name')
            institution_name = author.get('institutions')[0].get(
                'display_name') if author.get('institutions') else None
            institution_country_code = author.get('institutions')[0].get(
                'country_code') if author.get('institutions') else None
            return [
                f'RP {author_name} (corresponding author), {institution_name}, {institution_country_code}']
    return []


def process_author_ids(work):
    author_id_pairs = []
    for author in work.get('authorships'):
        if author.get('author').get('id'):
            author_id_pairs.append((author.get('author').get('display_name'),
                                    author.get('author').get('id')))
    author_id_pairs_formatted = ', '.join(
        [f'{name}/{author_id}' for name, author_id in author_id_pairs])
    return [f'RI {author_id_pairs_formatted}']


def process_orcid_ids(work):
    name_orcid_pairs = []
    for author in work.get('authorships'):
        if author.get('author').get('orcid'):
            author_name = author.get('author').get('display_name')
            orcid = author.get('author').get('orcid')
            name_orcid_pairs.append(
                (author_name, orcid.replace('https://orcid.org/', '')))
    name_orcid_pairs_formatted = ', '.join(
        [f'{name}/{orcid}' for name, orcid in name_orcid_pairs])
    return [f'OI {name_orcid_pairs_formatted}']


def process_funding_orgs(work):
    funding_orgs = []
    for grant in work.get('grants'):
        funder_name = grant.get('funder_display_name')
        award_id = grant.get('award_id')
        if funder_name and award_id:
            funding_orgs.append(f'{funder_name} [{award_id}]')
        elif funder_name:
            funding_orgs.append(f'{funder_name}')
    return [f'FU {"; ".join(funding_orgs)}']


def process_cited_by_count(work):
    cited_by_count = work.get('cited_by_count')
    return [f'CT {cited_by_count}']


def process_num_references(work):
    num_references = work.get('referenced_works_count')
    return [f'NR {num_references}']


def process_publisher(work):
    if work.get('primary_location') and work.get('primary_location').get(
            'source'):
        publisher = work.get('primary_location', {}).get('source', {}).get(
            'host_organization_name')
    else:
        publisher = None
    return [f'PU {publisher}']


def process_issn(work):
    if work.get('primary_location') and work.get('primary_location').get(
            'source'):
        issn = work.get('primary_location', {}).get('source', {}).get('issn_l')
    else:
        issn = None
    return [f'SN {issn}']


def process_e_issn(work):
    if work.get('primary_location') and work.get('primary_location').get(
            'source'):
        issn = work.get('primary_location', {}).get('source', {}).get('issn_l')
    else:
        issn = None
    return [f'EI {issn}']


def process_publication_date(work):
    publication_date = work.get('publication_date')
    publication_month_digit = publication_date[5:7]
    publication_month = datetime.datetime.strptime(publication_month_digit,
                                                   "%m").strftime("%b").upper()
    return [f'PD {publication_month}']


def process_pmid(work):
    if work.get('ids') and work.get('ids').get('pmid'):
        pmid = work.get('ids').get('pmid')
        pmid_formatted = pmid.replace('https://pubmed.ncbi.nlm.nih.gov/',
                                      '') if pmid else None
    else:
        pmid_formatted = None
    return [f'PM {pmid_formatted}']


def process_doi(work):
    doi = work.get('doi')
    if doi:
        doi_formatted = doi.replace('https://doi.org/', '')
        return [f'DI {doi_formatted}']
    else:
        return [f'DI ']


def process_number_of_pages(work):
    last_page = work.get('biblio').get('last_page')
    first_page = work.get('biblio').get('first_page')
    if last_page and first_page:
        try:
            number_of_pages = int(last_page) - int(first_page) + 1
            return [f'PG {number_of_pages}']
        except ValueError:
            return ['PG ']
    else:
        return ['PG ']


WOS_PROCESSORS = {
    'PT': process_pub_type,
    'AU': process_authors_au,
    'AF': process_authors_af,
    'TI': lambda work: [f'TI {work.get("display_name")}'],
    'SO': process_source_name,
    'LA': process_language,
    'DT': process_document_type,
    'C1': process_author_addresses,
    'C3': process_affiliations,
    'RP': process_corresponding_author,
    'RI': process_author_ids,
    'OI': process_orcid_ids,
    'FU': process_funding_orgs,
    'CT': process_cited_by_count,
    'NR': process_num_references,
    'PU': process_publisher,
    'SN': process_issn,
    'EI': process_e_issn,
    'PD': process_publication_date,
    'PY': lambda work: [f'PY {work.get("publication_year")}'],
    'VL': lambda work: [
        f'VL {work.get("biblio").get("volume") if work.get("biblio") else None}'],
    'IS': lambda work: [
        f'IS {work.get("biblio").get("issue") if work.get("biblio") else None}'],
    'BP': lambda work: [
        f'BP {work.get("biblio").get("first_page") if work.get("biblio") else None}'],
    'EP': lambda work: [
        f'EP {work.get("biblio").get("last_page") if work.get("biblio") else None}'],
    'DI': process_doi,
    'PG': process_number_of_pages,
    'PM': process_pmid,
    'OA': lambda work: [
        f'OA {work.get("open_access").get("oa_status") if work.get("open_access") else None}'],
    'DA': lambda work: [f'DA {datetime.datetime.now().strftime("%Y-%m-%d")}'],
}


def get_pub_type(openalex_type):
    if openalex_type == 'article':
        return 'J'
    elif openalex_type == 'book-chapter':
        return 'B'
    elif openalex_type == 'book':
        return 'B'
    elif openalex_type == 'conference-paper':
        return 'P'
    elif openalex_type == 'dataset':
        return 'D'
    elif openalex_type == 'dissertation':
        return 'D'
    elif openalex_type == 'preprint':
        return 'P'
    elif openalex_type == 'report':
        return 'R'
    elif openalex_type == 'software':
        return 'S'
    elif openalex_type == 'working-paper':
        return 'P'
    else:
        return 'U'


def get_full_language_name(short_code):
    try:
        language = pycountry.languages.get(alpha_2=short_code.lower())
        return language.name
    except AttributeError:
        return None


def get_author_addresses(authorship):
    addresses = []
    for institution in authorship.get("institutions"):
        if institution.get("display_name") and institution.get("country_code"):
            addresses.append(
                f'{institution.get("display_name")}, {institution.get("country_code")}')
        elif institution.get("display_name"):
            addresses.append(f'{institution.get("display_name")}')
    return addresses
