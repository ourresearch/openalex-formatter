import jinja2
import requests

from app import logger, mailgun_api_key


def send_email(to_address, subject, template_name, template_data, for_real=False):
    template_loader = jinja2.FileSystemLoader(searchpath='templates')
    template_env = jinja2.Environment(loader=template_loader)
    html_template = template_env.get_template(template_name + '.html')

    html = html_template.render(template_data)

    mailgun_url = f"https://api.mailgun.net/v3/ourresearch.org/messages"

    mailgun_auth = ("api", mailgun_api_key)

    mailgun_data = {
        "from": "OurResearch Team <team@ourresearch.org>",
        "to": [to_address],
        "subject": subject,
        "html": html
    }

    logger.info(f'sending email "{subject}" to {to_address}')

    if for_real:
        requests.post(mailgun_url, auth=mailgun_auth, data=mailgun_data)
        print("Sent an email")
    else:
        print("Didn't really send")
