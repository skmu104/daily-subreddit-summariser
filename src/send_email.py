import asyncio
from pathlib import Path
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders
from datetime import date
import json
import os
import sys
import aiosmtplib
import aiofiles
import copy

email_file_path = 'email_list.json'


async def send_mail(smtp, email_msg, send_to):
    """Sends an email using an existing SMTP connection."""
    email_msg['To'] = send_to
    await smtp.send_message(email_msg)


async def construct_email(content_path):
    """Constructs the email message asynchronously."""
    msg = MIMEMultipart()
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = f"NBA reddit daily recap {date.today()}"
    send_from = "devbot2406@gmail.com"
    msg['From'] = send_from
    message = "Reddit NBA chatter for the day"
    msg.attach(MIMEText(message))

    part = MIMEBase('application', "octet-stream")
    async with aiofiles.open(content_path, 'rb') as file:
        part.set_payload(await file.read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition',
                    f'attachment; filename={Path(content_path).name}')
    msg.attach(part)
    return msg


async def get_recipients_from_json() -> list[str]:
    """Reads recipients from a JSON file asynchronously."""
    try:
        async with aiofiles.open(email_file_path, 'r') as file:
            content = await file.read()
            json_file = json.loads(content)
            return json_file['recipients']
    except FileNotFoundError:
        print(f"Missing file: {email_file_path}")
        return []


async def send_recording_to_email_list(content, server="smtp.gmail.com", port=465, username='devbot2406@gmail.com', use_tls=True):
    """Sends the recording to the email list asynchronously."""
    print("Sending audio file to email list")
    password = os.environ.get('DEV_EMAIL_PASSWORD')
    if not password:
        print("DEV_EMAIL_PASSWORD environment variable not set.")
        sys.exit(1)

    msg = await construct_email(content_path=content)
    recipients = await get_recipients_from_json()

    smtp = aiosmtplib.SMTP(hostname=server, port=port, use_tls=use_tls)
    async with smtp:
        await smtp.login(username, password)
        tasks = []
        for recipient in recipients:
            recipient_msg = copy.deepcopy(msg)
            tasks.append(send_mail(smtp, recipient_msg, recipient))
        await asyncio.gather(*tasks)
    print("Email successfully sent")
