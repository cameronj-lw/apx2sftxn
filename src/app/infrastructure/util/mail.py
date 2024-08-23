from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from os.path import basename
from smtplib import SMTP


# native
from infrastructure.util.config import AppConfig


HTML_TEMPLATE = """\
<html>
  <head>
    <style>
      body {{ font-family: "Courier New", Courier, monospace; }}
    </style>
  </head>
  <body>
  	<pre>{body}</pre>
  </body>
</html>
"""

def send_plaintext_email(subject, plaintext='', recipients=None, files=None):
	"""
	Send an email as plaintext only

	:param subject: The subject
	:param body: Plaintext Message. Optional
	:param recipients: A list of emails to send to. If not specified the email will be sent to the
						list defined in the ALERT_EMAIL_TO setting
	:return: None
	"""

	# Create a text/plain message
	msg = MIMEText(plaintext, 'plain')
	msg['Subject'] = subject
	msg['From'] = AppConfig().get('email', 'from', fallback='no-reply@leithwheeler.com')
	msg['To'] = ','.join(recipients)

	for f in files or []:
		with open(f, "rb") as fil:
			part = MIMEApplication(
				fil.read(),
				Name=basename(f)
			)
		# After the file is closed
		part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
		msg.attach(part)

	# Send the message via our SMTP server.
	mail_server = SMTP(AppConfig().get('email', 'mail_server', fallback='leithwheeler-com.mail.protection.outlook.com'))
	mail_server.sendmail(msg['From'], recipients, msg.as_string())
	mail_server.quit()


def send_email(subject, body='', recipients=None, files=None):
	"""
	Send an email

	:param subject: The subject
	:param body: Optional. Will be sent as html and plaintext
	:param recipients: A list of emails to send to. If not specified the email will be sent to the
						list defined in the ALERT_EMAIL_TO setting
	:param files: Optional. A list of files (full paths and filenames) to attach.
	:return: None
	"""

	# Create a text/plain message
	msg = MIMEMultipart('alternative')
	msg['Subject'] = subject
	msg['From'] = AppConfig().get('email', 'from', fallback='no-reply@leithwheeler.com')
	msg['To'] = ','.join(recipients)

	# Add plaintext and html
	part1 = MIMEText(body, 'plain')
	msg.attach(part1)

	html = HTML_TEMPLATE.format(body=body)
	part2 = MIMEText(html, 'html')
	msg.attach(part2)

	for f in files or []:
		with open(f, "rb") as fil:
			part = MIMEApplication(
				fil.read(),
				Name=basename(f)
			)
		# After the file is closed
		part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
		msg.attach(part)

	# Send the message via our SMTP server.
	mail_server = SMTP(AppConfig().get('email', 'mail_server', fallback='leithwheeler-com.mail.protection.outlook.com'))
	mail_server.sendmail(msg['From'], recipients, msg.as_string())
	mail_server.quit()
