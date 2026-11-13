"""Strict Advisory adapter reusing the existing notification configuration and legacy API."""
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict

from notification_service import NotificationService, notification_service


class AdvisoryNotificationTransport(NotificationService):
    def __init__(self, shared=None):
        # Shared configuration is read, never changed. No second credential loader.
        self.config = (shared or notification_service).config

    def send_external_message(self, channel: str, subject: str, message: str) -> bool:
        """Explicit external acceptance; never use the legacy UI fallback."""
        from html import escape

        if channel == 'email':
            if not self.config['email_enabled'] or not all(self.config.get(k) for k in (
                'smtp_server', 'email_from', 'email_password', 'email_to'
            )):
                return False
            return self._send_custom_email(
                subject, f'<pre>{escape(message)}</pre>', message, strict_transport=True
            )
        if channel != 'webhook' or not self.config['webhook_enabled'] or not self.config['webhook_url']:
            return False
        webhook_type = self.config['webhook_type']
        if webhook_type == 'dingtalk':
            keyword = self.config.get('webhook_keyword', '')
            data = {'msgtype': 'text', 'text': {'content': f'{keyword}\n{subject}\n{message}'}}
            success_key = 'errcode'
        elif webhook_type == 'feishu':
            data = {'msg_type': 'text', 'content': {'text': f'{subject}\n{message}'}}
            success_key = 'code'
        else:
            return False
        try:
            response = self._post_webhook(data)
            code = response.json().get(success_key) if response.status_code == 200 else None
            return type(code) is int and code == 0
        except Exception:
            # A requests exception can contain the secret webhook URL.
            return False

    def _post_webhook(self, data: Dict):
        """Bounded strict HTTP transport; legacy transports remain inherited."""
        import requests
        return requests.post(
            self.config['webhook_url'], json=data,
            headers={'Content-Type': 'application/json'}, timeout=10
        )

    def _send_custom_email(self, subject: str, html_body: str, text_body: str,
                           *, strict_transport: bool = False) -> bool:
        """发送自定义邮件"""
        if not strict_transport:
            return super()._send_custom_email(subject, html_body, text_body)
        try:
            msg = MIMEMultipart('alternative')
            msg['From'] = self.config['email_from']
            msg['To'] = self.config['email_to']
            msg['Subject'] = subject

            part1 = MIMEText(text_body, 'plain', 'utf-8')
            part2 = MIMEText(html_body, 'html', 'utf-8')

            msg.attach(part1)
            msg.attach(part2)

            smtp_class = smtplib.SMTP_SSL if self.config['smtp_port'] == 465 else smtplib.SMTP
            with smtp_class(self.config['smtp_server'], self.config['smtp_port'], timeout=10) as server:
                if smtp_class is not smtplib.SMTP_SSL:
                    server.starttls()
                server.login(self.config['email_from'], self.config['email_password'])
                refused = server.send_message(msg)
                if refused:
                    return False

            return True

        except Exception:
            return False
