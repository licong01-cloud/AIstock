"""Strict Advisory adapter reusing the existing notification configuration and legacy API."""
import base64
import hashlib
import hmac
import json
import os
import smtplib
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict
from urllib.parse import urlsplit

from notification_service import NotificationService, notification_service


class ExternalNotificationDeliveryError(RuntimeError):
    """Sanitized transport failure; the Advisory queue owns retry and recording."""


class AdvisoryNotificationTransport(NotificationService):
    def __init__(self, shared=None):
        # Shared configuration is read, never changed. No second credential loader.
        self.config = (shared or notification_service).config
        if os.getenv('AISTOCK_ADVISORY_FEISHU_ENABLED', '').strip().lower() in {
            '1', 'true', 'yes', 'on'
        }:
            self.config = dict(self.config)
            url = os.getenv('AISTOCK_ADVISORY_FEISHU_WEBHOOK_URL', '').strip()
            valid = _valid_feishu_webhook(url)
            details = os.getenv('AISTOCK_ADVISORY_FEISHU_DETAILS_ENABLED', '').strip().lower() in {
                '1', 'true', 'yes', 'on'
            }
            self.config.update(
                advisory_feishu_completion_only=True,
                advisory_feishu_details=details,
                advisory_feishu_configuration_error=(
                    None if valid else 'FEISHU_WEBHOOK_URL_INVALID' if url
                    else 'FEISHU_WEBHOOK_URL_MISSING'
                ),
                webhook_enabled=True,
                webhook_type='feishu',
                webhook_url=url if valid else '',
                webhook_signing_secret=os.getenv('AISTOCK_ADVISORY_FEISHU_SIGNING_SECRET', ''),
            )
            if details and not all(os.getenv(k, '').strip() for k in (
                'AISTOCK_ADVISORY_FEISHU_APP_ID', 'AISTOCK_ADVISORY_FEISHU_APP_SECRET',
                'AISTOCK_ADVISORY_FEISHU_CHAT_ID',
            )):
                self.config.update(webhook_url='',
                                   advisory_feishu_configuration_error='FEISHU_DETAILS_CONFIGURATION_MISSING')

    def detail_delivery(self):
        from .feishu_sheets import DetailSheetDelivery, FeishuSheetsClient
        return DetailSheetDelivery(FeishuSheetsClient(*(
            os.getenv('AISTOCK_ADVISORY_FEISHU_' + k, '').strip()
            for k in ('APP_ID', 'APP_SECRET', 'CHAT_ID')
        )))

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
            secret = self.config.get('webhook_signing_secret')
            if secret:
                timestamp = str(int(time.time()))
                key = (timestamp+'\n'+secret).encode('utf-8')
                signature = base64.b64encode(hmac.new(key, digestmod=hashlib.sha256).digest()).decode('ascii')
                data.update(timestamp=timestamp, sign=signature)
            if len(json.dumps(data, allow_nan=False).encode('utf-8')) > 20 * 1024:
                raise ExternalNotificationDeliveryError('FEISHU_MESSAGE_TOO_LARGE')
            success_key = 'code'
        else:
            return False
        try:
            response = self._post_webhook(data)
            code = response.json().get(success_key) if response.status_code == 200 else None
            if (webhook_type == 'feishu' and self.config.get('advisory_feishu_details')
                    and type(code) is not int):
                raise ExternalNotificationDeliveryError('FEISHU_MESSAGE_OUTCOME_UNKNOWN')
            return type(code) is int and code == 0
        except Exception:
            # A requests exception can contain the secret webhook URL.
            raise ExternalNotificationDeliveryError('WEBHOOK_TRANSPORT_ERROR') from None

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
            raise ExternalNotificationDeliveryError('EMAIL_TRANSPORT_ERROR') from None


def _valid_feishu_webhook(url: str) -> bool:
    try:
        parts = urlsplit(url)
        prefix = '/open-apis/bot/v2/hook/'
        token = parts.path.removeprefix(prefix)
        return bool(
            parts.scheme == 'https'
            and parts.hostname in {'open.feishu.cn', 'open.larksuite.com'}
            and parts.port in {None, 443}
            and not parts.username and not parts.password
            and not parts.query and not parts.fragment
            and parts.path.startswith(prefix) and token and '/' not in token
        )
    except ValueError:
        return False
