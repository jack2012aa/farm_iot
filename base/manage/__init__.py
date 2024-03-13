"""Define classes to manage works done in different hierarchies."""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dataclasses import dataclass
from abc import ABC, abstractmethod

from general import type_check


@dataclass
class Report:
    """A dataclass help `Worker` objects communicate with `Manager` objects.
    
    :param sign: sign of the reporting object.
    :param content: the report content.
    """
    
    sign: object = None
    content: object = None


class Manager(ABC):

    def __init__(self, email_settings: dict = None) -> None:
        """A class which manages `Worker` objects and handles there report.
        
        email settings should look like:
        {
            "subject": 
            "from":
            "domain_mail_account":
            "domain_mail_password":
            "employeer_emails: {
                "A": "xxxx@gmail.com", 
                "B": "yyyy@gmail.com"
            }
        }

        :param email_settings: settings used for sending alarm email.
        """
        self.workers: set[Worker] = set()
        self.email_settings = email_settings

    @abstractmethod
    async def handle(self, report: Report) -> None:
        return NotImplemented
    
    @abstractmethod
    async def initialize(self, path: str) -> None:
        return NotImplemented
    
    async def send_alarm_email(self, recipients: tuple[str], content_text: str) -> None:
        """Send an alarm email to recipients' email address.

        Invalid emails and recipients will be ignored.
        
        :param recipients: emails of recipients.
        :param content_text: content in the email.
        :raises: TypeError, ValueError.
        """

        if self.email_settings is None:
            raise ValueError("Email settings should not be None if you want to send an email.")
        type_check(recipients, "recipients", tuple)
        type_check(content_text, "content_text", str)

        content = MIMEMultipart()
        content["subject"] = self.email_settings["subject"]
        content["from"] = self.email_settings["from"]
        emails = []
        for recipient in recipients:
            address = self.email_settings["employeer_emails"].get(recipient)
            if address is not None:
                emails.append(address)
        content["to"] = ", ".join(emails)
        content.attach(MIMEText(content_text))
        with smtplib.SMTP_SSL(host="smtp.gmail.com", port=465, local_hostname="[127.0.0.1]") as smtp:
            smtp.ehlo_or_helo_if_needed()
            smtp.login(
                self.email_settings["domain_mail_account"],
                self.email_settings["domain_mail_password"]
            )
            smtp.send_message(content)


class SimpleManager(Manager):
    """A simple manager which print the report."""

    def __init__(self, email_settings: dict = None) -> None:
        super().__init__(email_settings)

    async def initialize(self, path: str) -> None:
        return None
    
    async def handle(self, report: Report) -> None:
        print(type(report.sign), report.content)
        return None


class Worker(ABC):

    def __init__(self) -> None:
        """A class which do works and report to a manager."""
        self.__manager: Manager = None

    def set_manager(self, manager: Manager) -> None:
        """Set the manager of the worker.
        
        :param manager: manager to report when something happens.
        """
        if manager is None:
            return
        type_check(manager, "manager", Manager)
        self.__manager = manager

    async def notify_manager(self, report: Report) -> None:
        """Report to manager.
        
        :param report: things to report.
        """
        await self.__manager.handle(report)