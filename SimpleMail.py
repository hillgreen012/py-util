#!/usr/bin/env python
# coding=utf-8

import smtplib
import email
from email.message import Message

class SimpleMail:
    def __init__(self, server, port, user, passwd):
        self.server = server
        self.user = user

        self.smtp = None
        self.smtp = smtplib.SMTP(server, port)
        self.smtp.ehlo()
        self.smtp.login(user, passwd)

        assert self.smtp

    def sendMail(self, tolist, subject, content):
        msg = Message()
        msg['Mime-Version'] = '1.0'
        msg['From'] = self.user
        msg['To'] = ', '.join(tolist)
        msg['Subject'] = subject
        msg['Date'] = email.Utils.formatdate()
        msg.set_payload(content)
        self.smtp.sendmail(self.user, tolist, str(msg))
        
if __name__ == "__main__":
    import getpass as gp
    server = '10.0.33.25'
    user = 'xdata@z152.com'
    port = 25                 # really a string?
    # mailer = SimpleMail(server, port, user, gp.getpass('Input %s Password: ' % user))
    mailer = SimpleMail(server, port, user, 'xdata')
    tolist = ['test@z152.com']
    subject = 'test mail'
    text = 'content'
    mailer.sendMail(tolist, subject, text)
    # import getpass as gp
    # server = 'smtp.qq.com'
    # user = 'xds-test@qq.com'
    # port = 25                 # really a string?
    # tolist = ['']
    # subject = 'test python'
    # text = 'content'
    # mailer = SimpleMail(server, port, user, gp.getpass('Input %s Password: ' % user))
    # mailer.sendMail(tolist, subject, text)
