import smtplib
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class ConfParser():
    """Class for parsing a passed configuration file,
    returns a dictionary{param: value}
    """
    def __init__(self, param_list=[]):
        self.set_params(param_list)

    def set_params(self, param_list):
        if type(param_list) is list:
            self.param_list = param_list
        else:
            err = "ConfParser(): allowable parameters "
            err += "must be passed as list"
            raise TypeError(err)
            sys.exit(1)

    def set_config(self, conf_file):
        try:
            self.conf_file = open(conf_file, 'r')
        except Exception as e:
            print(e)
            sys.exit(e.errno)

    def get_options(self):
        if not self.param_list:
            err = "ConfParser(): param_list is empty "
            err += "but it must be initialized "
            err += "with the set_params(param_list) method"
            raise ValueError(err)
            sys.exit(1)

        confdict = {}
        for line in self.conf_file:
            if '=' in line:
                line = line.strip()
                if line and line[0] != '#':
                    line = line.split('=')
                    param = line[0].strip()

                    if param in self.param_list:
                        value = line[1].split('#')[0].strip()
                    else:
                        print("Error: unrecognized param %s" % param)
                        sys.exit(1)

                    confdict[param] = value

        self.conf_file.close()
        return confdict


class Mail():
    """Class for mail reporting.
    __init_(self, allow, smtp_srv, smtp_port, smtp_acc,
    smtp_pass, sender, recip_list, sbj)
    If you want to send mail notifications,
    pass "allow" param as True. If you don't want to do it,
    pass "False" respectively
    """
    def __init__(self, allow, smtp_srv, smtp_port,
                 smtp_acc, smtp_pass,
                 sender, recip_list, sbj):

        self.allow = allow
        self.smtp_srv = smtp_srv
        self.smtp_port = smtp_port
        self.smtp_acc = smtp_acc
        self.smtp_pass = smtp_pass
        self.sender = sender
        self.recip_list = recip_list
        self.sbj = sbj

    def send(self, ms):
        if self.allow:
            msg = MIMEMultipart()
            msg['Subject'] = (self.sbj)
            msg['From'] = self.sender
            msg['To'] = self.recip_list[0]
            msg.attach(MIMEText(ms, 'plain'))
            smtpconnect = smtplib.SMTP(self.smtp_srv, self.smtp_port)
            smtpconnect.starttls()
            smtpconnect.login(self.smtp_acc, self.smtp_pass)
            smtpconnect.sendmail(self.smtp_acc, self.recip_list,
                                 msg.as_string())
            smtpconnect.quit()
        else:
            pass
