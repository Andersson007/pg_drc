#!/usr/bin/python3
# pg_drc.py - Check PostgreSQL delayed replication state.
# See README.md on the https://github.com/Andersson007/pg_drc
# for more information
#
# Author: Andrey Klychkov aaklychkov@mail.ru
# Licence: Copyleft free software
# Date: 02-04-2018

import argparse
import datetime
import logging
import os
import random
import smtplib
import socket
import sys
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import lib.database as db
from lib.common import ConfParser, Mail

__VERSION__ = '1.3.3'
HOSTNAME = socket.gethostname()  # For mail reporting
TODAY = datetime.date.today().strftime('%Y%m%d')


def parse_cli_args():
    parser = argparse.ArgumentParser(description="Check "
                                     "PostgreSQL delayed replication state")
    parser.add_argument("-c", "--config", dest="config", required=True,
                        help="path to a configuration FILE", metavar="FILE",
                        default=False)
    parser.add_argument("-r", "--recovery-conf", dest="recovery_conf",
                        default=False, help="path to a database recovery.conf",
                        required=True)

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--check", action="store_true",
                       help="check delayed replication state and "
                       "send a mail report if it does not work right")
    group.add_argument("--report", action="store_true",
                       help="print and send a report about a timelag, "
                       "the current recovery_min_apply_delay value and "
                       "difference between them")
    group.add_argument("--version", action="version",
                       version=__VERSION__, help="show version and exit")

    return parser.parse_args()

args = parse_cli_args()


# ==============================
# Parsing of configuration files
# ==============================

# List of allowable parameters in a config file:
params = ['log_dir',
          'log_pref',
          'xact_timestamp_file',
          'recovery_conf',
          'db_host',
          'db_port',
          'db_name',
          'db_user',
          'db_passwd',
          'lag_threshold',
          'log_pref',
          'mail_allow',
          'mail_subject',
          'smtp_acc',
          'mail_recipient',
          'smtp_srv',
          'smtp_port',
          'smtp_pass',
          'mail_sender']

conf_parser = ConfParser()
conf_parser.set_params(params)
conf_parser.set_config(args.config)
configuration = conf_parser.get_options()

# Main params:
XACT_TIMESTAMP_FILE = configuration['xact_timestamp_file']
LAG_THRESHOLD = int(configuration['lag_threshold'])

# Log params:
LOG_DIR = configuration['log_dir']
LOG_PREF = configuration['log_pref']
 
# Connection params:
DB_CONTYPE = 'u_socket'
DB_HOST = ''
DB_PORT = '5432'
DB_NAME = 'postgres'
DB_USER = 'postgres'
DB_PASSWD = ''

if configuration['db_host']:
    if configuration['db_host'] != 'localhost':
        DB_CONTYPE = "network"
        DB_HOST = configuration['db_host']

if configuration['db_port']:
    DB_PORT = configuration['db_port']

if configuration['db_name']:
    DB_NAME = configuration['db_name']

if configuration['db_user']:
    DB_USER = configuration['db_user']

if configuration['db_passwd']:
    DB_PASSWD = configuration['db_passwd']

# Mail params:
ALLOW_MAIL_NOTIFICATION = int(configuration['mail_allow'])
SBJ = configuration['mail_subject']
SMTP_ACC = configuration['smtp_acc']
RECIPIENT = configuration['mail_recipient'].split(',')
SMTP_SRV = configuration['smtp_srv']
SMTP_PORT = configuration['smtp_port']
SMTP_PASS = configuration['smtp_pass']
SENDER = configuration['mail_sender']


class ReplicationDelay(db._DatBase):
    def __init__(self, name):
        super().__init__(name, name)
        # Path to a database recovery.conf file:
        self.recovery_conf = ''
        # "recovery_min_apply_delay" value from a recovery.conf file:
        self.apply_delay = None
        # Current time lag of wal replaying:
        self.current_time_lag_min = 0
        # Difference between the two previous variables:
        self.delay_diff = 0

    def set_recovery_conf(self, recovery_conf):
        """Sets a database recovery.conf file"""
        try:
            fp = open(recovery_conf, 'r')
            fp.close()
            self.recovery_conf = recovery_conf
        except Exception as e:
            print(e)
            sys.exit(e.errno)

    def get_rec_min_apply_delay(self):
        """Gets the recovery_min_apply_delay setting value
        from the self.recovery_conf and sets the self.apply_delay
        attribute if the self.recovery_conf does not exist or a database is not
        in the delayed replication mode
        """
        if not self.recovery_conf:
            err = 'ReplicationDelay().get_rec_min_apply_delay(): '\
                  'the self.recovery_conf should not be an empty string'
            raise ValueError(err)

        for line in open(self.recovery_conf):
            if 'recovery_min_apply_delay' in line:
                line = line.split("'")[1]
                s = ''.join([s for s in line if s.isdigit()])
                # Replication delay parameter in minutes:
                self.apply_delay = int(s)
                return self.apply_delay

        return None

    def get_timelag(self):
        """Returns the last_xact_replay_timestamp value (run on a slave)
        """
        self.do_query("SELECT EXTRACT(EPOCH FROM (now() - "
                      "pg_last_xact_replay_timestamp()))::INT;")
        return self.cursor.fetchone()

    def get_xact_replay_tstamp(self):
        self.do_query("select pg_last_xact_replay_timestamp()")
        return self.cursor.fetchone()


    def get_current_time_lag_min(self):
        """gets a current time lag of replaying wal files on a slave
        """
        self.current_time_lag_min = self.get_timelag()[0] // 60

    def get_delay_diff(self):
        """returns difference between a timelag and an apply delay"""
        if type(self.apply_delay) is not int:
            err = 'ReplicationDelay().get_delay_diff(): '\
                  'the self.apply_dilay should be an integer'
            raise ValueError(err)

        if self.current_time_lag_min:
            self.delay_diff = self.apply_delay - self.current_time_lag_min
            return self.delay_diff
        else:
            # Delay setting not found in a recovery.conf,
            # or a database is not in recovery mode:
            return 0


def main():
    # For mail reporting:
    mail_report = Mail(ALLOW_MAIL_NOTIFICATION, SMTP_SRV, SMTP_PORT,
                       SMTP_ACC, SMTP_PASS, SENDER, RECIPIENT, SBJ)

    # Set up a logging configuration:
    log_fname = '%s/%s-%s' % (LOG_DIR, LOG_PREF, TODAY)
    row_format = '%(asctime)s [%(levelname)s] %(message)s'
    logging.basicConfig(format=row_format, filename=log_fname,
                        level=logging.INFO)
    log = logging.getLogger('index_rebuilder')


    replication = ReplicationDelay(DB_NAME)
    replication.set_log(log)
    replication.get_connect(con_type=DB_CONTYPE, host=DB_HOST,
                            pg_port=DB_PORT, user=DB_USER,
                            passwd=DB_PASSWD)

    replication.set_recovery_conf(args.recovery_conf)

    if replication.get_rec_min_apply_delay() is None:
        msg = 'WARNING: recovery_min_apply_delay param is not found '\
              'in the recovery.conf. It means the database '\
              'is not in a recovery mode'
        print(msg)
        mail_report.send(msg)
        replication.close_connect()
        sys.exit(1)

    replication.get_current_time_lag_min()
    replication.get_delay_diff()

    if args.report:
        msg = "%s - delayed replication status\n"\
              "recovery_min_apply_delay: %s min\n"\
              "current timelag is: %s min\ndiff: %s" % (HOSTNAME,
            replication.apply_delay, replication.current_time_lag_min,
            replication.delay_diff)
        print(msg)
        log.info("recovery_min_apply_delay: %s min, "
                 "current timelag is: %s min, "
                 "diff: %s" % (replication.apply_delay,
                               replication.current_time_lag_min,
                               replication.delay_diff))
        mail_report.send(msg)
        replication.close_connect()
        sys.exit(0)

    # args.check by default:
    else:
        # If a timelag is more than the LAG_THRESHOLD:
        if replication.delay_diff < LAG_THRESHOLD:
            msg = "WARNING, timelag Lag (%s) more than "\
                  "LAG_THRESHOLD (%s)\nrecovery_min_apply_delay "\
                  "is %s" % (replication.current_time_lag_min,
                             LAG_THRESHOLD, replication.apply_delay)
            log.warning("recovery_min_apply_delay: %s min, "
                        "current timelag is: %s min, "
                        "diff: %s" % (replication.apply_delay,
                                      replication.current_time_lag_min,
                                      replication.delay_diff))
            mail_report.send(msg)
            replication.close_connect()
            sys.exit(0)

        # Additionally checks change of the xact_timestamp value,
        xact_timestamp = replication.get_xact_replay_tstamp()
        replication.close_connect()

        if os.path.isfile(XACT_TIMESTAMP_FILE):
            try:
                fp = open(XACT_TIMESTAMP_FILE, 'r')
            except Exception as e:
                print(e)
                send_mail(MAIL_SBJ, e)
                sys.exit(e.errno)

            for elem in fp:
                if elem:
                    prev_xact_timestamp = elem
                    break

            fp.close()

        # If the XACT_TIMESTAMP_FILE is empty,
        # writes the xact_timestamp to it and exit:
        else:
            try:
                f = open(XACT_TIMESTAMP_FILE, 'w')
                f.write(str(xact_timestamp))
                f.close()
            except Exception as e:
                print(e)
                send_mail(MAIL_SBJ, e)
                sys.exit(e.errno)

            sys.exit(0)

        if xact_timestamp == prev_xact_timestamp:
            # Probably a slave does not
            # apply any changes from wal logs
            msg = "WARNING, xact_timestamp has not been changed!"
            mail_report.send(msg)

        # Save the last xact_timestamp to a file:
        f = open(XACT_TIMESTAMP_FILE, 'w')
        f.write(str(xact_timestamp))
        f.close()


if __name__ == '__main__':
    main()
