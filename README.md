# pg_drc.py
pg_drc.py - Check PostgreSQL delayed replication state.

Author: Andrey Klychkov aaklychkov@mail.ru

Licence: Copyleft free software

Version: 1.3.3

Date: 11-04-2018

### Description:
The utility gets the last_xact_replay_timestamp value from a specified database,
computes a time lag and compares a result with the 'recovery_min_apply_delay' option
from a recovery.conf file.

Two modes are available:

--report  -Shows and sends by email (if allowed) a message about replication state (see examples below). It's suitable for daily reporting.

--check   -Checks replication state quietly. If the current time lag is more than the threshold value from pg_drc.conf, sends warning by email (if allowed in there). It's suitable for hourly checks by crontab.


### Requirements:

Python3+, psycopg2, pyyaml

### Synopsis:
```
pg_drc.py [-h] -c FILE -r RECOVERY_CONF [--check | --report | --version]
```
**Options:**
```
  -h, --help            show this help message and exit
  -c FILE, --config FILE
                        path to a configuration FILE
  -r RECOVERY_CONF, --recovery-conf RECOVERY_CONF
                        path to a database recovery.conf
  --check               check delayed replication state and send a mail report
                        if it does not work right
  --report              print and send a report about a timelag, the current
                        recovery_min_apply_delay value and difference between
                        them
  --version             show version and exit
```

**Notice:** for sending of mail reports SEND_MAIL option must be set to not null.

### Configuration:

Configuration file allows to set up:
- allowable lag of replication
- database connection params
- email notifications
- path to a tmp file of the utility
- path to a recovery.conf of a slave server
- path to a log file

See the pg_drc.conf.example file.

### Examples:
```
./pg_drc.py -c pg_drc.conf --report -f ~postgres/9.4/data/recovery.conf
```
Shows and, if allowed, sends a message about replication state similar as below:
```
myserver.lan - delayed replication status
recovery_min_apply_delay: 360 min
current timelag is: 360 min
diff: 0
```
Example of a warning nitification (runned with --check option)
```
Lag (789) more than LAG_THRESHOLD (-60)
recovery_min_apply_delay is 720
```

```
./pg_drc.py -c pg_drc.conf --check -f ~postgres/9.4/data/recovery.conf
```
Checks replication state quietly.
