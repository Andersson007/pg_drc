import datetime
import logging
import sys

try:
    import psycopg2
    assert psycopg2
except ImportError as e:
    print(e, "Hint: use pip3 install psycopg2-binary")
    sys.exit(1)

try:
    from yaml import load
    assert load
except ImportError as e:
    print(e, "Hint: use pip3 install pyyaml")
    sys.exit(1)

# Max lenght of a database object name:
MAX_NAME_LEN = 63

# Loading sql query templates:
SQL_FILE = './lib/database_sql.yml'
sql_templates = load(open(SQL_FILE, 'r'))


class _DatBase(object):
    """Base class of database objects that
    provides common methods and attributes
    """
    def __init__(self, name, dbname):
        self.set_name(name)
        self.set_dbname(dbname)
        self.log = None
        self.lock_query_timeout = '0'

    def set_name(self, name):
        err = self.__check_name(name)
        if err:
            raise ValueError(err)
            return False

        self.name = name

    def set_dbname(self, dbname):
        err = self.__check_name(dbname)
        if err:
            raise ValueError(err)
            return False

        self.dbname = dbname

    def __check_name(self, name):
        err = ''
        if len(name) > MAX_NAME_LEN:
            err = '_DatBase.set_name: passed name "%s" '\
                  'is too long (>%s chars)' % (name, MAX_NAME_LEN)
        elif name.isdigit():
            err = '_DatBase.set_name: passed name "%s" '\
                  'contents digits only' % name

        for c in name:
            if not c.isalpha() and not c.isdigit() and c != '_':
                err = '_DatBase.set_name: passed name "%s" '\
                      'contents not alphabetical, '\
                      'not numeric or not "_"  symbols' % name
        return err

    def set_lock_query_timeo(self, timeo):
        self.lock_query_timeo = timeo

    def set_log(self, log):
        arg_type = type(log)
        if arg_type is logging.Logger:
            self.log = log
        else:
            err = "_DatBase.set_log() requeres "\
                  "an argument as an object of the logging.Logger class, "\
                  "passed %s" % arg_type
            raise TypeError(err)
            sys.exit(1)

    def get_name(self):
        if self.name:
            return self.name
        else:
            return ''

    def get_connect(self, con_type='u_socket', host='', pg_port='5432',
                    user='postgres', passwd='', auto_commit=True):
        if not self.log:
            print("Error, attribute 'DatBase.log' is not defined")
            sys.exit(1)

        if con_type == 'u_socket':
            if user == 'postgres':
                params = 'dbname=%s user=postgres' % (self.dbname)

            else:
                params = 'dbname=%s user=%s '\
                         'password=%s' % (self.dbname, user, passwd)
        elif con_type == 'network':
            params = 'host=%s port=%s dbname=%s'\
                     'user=%s password=%s' % (host, pg_port,
                                              self.dbname, user, passwd)
        else:
            err = '_DatBase.get_connect(): '\
                  'con_type must be "u_socket" or "network"'
            raise ValueError(err)
            sys.exit(1)

        try:
            self.connect = psycopg2.connect(params)
            self.connect.set_session(autocommit=auto_commit)
            self.cursor = self.connect.cursor()
            self.do_query('SELECT version();', err_exit=True)
            self.log.info('Connection to database %s established'
                          % self.dbname)
            return self.connect
        except psycopg2.DatabaseError as e:
            print(e)
            self.log.error(e)
            return False

    def do_query(self, query, err_exit=False):
        try:
            return self.cursor.execute(query)
        except psycopg2.DatabaseError as e:
            self.log.error(e)
            if err_exit:
                sys.exit(1)
            return False

    def do_service_query(self, query, err_exit=False):
        try:
            if self.cursor.execute(query) is None:
                return True
            else:
                return False
        except KeyboardInterrupt:
            print('Query has been interrupted')
            return False
        except psycopg2.DatabaseError as e:
            print(e)
            self.log.error(e)
            return False

    def set_statement_timeout(self, timeout):
        return self.do_service_query("SET statement_timeout = '%s'" % timeout)

    def close_connect(self):
        try:
            self.connect.close()
        except psycopg2.DatabaseError as e:
            self.log.error(e)


class DatBaseObject(_DatBase):
    """Class for managing databases as
    an database cluster object)
    """
    def __init__(self, dbname):
        super().__init__(dbname, dbname)
        self.connect = None
        self.cursor = None
    # It may provide create/drop/alter
    # methods for example


class GlobIndexStat(_DatBase):
    """Class for showing index statistics"""
    def __init__(self, dbname):
        super().__init__('stat', dbname)

    def print_unused(self, scan_counter=0, size_threshold=0):
        """Print unused indexes with"""
        self.do_query(sql_templates['IDX_SCAN_STAT_SQL'] %
                      (scan_counter, size_threshold))
        stat = self.cursor.fetchall()

        print(' n   {:{}{}}{:{}{}}{:{}{}}{:{}{}}'
              .format('| iname', '<', '66', '| size', '<', '10',
                      '| usage', '<', '8', '| tname', '<', '42'))

        print('-' * 120)

        i = 0
        row_num = len(stat)
        for s in stat:
            if i == row_num:
                break

            print('{:{}{}} | {:{}{}}| {:{}{}}| {:{}{}}| {:{}{}}'
                  .format(i, '>', '4', s[0], '<', '64', s[1], '<', '8',
                          s[2], '<', '6', s[3], '<', '42'))
            i += 1

    def print_bloat_top(self):
        """Print top of bloated indexes"""
        self.do_query(sql_templates['IDX_BLOAT_STAT_SQL'])
        stat = self.cursor.fetchall()

        if stat:
            print('{:{}{}}{:{}{}}{:{}{}}'
                  '{:{}{}}{:{}{}}{:{}{}}'
                  .format('n', '^', '4', '| tname', '<', '48',
                          '| iname', '<', '64', '|     size', '<', '11',
                          '| bloat', '<', '11', '| ratio', '<', 8))

            print('-' * 146)

            for s in stat:
                print('{:{}{}} | {:{}{}} | {:{}{}} | '
                      '{:{}{}} | {:{}{}} | {:{}{}}'
                      .format(s[0], '>', '3', s[1], '<', '45',
                              s[2], '<', '61', s[3], '<', '8',
                              s[4], '<', '8', s[5], '<', '8'))

        else:
            print('No bloated indexes found')

    def print_invalid(self):
        """Print invalid indexes"""
        self.do_query(sql_templates['GET_INVALID_IDX'])
        inv_idx_list = self.cursor.fetchall()

        if inv_idx_list:
            print('Invalid indexes found:')
            print('=' * 22)
            for s in inv_idx_list:
                print('%s' % s[0])
        else:
            print('No invalid indexes found')


class _Relation(_DatBase):
    """Basic relation class
    (for table/index database objects)
    """
    def __init__(self, name, dbname):
        super().__init__(name, dbname)
        self.relkind = ''
        self.relsize = 0

    def check_relation(self, name=""):
        """Check relation existance"""
        if not name:
            relname = self.name
        else:
            relname = name

        if self.__check_name(name):
            return False

        self.do_query(sql_templates['GET_RELNAME'] % relname)

        if self.cursor.fetchone():
            return True
        else:
            return False

    def get_relkind(self, relname=''):
        """Get a kind of a relation:
        1) False - if relation does not exist
        2) 'i' - if relation is an index
        3) 'r' - if relation is a table
        """
        if not relname:
            relname = self.name

        self.do_query(sql_templates['GET_RELKIND_SQL'] % relname)
        self.relkind = self.cursor.fetchone()
        if self.relkind:
            self.relkind = self.relkind[0]

        return self.relkind

    def get_relsize(self):
        self.do_query(sql_templates['GET_RELSIZE_SQL'] % self.name)
        size = self.cursor.fetchone()[0]
        self.relsize = int(size)

        return self.relsize


class Index(_Relation):
    """Class for working with indexes"""
    def __init__(self, name, dbname):
        super().__init__(name, dbname)
        self.idef = ''
        self.icomment = ''
        self.__tmp_name = ''
        self.__create_new_cmd = ''

    def get_indexdef(self):
        """Get index definition - in fact its creation command"""
        self.do_query(sql_templates['GET_IDXDEF_SQL'] % self.name)
        self.idef = self.cursor.fetchone()[0]
        if 'UNIQUE' in self.idef:
            self.log.error('It\'s UNIQUE or PRIMARY KEY. Exit')
            return False
        return True

    def set_idef(idef):
        if type(idef) is str:
            self.idef = idef
        else:
            err = "Index(): index definition "\
                  "must be passed as string"
            raise TypeError(err)
            sys.exit(1)

    def check_validity(self, iname=''):
        """check_validity(iname):
        the method checks index validity
        """
        if not iname:
            iname = self.name

        self.do_query(sql_templates['CHECK_IDXVALID_SQL'] % iname)
        self.valid = self.cursor.fetchone()[0]

        return self.valid

    def get_indexcomment(self):
        """Get a comment of index if it exists"""
        self.do_query(sql_templates['GET_IDXCOMMENT_SQL'] % self.name)
        self.icomment = self.cursor.fetchone()[0]

    def __get_tmp_name(self, pref):
        """Make a temporary name
        of a new index using concatenation
        of "pref" and a current index name
        """
        self.__tmp_name = pref+self.name
        return self.__tmp_name

    def __make_creat_new_cmd(self):
        """Make a creation command for a new index"""
        if not self.__tmp_name:
            err = 'Index.__make_creat_new_cmd(): '\
                  'self.__tmp_name must be predefined'
            raise ValueError(err)
            sys.exit(1)

        c = self.idef.split()
        c[1] = 'INDEX CONCURRENTLY'
        c[2] = self.__tmp_name
        self.__creat_new_cmd = ' '.join(c)

    def create_new(self):
        return self.do_service_query(self.__creat_new_cmd)

    def drop(self, iname):
        return self.do_service_query('DROP INDEX %s' % iname)

    def rename(self, src_iname, final_iname):
        return self.do_service_query('ALTER INDEX %s RENAME TO %s' %
                                     (src_iname, final_iname))

    def add_comment(self, iname, icomment):
        return self.do_service_query("COMMENT ON INDEX %s IS '%s';" %
                                     (iname, icomment))

    def rebuild(self):
        """Rebuild index concurrently (without table locking)"""
        # For exec time statistics:
        start_time = datetime.datetime.now()

        # If a relation does not exist or if it isn't an index,
        # exit the function:
        relkind = self.get_relkind()
        if not relkind:
            msg = '%s: relation does not exist. Exit' % self.name
            self.log.error(msg)
            return False

        if relkind != 'i':
            msg = '%s: relation is not an index. Exit' % self.name
            self.log.error(msg)
            return False

        # For size difference after/before statistics:
        prev_size = self.get_relsize()
        self.log.info('Start to rebuild of %s, '
                      'current size: %s bytes' % (self.name, prev_size))

        #
        # 1. Check validity of a current index
        #
        if not self.check_validity():
            msg = '%s: index is invalid. Check it' % self.name
            self.log.warning(msg)
            return False
        else:
            self.log.info('Index is valid')

        #
        # 2. Get current index definition
        #
        self.get_indexdef()

        #
        # 3. Get an index comment if it exists
        #
        self.get_indexcomment()

        #
        # 4. Get a temporary name for a new index
        #
        self.__get_tmp_name('new_')

        # If a relation does not exist or if it's not an index,
        # exit the function:
        relkind = self.get_relkind(self.__tmp_name)
        if relkind:
            if not self.check_validity(self.__tmp_name):
                msg = '%s: relation exists now and '\
                      'it\'s invalid. Exit' % self.__tmp_name
            else:
                msg = '%s: relation exists now. Exit' % self.__tmp_name

            self.log.error(msg)
            return False

        #
        # 5. Make a creation command for a new index
        #
        self.__make_creat_new_cmd()

        #
        # 6. Create a new index
        #
        self.log.info('Try: %s' % self.__creat_new_cmd)
        if self.create_new():
            self.log.info('Creation has been complited')
        else:
            msg = '%s: creation FAILED' % self.__tmp_name
            self.log.error(msg)
            return False

        #
        # 7. Add a comment on a new index if it exists on an old index
        #
        if self.icomment:
            self.log.info("Add comment: '%s'" % self.icomment)
            if self.add_comment(self.__tmp_name, self.icomment):
                self.log.info('Comment has been added')
            else:
                msg = '%s: comment has NOT been added' % self.__tmp_name
                self.log.warning(msg)

        #
        # 8. Check validity of a new index
        #
        if not self.check_validity(self.__tmp_name):
            # If the index is invalid, exit the function
            msg = 'New index %s is invalid. ' % self.__tmp_name
            msg += 'Check and drop it manually'
            self.log.warning(msg)
            return False
        else:
            self.log.info('New index %s is valid, continue' % self.__tmp_name)

        #
        # 9. Drop an old index
        #
        self.log.info('Try to drop index %s' % self.name)

        # Index dropping / altering locks a table,
        # therefore it needs to set allowable statement timeout
        # for this action in order to prevent queues of queries:
        if self.set_statement_timeout(self.lock_query_timeo):
            self.log.info("Set statement timeout '%s': success" %
                          self.lock_query_timeo)
        else:
            self.log.info("Set statement timeout '%s': failure" %
                          self.lock_query_timeo, ERR)

        if self.drop(self.name):
            self.log.info('Dropping done')
        else:
            # If index has not been dropped, exit the function:
            msg = '%s: rebuilding FAILED, '\
                  'index is NOT dropped' % self.name
            self.log.warning(msg)
            return False

        #
        # 10. Rename a new index
        #
        # If the previous step (dropping of a current index)
        # was done successfully,
        # rename the new index to a persistant name
        # (as the name of the dropped index)
        self.log.info('Try to rename index %s to %s' % (
                      self.__tmp_name, self.name))
        if self.rename(self.__tmp_name, self.name):
            self.log.info('Renaming is done')
        else:
            msg = '%s: renaming FAILED. Do it manually' % self.__tmp_name
            self.log.warning(msg)
            return False

        #
        # Reset a statement timeout that was established previously:
        #
        if self.set_statement_timeout('0'):
            self.log.info("Reset statement timeout to '0': success")
        else:
            self.log.info("Reset statement timeout to '0': failure", ERR)

        # Make time execution statictics and return it:
        fin_size = self.get_relsize()
        diff = prev_size - fin_size

        end_time = datetime.datetime.now()
        exec_time = end_time - start_time
        stat = '%s: done. Size (in bytes): prev %s, '\
               'fin %s, diff %s, exec time %s' % (self.name, prev_size,
                                                  fin_size, diff, exec_time)
        self.log.info(stat)

        return stat
