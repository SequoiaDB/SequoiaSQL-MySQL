#! /usr/bin/python
# -*- coding: utf-8 -*-
# @Author Yang Shangde
import ConfigParser
import base64
import csv
import logging.config
import random
import sched
import socket
import traceback

from datetime import datetime

import subprocess
import time
import os
import sys
import re
import io
from keywords import *

reload(sys)
sys.setdefaultencoding('utf8')

# MySQL error definitions
MYSQL_OK = 0
CONN_ERR = 1
SYNTAX_ERR = 2
SYNTAX_ERR_2 = 3
UNHANDLED_ERR = 10000

# Error key pattern in the error message.
MYSQL_ERRORS = {
    CONN_ERR: "ERROR 2003",
    SYNTAX_ERR: "ERROR 1604",
    SYNTAX_ERR_2: "ERROR 1064"
}


class CryptoUtil:
    def __init__(self):
        pass

    @classmethod
    def encrypt(cls, source_str):
        random_choice = ("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
                         "1234567890!@#$%^&*()")
        to_encrypt_arr = []
        shift_str = ""
        for char in source_str:
            shift_str = shift_str + chr(ord(char) + 3)
        shift_index = 0
        for index in range(0, len(shift_str) * 3):
            if index % 3 != 0:
                rand_char = random.choice(random_choice)
                to_encrypt_arr.append(rand_char)
            else:
                to_encrypt_arr.append(shift_str[shift_index])
                shift_index = shift_index + 1
        to_encrypt_str = ''.join(to_encrypt_arr)
        encrypt_str = base64.b64encode(to_encrypt_str)
        return encrypt_str

    @classmethod
    def decrypt(cls, encrypt_str):
        decrypt_str = base64.b64decode(encrypt_str)
        shift_str = []
        for index in range(len(decrypt_str)):
            if index % 3 == 0:
                shift_str.append(decrypt_str[index])
        source_arr = []
        for char in shift_str:
            source_arr.append(chr(ord(char) - 3))
        source_str = "".join(source_arr)
        return source_str


class DateUtils:

    def __init__(self):
        pass

    @classmethod
    def get_current_date(cls):
        """get current time of year-month-day format

        :return: time of year-month-day format
        """
        return datetime.now().strftime('%Y-%m-%d')

    @classmethod
    def get_current_time(cls):
        """get current time of year-month-day hour:minute:second.microsecond
           format

        :return: time of year-month-day hour:minute:second.microsecond format
        """
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

    @classmethod
    def timestamp_to_datetime(cls, timestamp):

        local_dt_time = datetime.fromtimestamp(timestamp / 1000000.0)
        return local_dt_time

    @classmethod
    def datetime_to_strtime(cls, datetime_obj, date_format):
        local_str_time = datetime_obj.strftime(date_format)
        return local_str_time

    @classmethod
    def datetime_to_timestamp(cls, datetime_obj):
        local_timestamp = long(time.mktime(
            datetime_obj.timetuple()) * 1000000.0 + datetime_obj.microsecond)
        return local_timestamp

    @classmethod
    def strtime_to_datetime(cls, timestr, date_format):
        local_datetime = datetime.strptime(timestr, date_format)
        return local_datetime

    @classmethod
    def timestamp_to_strtime(cls, timestamp, date_format):
        return cls.datetime_to_strtime(cls.timestamp_to_datetime(timestamp),
                                       date_format)

    @classmethod
    def strtime_to_timestamp(cls, timestr, date_format):
        try:
            local_str_time = cls.datetime_to_timestamp(
                cls.strtime_to_datetime(timestr, date_format))
            return local_str_time
        except Exception:
            return 0

    @classmethod
    def get_file_ctime_timestamp(cls, f):
        return cls.datetime_to_timestamp(
            datetime.fromtimestamp(os.path.getctime(f)))

    @classmethod
    def get_file_mtime_timestamp(cls, f):
        return cls.datetime_to_timestamp(
            datetime.fromtimestamp(os.path.getmtime(f)))

    @staticmethod
    def compare_mtime(x, y):
        x_mtime = x["mtime"]
        y_mtime = y["mtime"]
        if x_mtime < y_mtime:
            return -1
        elif x_mtime > y_mtime:
            return 1
        else:
            return 0


class Logger:
    def __init__(self):
        self.logger = None

    def init(self, log_config_file):
        try:
            # Get the log file path from the log configuration file, and create
            # the directory if it dose not exist.
            config_parser = ConfigParser.ConfigParser()
            files = config_parser.read(log_config_file)
            if len(files) != 1:
                print("[Error] Read log configuration file failed")
                return 1
            log_file = config_parser.get("handler_rotatingFileHandler",
                                         "args").split('\'')[1]
            curr_path = os.path.abspath(os.path.dirname(log_config_file))
            log_file_full_path = os.path.join(curr_path, log_file)
            log_file_parent_dir = \
                os.path.abspath(os.path.join(log_file_full_path, ".."))
            if not os.path.exists(log_file_parent_dir):
                os.makedirs(log_file_parent_dir)

            logging.config.fileConfig(log_config_file)
            self.logger = logging.getLogger("ddlLogger")
            return 0
        except BaseException as e:
            print("[Error] Initialize logging failed. Error Message: " +
                  e.message)
            return 1

    def get_logger(self):
        return self.logger


class OptionMgr:
    def __init__(self):
        self.options = {
            KW_MYSQL: {
                KW_HOSTS: '',
                KW_MYSQL_PWD_TYPE: 0,
                KW_MYSQL_USER: 'sdbadmin',
                KW_MYSQL_PWD: 'sdbadmin',
                KW_PORT: 3306,
                KW_MYSQL_INST_DIR: '/opt/sequoiasql/mysql',
                KW_AUDIT_DIR: '/opt/sequoiasql/mysql/database/auditlog',
                KW_AUDIT_FILE: 'server_audit.log'
            },
            KW_EXECUTE: {
                KW_INTERVAL: 5,
                KW_IGNORE_ERR: True,
                KW_RETRY_TIMES: 5
            }
        }
        self.parser = None

    def __update_conf(self):
        self.parser.write(open(config_file, 'w'))

    def __validate(self):
        if 0 == len(self.get_hosts()):
            logging.error('No valid host is configured in the config file')
            return 1

        pwd_type = self.get_mysql_passwd_type()
        if 0 != pwd_type and 1 != pwd_type:
            logging.error('MySQL password type [{}] in configuration file is '
                          'invalid'.format(pwd_type))
            return 1

        if 0 == len(self.get_mysql_user()):
            logging.error('MySQL user name in configuration file is empty')
            return 1

        if 0 == len(self.get_mysql_passwd()):
            logging.error('MySQL user password in configuration file is empty')
            return 1

        interval = self.get_scan_interval()
        if interval < 1 or interval > 3600:
            logging.error('Value of interval_time [{}] in configuration file '
                          'is invalid'.format(interval))
            return 1

        ignore_error = self.get_option(KW_EXECUTE, KW_IGNORE_ERR).lower()
        if 'true' != ignore_error and 'false' != ignore_error:
            logging.error('Value of ignore_error [{}] in configuration file is '
                          'invalid'.format(ignore_error))
            return 1

        retry_limit = self.get_retry_limit()
        if retry_limit < 1 or retry_limit > 1000:
            logging.error('Value of max_retry_times [{}] in configuration file '
                          'is invalid'.format(retry_limit))
            return 1

        mysql_exec = os.path.join(self.get_mysql_home(), 'bin/mysql')
        if not os.path.exists(mysql_exec):
            logging.error('mysql is not found in the configured path: {}'
                          .format(self.get_mysql_home()))
            return 1

        audit_file = os.path.join(self.get_audit_log_path(),
                                  self.get_audit_log_name())
        if not os.path.exists(audit_file):
            logging.error('Audit file {} is not found'.format(audit_file))
            return 1

        return 0

    def __post_load(self):
        refresh_config = False

        # Add options which are not in the configuration file, with default
        # values.
        for section, options in self.options.iteritems():
            for option, value in options.iteritems():
                if not self.parser.has_option(section, option):
                    if not self.parser.has_section(section):
                        self.parser.add_section(section)
                    self.parser.set(section, option, str(value))
                    if not refresh_config:
                        refresh_config = True

        # Remove self host from the host list.
        hosts = self.get_hosts()
        self_host = socket.gethostname()
        if self_host in hosts:
            hosts.remove(self_host)
            hosts = ','.join(hosts)
            self.parser.set(KW_MYSQL, KW_HOSTS, hosts)
            if not refresh_config:
                refresh_config = True

        rc = self.__validate()
        if 0 != rc:
            logging.error('Validate configuration failed: {}'.format(rc))
            return rc

        # Encrypt the password in the configuration file if it's in plain text.
        if not self.is_mysql_pwd_encrypt():
            password = CryptoUtil.encrypt(self.get_mysql_passwd())
            self.parser.set(KW_MYSQL, KW_MYSQL_PWD, password)
            self.parser.set(KW_MYSQL, KW_MYSQL_PWD_TYPE, '1')
            if not refresh_config:
                refresh_config = True
        if refresh_config:
            self.__update_conf()

        return 0

    def load_configs(self, config_file):
        config_file = os.path.join(my_home, config_file)
        if not os.path.exists(config_file):
            logger.error('Configuration file {} dose not exist'.format(
                self.config_file))
            return 1
        self.parser = ConfigParser.ConfigParser()
        self.parser.read(config_file)
        rc = self.__post_load()
        if 0 != rc:
            logging.error('Load configurations failed: {}'.format(rc))
            return rc

        return 0

    def get_option(self, section, option):
        return self.parser.get(section, option)

    def get_hosts(self):
        return self.get_option(KW_MYSQL, KW_HOSTS).replace(' ', '').split(',')

    def get_port(self):
        return self.get_option(KW_MYSQL, KW_PORT)

    def get_mysql_home(self):
        return self.get_option(KW_MYSQL, KW_MYSQL_INST_DIR)

    def get_mysql_user(self):
        return self.get_option(KW_MYSQL, KW_MYSQL_USER)

    def get_mysql_passwd_type(self):
        return int(self.get_option(KW_MYSQL, KW_MYSQL_PWD_TYPE))

    def is_mysql_pwd_encrypt(self):
        return 1 == self.get_mysql_passwd_type()

    def get_mysql_passwd(self):
        password = self.get_option(KW_MYSQL, KW_MYSQL_PWD)
        if self.is_mysql_pwd_encrypt():
            password = CryptoUtil.decrypt(password)
        return password

    def get_scan_interval(self):
        return int(self.get_option(KW_EXECUTE, KW_INTERVAL))

    def ignore_error(self):
        return 'true' == self.get_option(KW_EXECUTE, KW_IGNORE_ERR).lower()

    def get_retry_limit(self):
        return int(self.get_option(KW_EXECUTE, KW_RETRY_TIMES))

    def get_audit_log_path(self):
        return self.get_option(KW_MYSQL, KW_AUDIT_DIR)

    def get_audit_log_name(self):
        return self.get_option(KW_MYSQL, KW_AUDIT_FILE)


class StatMgr:
    def __init__(self):
        self.stat_file = None
        self.parser = None
        self.file_last_mod_time = 0
        self.file_first_line_time = 0
        self.file_first_line_thread_id = 0
        self.file_first_line_seq = 0
        self.last_parse_row = 0

    def __init_stat_file(self):
        self.set_file_last_mod_time(0)
        self.set_file_first_line_time(0)
        self.set_file_first_line_thread_id(0)
        self.set_file_first_line_seq(0)
        self.set_last_parse_row(0)
        self.update_stat()

    def load_stat(self, stat_file):
        self.parser = ConfigParser.ConfigParser()
        self.stat_file = stat_file
        if not os.path.exists(stat_file):
            logger.warn('Status file {} dose not exist. Init it with default '
                        'values'.format(stat_file))
            self.__init_stat_file()

        self.parser.read(stat_file)
        stat_sec_name = 'status'

        self.file_last_mod_time = DateUtils.strtime_to_timestamp(
            self.parser.get(stat_sec_name, 'file_last_modified_time'),
            "%Y-%m-%d-%H:%M:%S.%f")
        self.file_first_line_time = DateUtils.strtime_to_timestamp(
            self.parser.get(stat_sec_name, 'file_first_line_time'),
            "%Y-%m-%d-%H:%M:%S")
        self.file_first_line_thread_id = \
            int(self.parser.get(stat_sec_name, 'file_first_line_thread_id'))
        self.file_first_line_seq = int(self.parser.get(stat_sec_name,
                                                       'file_first_line_seq'))
        self.last_parse_row = int(self.parser.get(stat_sec_name,
                                                  'last_parse_row'))

        return 0

    def get_file_last_mod_time(self):
        return self.file_last_mod_time

    def set_file_last_mod_time(self, newtime):
        self.file_last_mod_time = newtime

    def get_file_first_line_time(self):
        return self.file_first_line_time

    def set_file_first_line_time(self, newtime):
        self.file_first_line_time = newtime

    def get_file_first_line_thread_id(self):
        return self.file_first_line_thread_id

    def set_file_first_line_thread_id(self, thread_id):
        self.file_first_line_thread_id = thread_id

    def get_file_first_line_seq(self):
        return self.file_first_line_seq

    def set_file_first_line_seq(self, sequence):
        self.file_first_line_seq = sequence

    def get_last_parse_row(self):
        return self.last_parse_row

    def set_last_parse_row(self, row):
        self.last_parse_row = row

    def update_stat(self):
        stat_sec_name = 'status'
        if not self.parser.has_section(stat_sec_name):
            self.parser.add_section(stat_sec_name)

        self.parser.set(stat_sec_name, "file_last_modified_time",
                        DateUtils.timestamp_to_strtime(self.file_last_mod_time,
                                                       "%Y-%m-%d-%H:%M:%S.%f"))
        self.parser.set(stat_sec_name, "file_first_line_time",
                        DateUtils.timestamp_to_strtime(
                            self.file_first_line_time, "%Y-%m-%d-%H:%M:%S"))
        self.parser.set(stat_sec_name, "file_first_line_thread_id",
                        self.file_first_line_thread_id)
        self.parser.set(stat_sec_name, "file_first_line_seq",
                        self.file_first_line_seq)
        self.parser.set(stat_sec_name, "last_parse_row", self.last_parse_row)
        self.parser.write(open(stat_file, 'w'))


class PreProcessor:
    def __init__(self):
        pass

    def __get_conf_ver(self):
        version = 2
        parser = ConfigParser.ConfigParser()
        parser.read(config_file)
        if parser.has_section('parse'):
            version = 1
        return version

    def __upgrade(self):
        logging.info('Upgrade from old version...')
        parse_sec_name = 'parse'
        stat_sec_name = 'status'
        mysql_sec_name = 'mysql'

        try:
            conf_parser = ConfigParser.ConfigParser()
            conf_parser.read(config_file)
            stat_parser = ConfigParser.ConfigParser()
            parse_items = conf_parser.items(parse_sec_name)
            stat_parser.add_section(stat_sec_name)
            for key, value in parse_items:
                # Move the following two items into section 'mysql', and rename
                # 'parse_log_directory' to 'audit_log_directory'.
                # Other items in section 'parse' will be move to the stat file.
                if 'parse_log_directory' == key:
                    conf_parser.set(mysql_sec_name, 'audit_log_directory',
                                    value)
                elif 'audit_log_file_name' == key:
                    conf_parser.set(mysql_sec_name, key, value)
                else:
                    stat_parser.set(stat_sec_name, key, value)

            stat_parser.write(open(stat_file, 'w'))
            conf_parser.remove_section(parse_sec_name)
            conf_parser.write(open(config_file, 'w'))
        except BaseException:
            msg = traceback.format_exc()
            logger.error(
                'Exception occurred when preprocessing: {}'.format(msg)
            )
            return 1
        return 0

    def run(self):
        # The config file should always be there.
        if not os.path.exists(config_file):
            logger.error("Configuration file {} dose not exist"
                         .format(config_file))
            return 1

        if 1 == self.__get_conf_ver():
            rc = self.__upgrade()
            if 0 != rc:
                logging.error('Upgrade from old version failed: {}'.format(rc))
                return rc
            logging.info('Upgrade successfully')
        return 0


class MysqlMetaSync:
    """ parsing DDL operation in the audit log at a specified time interval and
        execute on other SSQL servers

    """

    def __init__(self):
        self.sleep_time = 1
        self.SUCCESS_STATE = 0
        self.ignore_file = "ignore.info"

    @staticmethod
    def __is_database_opr(sql):
        sql = sql.lower().strip()
        db_regex = r'create(\s+)database|drop(\s+)database|alter(\s+)database'
        if re.match(db_regex, sql):
            return True
        else:
            return False

    def __execute_command(self, command):
        cmd_str = " ".join(command)
        # Remove the password from the command, for logging.
        safe_cmd_str = re.sub('-p[^\s]+\s', '', cmd_str)
        try:
            process = subprocess.Popen(command, shell=False,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE)
            out, error = process.communicate()
            if "" != error and MYSQL_ERRORS[CONN_ERR] in error:
                logger.error("Not able to connect to remote instance. "
                             "Command: " + safe_cmd_str)
                return CONN_ERR
            elif "" != error and (MYSQL_ERRORS[SYNTAX_ERR] in error or
                                  MYSQL_ERRORS[SYNTAX_ERR_2] in error):
                # If syntax error is encountered, it is most likely to be the
                # sql_mode settings. So set the sql_mode to ANSI_QUOTES and
                # try again.
                logger.warn("Encounter syntax error, retry with sql_mode "
                            "set to ANSI_QUOTES...")
                command[len(command) - 1] = 'set sql_mode="ANSI_QUOTES";' + \
                                            command[len(command) - 1]
                cmd_str = " ".join(command)
                safe_cmd_str = re.sub('-p[^\s]+\s', '', cmd_str)
                # Try again
                process = subprocess.Popen(command, shell=False,
                                           stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE)
                out, error = process.communicate()
                if "" != error and MYSQL_ERRORS[CONN_ERR] in error:
                    logger.error("Not able to connect to remote instance. "
                                 "Command: " + safe_cmd_str)
                    return CONN_ERR
                elif "" != error and (MYSQL_ERRORS[SYNTAX_ERR] in error or
                                      MYSQL_ERRORS[SYNTAX_ERR_2] in error):
                    logger.error("Syntax error in statement. Command: " +
                                 safe_cmd_str)
                    return SYNTAX_ERR
            if 0 != process.returncode:
                logger.error("Execute command failed, subprocess return "
                             "code: " + str(process.returncode) +
                             ", error: " + error.strip() + ". Command: " +
                             safe_cmd_str)
                return UNHANDLED_ERR
            logger.info("Execute command succeed. Command detail: " +
                        safe_cmd_str)
            return MYSQL_OK
        except subprocess.CalledProcessError:
            msg = traceback.format_exc()
            logger.error("Execute command failed: " + msg + ". Command: " +
                         safe_cmd_str)
            return UNHANDLED_ERR

    def __log_ignore_stmt(self, stmt):
        ignore_file = open(self.ignore_file, "a")
        ignore_file.write(
            datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f") + " " + stmt + "\n")
        ignore_file.close()

    def get_audit_log_files(self):
        """get the list of log files to be parsed

        :return: list of log files to be parsed
        """
        target_files = []
        try:
            audit_log_path = option_mgr.get_audit_log_path()
            logger.info("get audit log file list in {directory}"
                        .format(directory=audit_log_path))
            for f in os.listdir(audit_log_path):
                if f.startswith(option_mgr.get_audit_log_name()) \
                        and not f.endswith(".swp"):
                    file_path = os.path.join(audit_log_path, f)
                    tmp_last_modified_time = DateUtils.get_file_mtime_timestamp(
                        file_path)
                    tmp_meta_modified_time = DateUtils.get_file_ctime_timestamp(
                        file_path)
                    if stat_mgr.get_file_last_mod_time() <= \
                            tmp_last_modified_time:
                        target_file = {
                            "name": f,
                            "file": file_path,
                            "mtime": tmp_last_modified_time,
                            "ctime": tmp_meta_modified_time
                        }
                        logger.debug(
                            "{source} <= {target}, put audit log file "
                            "[{file_name}] into list".format(
                                file_name=f,
                                source=stat_mgr.get_file_last_mod_time(),
                                target=tmp_last_modified_time)
                        )
                        target_files.append(target_file)
        except OSError as e:
            if e.errno == 2:
                logger.warn("audit file list was changed when collecting "
                            "audit file list")
                target_files = []
            else:
                raise e
        if target_files:
            logger.debug("sort list")
            sorted(target_files, cmp=DateUtils.compare_mtime)
        return target_files

    def execute_sql(self, exec_sql_info, db_required, session_attr=None):
        database = exec_sql_info["database"]
        sql = exec_sql_info["sql"]
        if session_attr is not None:
            sql = session_attr + sql
        mysql_exec = option_mgr.get_mysql_home() + '/bin/mysql'
        for host in option_mgr.get_hosts():
            # On other instances, the operation should only be done on MySQL.
            if db_required:
                command = [mysql_exec,
                           '-h', host,
                           '-P', option_mgr.get_port(),
                           '-D', database,
                           '-u', option_mgr.get_mysql_user(),
                           '-p' + option_mgr.get_mysql_passwd(),
                           '-e', sql
                           ]
            else:
                command = [mysql_exec,
                           '-h', host,
                           '-P', option_mgr.get_port(),
                           '-u', option_mgr.get_mysql_user(),
                           '-p' + option_mgr.get_mysql_passwd(),
                           '-e', sql
                           ]

            retry_times = 0
            while True:
                logger.info(
                    "begin to connect [{host}]'s mysql server to execute sql"
                    .format(host=host))
                retry_times += 1
                result = self.__execute_command(command)
                if MYSQL_OK == result:
                    logger.info("finish to execute sql")
                    break
                elif CONN_ERR != result and option_mgr.ignore_error() and \
                        retry_times > option_mgr.get_retry_limit():
                    cmd_str = " ".join(command)
                    # Remove the password from the command, for logging.
                    safe_cmd_str = re.sub('-p[^\s]+\s', '', cmd_str)
                    logger.error("Failed to execute command. Write command "
                                 "into ignore file... Command: " +
                                 safe_cmd_str)
                    self.__log_ignore_stmt(safe_cmd_str)
                    break
                logger.error("Execute command failed. Sleep for 3 seconds "
                             "and try again...")
                time.sleep(3)

    def parse_audit_log_file(self, f):
        """ parse log file

        :param f: file descriptor of the audit log file
        """
        audit_log_field = ["log_time", "server_host", "user", "remote_host",
                           "thread_id", "seq", "operation",
                           "database",
                           "sql", "exec_state"]
        actual_parse_count = 0
        row_number = 0
        lines = f.readlines()
        for line in lines:
            row_number += 1
            # start from last parse row
            if int(stat_mgr.get_last_parse_row()) >= row_number:
                continue
            # The statement may contain '\n' or '\t'. They will impact the
            # action of the DictReader. So remove them before parse.
            line = line.replace('\\r\\n', ' ').replace('\\n', ' ').replace(
                '\\t', ' ').strip()
            reader_list = csv.DictReader(io.StringIO(unicode(line, "utf-8")),
                                         fieldnames=audit_log_field,
                                         delimiter=',',
                                         quotechar="'", quoting=csv.QUOTE_ALL,
                                         escapechar='\\')
            row = next(reader_list)

            actual_parse_count = actual_parse_count + 1
            # filter error log
            exec_state = row["exec_state"]
            try:
                if int(exec_state) != self.SUCCESS_STATE:
                    logger.debug("filter error log: {row}".format(row=row))
                    stat_mgr.last_parse_row = row_number
                    continue
            except BaseException as e:
                msg = traceback.format_exc()
                logger.error(
                    "filter error audit log failed, err:{error}".format(
                        error=msg))
                raise e

            # filter other mysql host log
            remote_host = row["remote_host"]
            if remote_host in option_mgr.get_hosts():
                logger.debug("filter other mysql host opr: {host}".format(
                    host=remote_host))
                stat_mgr.set_last_parse_row(row_number)
                continue

            # filter select,insert,update sql
            sql = row["sql"]
            low_sql = sql.lower()
            if low_sql.startswith("alter") \
                    or low_sql.startswith("create") \
                    or low_sql.startswith("drop") \
                    or low_sql.startswith("declare") \
                    or low_sql.startswith("grant") \
                    or low_sql.startswith("revoke") \
                    or low_sql.startswith("flush") \
                    or low_sql.startswith("rename"):

                database = row["database"]
                if not database.strip():
                    logger.warn("database is empty, exec sql [{sql}] in "
                                "mysql database".format(sql=sql))
                    database = "mysql"
                # mysql command指定库名时，数据库名不能含有`
                elif database.startswith("`"):
                    database = database[1:-1]

                db_required = True
                # Replace 'ALGORITHM=COPY' with one blank, as on other
                # instances, the operation should never be done in copy mode.
                sql = re.sub(r'[,]*(\s*)ALGORITHM(\s*)=(\s*)COPY(\s*)[,]*', ' ',
                             sql, flags=re.IGNORECASE)
                exec_sql_info = {"database": database, "sql": str(sql)}

                # If it's create/drop database operation, ignore the database
                # argument.
                if self.__is_database_opr(low_sql):
                    db_required = False
                session_attr = "set session sequoiadb_execute_only_in_mysql=on;"
                self.execute_sql(exec_sql_info, db_required, session_attr)
            stat_mgr.set_last_parse_row(row_number)
        return actual_parse_count

    def run_parse_task(self):
        logger.info("begin to sync")
        try:
            while True:
                files = self.get_audit_log_files()
                if not files:
                    self.sleep_time = self.sleep_time ** 2
                    logger.warn("after {sleep_time} seconds, retry to get "
                                "audit file list."
                                .format(sleep_time=self.sleep_time))
                    time.sleep(self.sleep_time)
                    continue
                self.sleep_time = 1
                file_index = 0
                file_count = len(files)
                logger.info("audit log file list count is {count}".format(
                    count=file_count))
                finish_parse_file_list = True
                parse_next_file = False
                for index in range(file_count):
                    current_file = files[index]
                    current_file_path = current_file["file"]
                    current_file_name = current_file["name"]
                    current_file_ctime = current_file["ctime"]
                    current_file_mtime = current_file["mtime"]
                    with open(current_file_path, "rb") as f:
                        try:
                            current_file_actual_ctime = \
                                DateUtils.get_file_ctime_timestamp(
                                    current_file_path
                                )
                            # 文件列表中的文件个数发生变化的情况
                            if current_file_name.endswith(
                                    option_mgr.get_audit_log_name()) \
                               and current_file_ctime != \
                                    current_file_actual_ctime \
                               and file_index >= 1:
                                pre_file = files[index - 1]
                                pre_file_path = pre_file["file"]
                                pre_file_ctime = pre_file["ctime"]
                                pre_file_actual_ctime = \
                                    DateUtils.get_file_ctime_timestamp(
                                        pre_file_path
                                    )
                                if pre_file_ctime != pre_file_actual_ctime:
                                    logger.warn("file list is changed, get "
                                                "file list again.")
                                    finish_parse_file_list = False
                                    break
                            if current_file_name.endswith(
                                    option_mgr.get_audit_log_name()) \
                               and current_file_ctime != \
                                    current_file_actual_ctime \
                               and len(os.listdir(
                                   option_mgr.get_audit_log_path())) != 1:
                                logger.warn(
                                    "file list is changed, get file list again."
                                )
                                finish_parse_file_list = False
                                break
                            elif not current_file_name.endswith(
                                    option_mgr.get_audit_log_name()) \
                                    and current_file_ctime != \
                                    current_file_actual_ctime:
                                logger.warn(
                                    "file list is changed, get file list again."
                                )
                                finish_parse_file_list = False
                        except OSError as e:
                            if e.errno == 2:
                                logger.warn(
                                    "audit file list was changed when get audit"
                                    " file {file} last modified time"
                                    .format(file=current_file_name))
                                finish_parse_file_list = False
                                break
                            else:
                                msg = traceback.format_exc()
                                logger.error(
                                    "fail to get audit file [{file}] last "
                                    "modified time".format(
                                        file=current_file_name
                                    )
                                )
                                logger.error(msg)
                                raise e
                        # parse file
                        line = f.readline()
                        if not line:
                            logger.info("audit file [{file}] is empty".format(
                                file=current_file_name))
                            finish_parse_file_list = True
                            break
                        elements = line.split(",")
                        first_line_time = DateUtils.strtime_to_timestamp(
                            elements[0], "%Y%m%d %H:%M:%S")
                        first_line_thread_id = long(elements[4])
                        first_line_seq = long(elements[5])

                        if stat_mgr.get_file_last_mod_time() == 0:
                            stat_mgr.set_last_parse_row(0)
                            stat_mgr.set_file_first_line_time(first_line_time)
                            stat_mgr.set_file_first_line_thread_id(
                                first_line_thread_id)
                            stat_mgr.set_file_first_line_seq(first_line_seq)
                            stat_mgr.set_file_last_mod_time(current_file_mtime)

                            logger.info(
                                "parse audit log file: {}, it's last "
                                "modified time is {}"
                                .format(current_file_name,
                                        DateUtils.timestamp_to_strtime(
                                            stat_mgr.get_file_last_mod_time(),
                                            "%Y-%m-%d-%H:%M:%S.%f")
                                        )
                            )
                            f.seek(0)
                            actual_parse_count = self.parse_audit_log_file(f)
                            logger.info(
                                "file row count: {}, parse count: {}"
                                .format(stat_mgr.get_last_parse_row(),
                                        actual_parse_count)
                            )
                        elif first_line_time == \
                                stat_mgr.get_file_first_line_time() \
                                and first_line_thread_id == \
                                stat_mgr.get_file_first_line_thread_id() \
                                and first_line_seq == \
                                stat_mgr.get_file_first_line_seq():
                            stat_mgr.set_file_last_mod_time(current_file_mtime)
                            logger.info(
                                "parse audit log file: {}, it's last modified "
                                "time is {}".format(
                                    current_file_name,
                                    DateUtils.timestamp_to_strtime(
                                        stat_mgr.get_file_last_mod_time(),
                                        "%Y-%m-%d-%H:%M:%S.%f")
                                )
                            )
                            f.seek(0)
                            actual_parse_count = self.parse_audit_log_file(f)
                            logger.info(
                                "file row count: {}, parse count: {}".format(
                                    stat_mgr.get_last_parse_row(),
                                    actual_parse_count
                                )
                            )
                        elif parse_next_file:  # 扫描文件列表的下一个文件
                            stat_mgr.set_file_last_mod_time(current_file_mtime)
                            stat_mgr.set_last_parse_row(0)
                            stat_mgr.set_file_first_line_time(first_line_time)
                            stat_mgr.set_file_first_line_thread_id(
                                first_line_thread_id)
                            stat_mgr.set_file_first_line_seq(first_line_seq)
                            logger.info(
                                "parse audit log file: {}, it's last "
                                "modified time is {}".format(
                                    current_file_name,
                                    DateUtils.timestamp_to_strtime(
                                        stat_mgr.get_file_last_mod_time(),
                                        "%Y-%m-%d-%H:%M:%S.%f")
                                )
                            )
                            f.seek(0)
                            actual_parse_count = self.parse_audit_log_file(f)
                            logger.info(
                                "file row count : {}, parse count: {}"
                                .format(stat_mgr.get_last_parse_row(),
                                        actual_parse_count)
                            )
                        else:
                            logger.error(
                                "Information(file_last_modified_time, "
                                "file_first_line_time, "
                                "file_first_line_thread_id, "
                                "file_first_line_seq) of file {} "
                                "is not as expected. "
                                "Expect: [{}, {}, {}, {}]. "
                                "Actual: [{}, {}, {}, {}]"
                                .format(current_file_name,
                                        current_file_mtime,
                                        first_line_time,
                                        first_line_thread_id,
                                        first_line_seq,
                                        stat_mgr.get_file_last_mod_time(),
                                        stat_mgr.get_file_first_line_time(),
                                        stat_mgr.get_file_first_line_thread_id(),
                                        stat_mgr.get_file_first_line_seq()
                                        )
                            )
                            sys.exit(-1)
                    file_index = file_index + 1
                    if file_index < file_count:
                        parse_next_file = True
                    else:
                        parse_next_file = False
                    stat_mgr.update_stat()
                if finish_parse_file_list:  # 解析完文件列表,退出循环
                    logger.info("finish to parse audit log file list.")
                    break
        except BaseException as e:
            msg = traceback.format_exc()
            logger.error("run parse task failed: {error}".format(error=msg))
            raise e
        finally:
            stat_mgr.update_stat()
            logger.info("finish to sync")


def init_log(log_config_file):
    try:
        # Get the log file path from the log configuration file, and create the
        # directory if it dose not exist.
        config_parser = ConfigParser.ConfigParser()
        files = config_parser.read(log_config_file)
        if len(files) != 1:
            print("Error: Read log configuration file failed")
            return None
        log_file = config_parser.get("handler_rotatingFileHandler", "args")\
                   .split('\'')[1]
        curr_path = os.path.abspath(os.path.dirname(log_config_file))
        log_file_full_path = os.path.join(curr_path, log_file)
        log_file_parent_dir = os.path.abspath(
            os.path.join(log_file_full_path, ".."))
        if not os.path.exists(log_file_parent_dir):
            os.makedirs(log_file_parent_dir)

        logging.config.fileConfig(log_config_file)
        log = logging.getLogger("ddlLogger")
        return log
    except BaseException as e:
        print("Error: Initialize logging failed. Error Message: " + e.message)
        return None


def run_task():
    ssql_mysql = MysqlMetaSync()
    scheduler = sched.scheduler(time.time, time.sleep)
    while True:
        scheduler.enter(option_mgr.get_scan_interval(), 1,
                        ssql_mysql.run_parse_task, ())
        scheduler.run()


def main():
    global my_home
    global logger
    global option_mgr
    global config_file
    global stat_mgr
    global stat_file

    config_file = 'config'
    stat_file = 'sync.stat'

    my_home = os.path.abspath(os.path.dirname(__file__))
    os.chdir(my_home)

    config_file = os.path.join(my_home, config_file)
    stat_file = os.path.join(my_home, stat_file)

    pid_file = os.path.join(my_home, "APP_ID")
    if os.path.exists(pid_file):
        with open(pid_file, "r") as f:
            pid = str(f.readline())
        if os.path.exists("/proc/{pid}".format(pid=pid)):
            with open("/proc/{pid}/cmdline".format(pid=pid), "r") as process:
                process_info = process.readline()
            if process_info.find(sys.argv[0]) != -1:
                print("Only one meta sync process is allowed to run at the same"
                      " time. Exit...")
                return 1
    with open(pid_file, "w") as f:
        pid = str(os.getpid())
        f.write(pid)

    log_config_file = os.path.join(my_home, "log.config")
    log_instance = Logger()
    rc = log_instance.init(log_config_file)
    if 0 != rc:
        print("[ERROR] Initialize logging failed: {}".format(rc))
        return rc
    logger = log_instance.get_logger()
    logger.info("Start MySQL metadata sync tool...")

    preprocessor = PreProcessor()
    rc = preprocessor.run()
    if 0 != rc:
        logger.error("Preprocessing failed: {}".format(rc))
        return rc

    option_mgr = OptionMgr()
    rc = option_mgr.load_configs(config_file)
    if 0 != rc:
        logger.error('Load configurations failed: {}'.format(rc))
        return rc

    stat_mgr = StatMgr()
    rc = stat_mgr.load_stat(stat_file)
    if 0 != rc:
        logger.error('Load status failed: {}'.format(rc))
        return rc

    return run_task()


if __name__ == '__main__':
    rc = main()
    if 0 != rc:
        print('[ERROR] Start MySQL metadata sync tool failed. Please refer to '
              'the log for more detail')
    sys.exit(rc)
