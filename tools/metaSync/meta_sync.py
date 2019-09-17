#! /usr/bin/python
# -*- coding: utf-8 -*-
# @Author JianHong Xie
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
        random_choice = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890!@#$%^&*()"
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
        """get current time of year-month-day hour:minute:second.microsecond format

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
        local_timestamp = long(time.mktime(datetime_obj.timetuple()) * 1000000.0 + datetime_obj.microsecond)
        return local_timestamp

    @classmethod
    def strtime_to_datetime(cls, timestr, date_format):
        local_datetime = datetime.strptime(timestr, date_format)
        return local_datetime

    @classmethod
    def timestamp_to_strtime(cls, timestamp, date_format):
        return cls.datetime_to_strtime(cls.timestamp_to_datetime(timestamp), date_format)

    @classmethod
    def strtime_to_timestamp(cls, timestr, date_format):
        try:
            local_str_time = cls.datetime_to_timestamp(cls.strtime_to_datetime(timestr, date_format))
            return local_str_time
        except Exception as e:
            return 0

    @classmethod
    def get_file_ctime_timestamp(cls, f):
        return cls.datetime_to_timestamp(datetime.fromtimestamp(os.path.getctime(f)))

    @classmethod
    def get_file_mtime_timestamp(cls, f):
        return cls.datetime_to_timestamp(datetime.fromtimestamp(os.path.getmtime(f)))

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


class MysqlMetaSync:
    """ parsing DDL operation in the audit log at a specified time interval and execute on other SSQL servers

    """

    def __init__(self, log):
        # 获取当前文件路径
        current_file_path = os.path.split(os.path.realpath(__file__))[0]
        self.config_file = os.path.join(current_file_path, "config")
        self.log = log
        self.config = ConfigParser.ConfigParser()
        self.config.read(self.config_file)

        self.install_dir = self.config.get('mysql', 'install_dir')
        self.hosts = self.config.get('mysql', 'hosts').replace(' ', '').split(',')
        self.port = self.config.get('mysql', 'port')
        self.mysql_password_type = self.config.get('mysql', 'mysql_password_type')
        self.mysql_user = self.config.get('mysql', 'mysql_user')
        self.mysql_password = self.config.get('mysql', 'mysql_password')
        self.interval_time = int(self.config.get('execute', 'interval_time'))
        ignore_option = self.config.get('execute', 'ignore_error')
        if "true" == ignore_option.lower():
            self.ignore_error = True
        else:
            self.ignore_error = False
        # self.ignore_error = self.config.get('execute', 'ignore_error')
        self.max_retry_times = int(self.config.get('execute', 'max_retry_times'))
        self.parse_log_directory = self.config.get('parse', 'parse_log_directory')
        self.audit_log_file_name = self.config.get('parse', 'audit_log_file_name')
        self.file_last_modified_time = DateUtils.strtime_to_timestamp(
            self.config.get('parse', 'file_last_modified_time'), "%Y-%m-%d-%H:%M:%S.%f")
        self.file_first_line_time = DateUtils.strtime_to_timestamp(
            self.config.get('parse', 'file_first_line_time'), "%Y-%m-%d-%H:%M:%S")
        self.file_first_line_thread_id = int(self.config.get('parse', 'file_first_line_thread_id'))
        self.file_first_line_seq = int(self.config.get('parse', 'file_first_line_seq'))
        self.last_parse_row = int(self.config.get('parse', 'last_parse_row'))
        self.sleep_time = 1
        self.SUCCESS_STATE = 0
        self.level = {"debug": "DEBUG", "info": "INFO", "warning": "WARNING", "error": "ERROR"}
        self.level_priority = {"DEBUG": 4, "INFO": 3, "WARNING": 2, "ERROR": 1}
        self.ignore_file = "ignore.info"
        self.check_avg()
        self.logger(self.level["info"], "Start MySQL meta_sync...")

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
            process = subprocess.Popen(command, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, error = process.communicate()
            if "" != error and MYSQL_ERRORS[CONN_ERR] in error:
                self.logger(self.level["error"], "Not able to connect to remote instance. Command: " + safe_cmd_str)
                return CONN_ERR
            elif "" != error and (MYSQL_ERRORS[SYNTAX_ERR] in error or
                                  MYSQL_ERRORS[SYNTAX_ERR_2] in error):
                # If syntax error is encountered, it is most likely to be the sql_mode settings. So set the sql_mode to
                # ANSI_QUOTES and try again.
                self.logger(self.level["warning"],
                            "Encounter syntax error, retry with sql_mode set to ANSI_QUOTES...")
                command[len(command) - 1] = 'set sql_mode="ANSI_QUOTES";' + command[len(command) - 1]
                cmd_str = " ".join(command)
                safe_cmd_str = re.sub('-p[^\s]+\s', '', cmd_str)
                # Try again
                process = subprocess.Popen(command, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                out, error = process.communicate()
                if "" != error and MYSQL_ERRORS[CONN_ERR] in error:
                    self.logger(self.level["error"], "Not able to connect to remote instance. Command: " + safe_cmd_str)
                    return CONN_ERR
                elif "" != error and (MYSQL_ERRORS[SYNTAX_ERR] in error or
                                      MYSQL_ERRORS[SYNTAX_ERR_2] in error):
                    self.logger(self.level["error"], "Syntax error in statement. Command: " + safe_cmd_str)
                    return SYNTAX_ERR
            if 0 != process.returncode:
                self.logger(self.level["error"], "Execute command failed, subprocess return code: " +
                            str(process.returncode) + ", error: " + error.strip() +
                            ". Command: " + safe_cmd_str)
                return UNHANDLED_ERR
            self.logger(self.level["info"], "Execute command succeed. Command detail: " + safe_cmd_str)
            return MYSQL_OK
        except subprocess.CalledProcessError:
            msg = traceback.format_exc()
            self.logger(self.level["error"], "Execute command failed: " + msg + ". Command: " + safe_cmd_str)
            return UNHANDLED_ERR

    def __log_ignore_stmt(self, stmt):
        ignore_file = open(self.ignore_file, "a")
        ignore_file.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f") + " " + stmt + "\n")
        ignore_file.close()

    def check_avg(self):

        # if not os.path.exists(self.log_directory):
        #     os.makedirs(self.log_directory)

        if int(self.mysql_password_type) == 0:
            encrypt_password = CryptoUtil.encrypt(self.mysql_password)
            self.mysql_password = encrypt_password
            self.mysql_password_type = 1
            self.update_password()

    def update_password(self):
        self.config.set('mysql', 'mysql_password', self.mysql_password)
        self.config.set('mysql', 'mysql_password_type', self.mysql_password_type)
        self.config.write(open(self.config_file, "w"))

    def remove_current_hostname(self):
        current_hostname = socket.gethostname()
        if current_hostname in self.hosts:
            self.logger(self.level["debug"], "filter host [{host}]".format(host=current_hostname))
            self.hosts.remove(current_hostname)

    # def get_current_log_file(self):
    #     """get the absolute path of current log file
    #
    #     :return: the absolute path string of current log file
    #     """
    #     current_log_file_name = self.log_file_prefix + ".log." + DateUtils.get_current_date()
    #     current_log_file = os.path.join(self.log_directory, current_log_file_name)
    #     return current_log_file

    def get_audit_log_files(self):
        """get the list of log files to be parsed

        :return: list of log files to be parsed
        """
        target_files = []
        try:
            self.logger(self.level["info"],
                        "get audit log file list in {directory}".format(directory=self.parse_log_directory))
            for f in os.listdir(self.parse_log_directory):
                if f.startswith(self.audit_log_file_name) and not f.endswith(".swp"):
                    file_path = os.path.join(self.parse_log_directory, f)
                    tmp_last_modified_time = DateUtils.get_file_mtime_timestamp(file_path)
                    tmp_meta_modified_time = DateUtils.get_file_ctime_timestamp(file_path)
                    if self.file_last_modified_time <= tmp_last_modified_time:
                        target_file = {
                            "name": f,
                            "file": file_path,
                            "mtime": tmp_last_modified_time,
                            "ctime": tmp_meta_modified_time
                        }
                        self.logger(self.level["debug"],
                                    "{source} <= {target}, put audit log file [{file_name}] into list".format(
                                        file_name=f,
                                        source=self.file_last_modified_time,
                                        target=tmp_last_modified_time
                                    ))
                        target_files.append(target_file)
        except OSError as e:
            if e.errno == 2:
                self.logger(self.level["warning"], "audit file list was changed when collecting audit file list")
                target_files = []
            else:
                raise e
        if target_files:
            self.logger(self.level["debug"], "sort list")
            sorted(target_files, cmp=DateUtils.compare_mtime)
        return target_files

    def execute_sql(self, exec_sql_info, db_required, session_attr=None):
        database = exec_sql_info["database"]
        sql = exec_sql_info["sql"]
        if session_attr is not None:
            sql = session_attr + sql
        for host in self.hosts:
            # On other instances, the operation should only be done on MySQL.
            if db_required:
                command = [self.install_dir + '/bin/mysql',
                           '-h', host,
                           '-P', self.port,
                           '-D', database,
                           '-u', self.mysql_user,
                           '-p' + CryptoUtil.decrypt(self.mysql_password),
                           '-e', sql
                           ]
            else:
                command = [self.install_dir + '/bin/mysql',
                           '-h', host,
                           '-P', self.port,
                           '-u', self.mysql_user,
                           '-p' + CryptoUtil.decrypt(self.mysql_password),
                           '-e', sql
                           ]

            retry_times = 0
            while True:
                self.logger(self.level["info"],
                            "begin to connect [{host}]'s mysql server to execute sql".format(host=host))
                retry_times += 1
                result = self.__execute_command(command)
                if MYSQL_OK == result:
                    self.logger(self.level["info"], "finish to execute sql")
                    break
                elif CONN_ERR != result and self.ignore_error and retry_times > self.max_retry_times:
                    cmd_str = " ".join(command)
                    # Remove the password from the command, for logging.
                    safe_cmd_str = re.sub('-p[^\s]+\s', '', cmd_str)
                    self.logger(self.level["error"], "Failed to execute command. Write command into ignore file... "
                                "Command: " + safe_cmd_str)
                    self.__log_ignore_stmt(safe_cmd_str)
                    break
                self.logger(self.level["error"], "Execute command failed. Sleep for 3 seconds and try again...")
                time.sleep(3)

    def parse_audit_log_file(self, f):
        """ parse log file

        :param f: file descriptor of the audit log file
        """
        audit_log_field = ["log_time", "server_host", "user", "remote_host", "thread_id", "seq", "operation",
                           "database",
                           "sql", "exec_state"]
        actual_parse_count = 0
        row_number = 0
        lines = f.readlines()
        for line in lines:
            row_number += 1
            if int(self.last_parse_row) >= row_number:  # start from last parse row
                continue
            # The statement may contain '\n' or '\t'. They will impact the action of the DictReader. So remove
            # them before parse.
            line = line.replace('\\n', ' ').replace('\\t', ' ').strip()
            reader_list = csv.DictReader(io.StringIO(unicode(line, "utf-8")), fieldnames=audit_log_field, delimiter=',',
                                         quotechar="'", quoting=csv.QUOTE_ALL, escapechar='\\')
            row = next(reader_list)

            actual_parse_count = actual_parse_count + 1
            # filter error log
            exec_state = row["exec_state"]
            try:
                if int(exec_state) != self.SUCCESS_STATE:
                    self.logger(self.level["debug"], "filter error log: {row}".format(row=row))
                    self.last_parse_row = row_number
                    continue
            except BaseException as e:
                msg = traceback.format_exc()
                self.logger(self.level["error"], "filter error audit log failed, err:{error}".format(error=msg))
                raise e

            # filter other mysql host log
            remote_host = row["remote_host"]
            if remote_host in self.hosts:
                self.logger(self.level["debug"], "filter other mysql host opr: {host}".format(host=remote_host))
                self.last_parse_row = row_number
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
                    self.logger(self.level["warning"],
                                "database is empty, exec sql [{sql}] in mysql database".format(sql=sql))
                    database = "mysql"
                # mysql command指定库名时，数据库名不能含有`
                elif database.startswith("`"):
                    database = database[1:-1]

                db_required = True
                # Replace 'ALGORITHM=COPY' with one blank, as on other instances, the operation should never be done
                # in copy mode.
                sql = re.sub(r'[,]*(\s*)ALGORITHM(\s*)=(\s*)COPY(\s*)[,]*', ' ', sql, flags=re.IGNORECASE)
                exec_sql_info = {"database": database, "sql": str(sql)}

                # If it's create/drop database operation, ignore the database argument.
                if self.__is_database_opr(low_sql):
                    db_required = False
                session_attr = "set session sequoiadb_execute_only_in_mysql=on;"
                self.execute_sql(exec_sql_info, db_required, session_attr)
            self.last_parse_row = row_number
        return actual_parse_count

    def run_parse_task(self):
        self.logger(self.level["info"], "begin to sync")
        try:
            while True:
                files = self.get_audit_log_files()
                if not files:
                    self.sleep_time = self.sleep_time ** 2
                    self.logger(self.level["warning"],
                                "after {sleep_time} seconds, retry to get audit file list.".format(
                                    sleep_time=self.sleep_time))
                    time.sleep(self.sleep_time)
                    continue
                self.sleep_time = 1
                file_index = 0
                file_count = len(files)
                self.logger(self.level["info"], "audit log file list count is {count}".format(count=file_count))
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
                            current_file_actual_ctime = DateUtils.get_file_ctime_timestamp(current_file_path)
                            # 文件列表中的文件个数发生变化的情况
                            if current_file_name.endswith(self.audit_log_file_name) \
                                    and current_file_ctime != current_file_actual_ctime \
                                    and file_index >= 1:
                                pre_file = files[index - 1]
                                pre_file_path = pre_file["file"]
                                pre_file_ctime = pre_file["ctime"]
                                pre_file_actual_ctime = DateUtils.get_file_ctime_timestamp(pre_file_path)
                                if pre_file_ctime != pre_file_actual_ctime:
                                    self.logger(self.level["warning"], "file list is changed, get file list again.")
                                    finish_parse_file_list = False
                                    break
                            if current_file_name.endswith(self.audit_log_file_name) \
                                    and current_file_ctime != current_file_actual_ctime \
                                    and len(os.listdir(self.parse_log_directory)) != 1:
                                self.logger(self.level["warning"], "file list is changed, get file list again.")
                                finish_parse_file_list = False
                                break
                            elif not current_file_name.endswith(self.audit_log_file_name) \
                                    and current_file_ctime != current_file_actual_ctime:
                                self.logger(self.level["warning"], "file list is changed, get file list again.")
                                finish_parse_file_list = False
                        except OSError as e:
                            if e.errno == 2:
                                self.logger(self.level["warning"],
                                            "audit file list was changed when get audit file {file} last modified time"
                                            .format(file=current_file_name))
                                finish_parse_file_list = False
                                break
                            else:
                                msg = traceback.format_exc()
                                self.logger(self.level["error"],
                                            "fail to get audit file [{file}] last modified time".format(
                                                file=current_file_name
                                            ))
                                self.logger(self.level["error"], msg)
                                raise e
                        # parse file
                        line = f.readline()
                        if not line:
                            self.logger(self.level["info"],
                                        "audit file [{file}] is empty".format(file=current_file_name))
                            finish_parse_file_list = True
                            break
                        elements = line.split(",")
                        first_line_time = DateUtils.strtime_to_timestamp(elements[0], "%Y%m%d %H:%M:%S")
                        first_line_thread_id = long(elements[4])
                        first_line_seq = long(elements[5])

                        if self.file_last_modified_time == 0:  # 首次扫描审计日志文件

                            self.last_parse_row = 0
                            self.file_first_line_time = first_line_time
                            self.file_first_line_thread_id = first_line_thread_id
                            self.file_first_line_seq = first_line_seq
                            self.file_last_modified_time = current_file_mtime

                            self.logger(self.level["info"],
                                        "parse audit log file: {file}, it's last modified time is {last_modified_time}"
                                        .format(
                                            file=current_file_name,
                                            last_modified_time=DateUtils.timestamp_to_strtime(
                                                self.file_last_modified_time, "%Y-%m-%d-%H:%M:%S.%f")
                                        ))
                            f.seek(0)
                            actual_parse_count = self.parse_audit_log_file(f)
                            self.logger(self.level["info"],
                                        "file row count: {row}, parse count: {count}".format(
                                            row=self.last_parse_row,
                                            count=actual_parse_count
                                        ))
                        elif first_line_time == self.file_first_line_time \
                                and first_line_thread_id == self.file_first_line_thread_id \
                                and first_line_seq == self.file_first_line_seq:  # 从上一次结束位置开始扫描

                            self.file_last_modified_time = current_file_mtime
                            self.logger(self.level["info"],
                                        "parse audit log file: {file}, it's last modified time is {last_modified_time}"
                                        .format(
                                            file=current_file_name,
                                            last_modified_time=DateUtils.timestamp_to_strtime(
                                                self.file_last_modified_time, "%Y-%m-%d-%H:%M:%S.%f")
                                        ))
                            f.seek(0)
                            actual_parse_count = self.parse_audit_log_file(f)
                            self.logger(self.level["info"],
                                        "file row count: {row}, parse count: {count}".format(
                                            row=self.last_parse_row,
                                            count=actual_parse_count
                                        ))
                        elif parse_next_file:  # 扫描文件列表的下一个文件

                            self.file_last_modified_time = current_file_mtime
                            self.last_parse_row = 0
                            self.file_first_line_time = first_line_time
                            self.file_first_line_thread_id = first_line_thread_id
                            self.file_first_line_seq = first_line_seq
                            self.logger(self.level["info"],
                                        "parse audit log file: {file}, it's last modified time is {last_modified_time}"
                                        .format(
                                            file=current_file_name,
                                            last_modified_time=DateUtils.timestamp_to_strtime(
                                                self.file_last_modified_time, "%Y-%m-%d-%H:%M:%S.%f")
                                        ))
                            f.seek(0)
                            actual_parse_count = self.parse_audit_log_file(f)
                            self.logger(self.level["info"],
                                        "file row count : {row}, parse count: {count}".format(
                                            row=self.last_parse_row,
                                            count=actual_parse_count
                                        ))
                        else:
                            self.logger(self.level["error"],
                                        "don't parse {audit_log_file},"
                                        "it's file last modified time: {file_last_modified_time},"
                                        "first line time: {first_line_time}, "
                                        "first line thread id: {first_line_thread_id},"
                                        "first line begin seq: {first_line_seq};"
                                        "but in config file, file last modified time: {config_file_last_modified_time},"
                                        "first line time: {config_first_line_time},"
                                        "first line thread id: {config_first_line_thread_id},"
                                        "first_line_seq: {config_first_line_seq}"
                                        .format(audit_log_file=current_file_name,
                                                file_last_modified_time=current_file_mtime,
                                                first_line_time=first_line_time,
                                                first_line_thread_id=first_line_thread_id,
                                                first_line_seq=first_line_seq,
                                                config_file_last_modified_time=self.file_last_modified_time,
                                                config_first_line_time=self.file_first_line_time,
                                                config_first_line_thread_id=self.file_first_line_thread_id,
                                                config_first_line_seq=self.file_first_line_seq
                                                )
                                        )
                            sys.exit(-1)
                    file_index = file_index + 1
                    if file_index < file_count:
                        parse_next_file = True
                    else:
                        parse_next_file = False
                    self.save_config()
                if finish_parse_file_list:  # 解析完文件列表,退出循环
                    self.logger(self.level["info"], "finish to parse audit log file list.")
                    break
        except BaseException as e:
            msg = traceback.format_exc()
            self.logger(self.level["error"], "run parse task failed: {error}".format(error=msg))
            raise e
        finally:
            self.save_config()
            self.logger(self.level["info"], "finish to sync")

    def save_config(self):
        self.config.set("parse", "file_last_modified_time",
                        DateUtils.timestamp_to_strtime(self.file_last_modified_time, "%Y-%m-%d-%H:%M:%S.%f"))
        self.config.set("parse", "file_first_line_time",
                        DateUtils.timestamp_to_strtime(self.file_first_line_time, "%Y-%m-%d-%H:%M:%S"))
        self.config.set("parse", "file_first_line_thread_id", self.file_first_line_thread_id)
        self.config.set("parse", "file_first_line_seq", self.file_first_line_seq)
        self.config.set("parse", "last_parse_row", self.last_parse_row)
        self.config.write(open(self.config_file, "w"))

    def logger(self, log_level, message):

        if log_level == self.level["error"]:
            self.log.error(message)
        elif log_level == self.level["warning"]:
            self.log.warn(message)
        elif log_level == self.level["info"]:
            self.log.info(message)
        elif log_level == self.level["debug"]:
            self.log.debug(message)


def init_log(log_config_file):
    try:
        # Get the log file path from the log configuration file, and create the directory if it dose not exist.
        config_parser = ConfigParser.ConfigParser()
        files = config_parser.read(log_config_file)
        if len(files) != 1:
            print("Error: Read log configuration file failed")
            return None
        log_file = config_parser.get("handler_rotatingFileHandler", "args").split('\'')[1]
        curr_path = os.path.abspath(os.path.dirname(log_config_file))
        log_file_full_path = os.path.join(curr_path, log_file)
        log_file_parent_dir = os.path.abspath(os.path.join(log_file_full_path, ".."))
        if not os.path.exists(log_file_parent_dir):
            os.makedirs(log_file_parent_dir)

        logging.config.fileConfig(log_config_file)
        log = logging.getLogger("ddlLogger")
        return log
    except BaseException as e:
        print("Error: Initialize logging failed. Error number: " + ". Message: " + e.message)
        return None


def run_task(log):
    ssql_mysql = MysqlMetaSync(log)
    ssql_mysql.remove_current_hostname()    # 删除当前主机的主机名
    scheduler = sched.scheduler(time.time, time.sleep)  # 定时器
    while True:
        scheduler.enter(ssql_mysql.interval_time, 1, ssql_mysql.run_parse_task, ())
        scheduler.run()


def main():
    current_file_path = os.path.split(os.path.realpath(__file__))[0]
    pid_file = os.path.join(current_file_path, "APP_ID")
    if os.path.exists(pid_file):
        with open(pid_file, "r") as f:
            pid = str(f.readline())
        if os.path.exists("/proc/{pid}".format(pid=pid)):
            with open("/proc/{pid}/cmdline".format(pid=pid), "r") as process:
                process_info = process.readline()
            if process_info.find(sys.argv[0]) != -1:
                return
    with open(pid_file, "w") as f:
        pid = str(os.getpid())
        f.write(pid)

    log_config_file= os.path.join(current_file_path, "log.config")
    log = init_log(log_config_file)
    if log is None:
        print("Initialize logging failed. Exit...")
        return 1

    run_task(log)


reload(sys)
sys.setdefaultencoding('utf8')

if __name__ == '__main__':
    main()
