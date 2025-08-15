/*******************************************************************************

   Copyright (C) 2011-Present SequoiaDB Ltd.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

   
*******************************************************************************/

#include <my_global.h>
#include "server_ha_slow_log.h"
#include "sdb_conn.h"
#include "sdb_cl.h"
#include "server_ha.h"
#include "ha_sdb_errcode.h"
#include "server_ha_util.h"
#include <exception>
#include "mysql.h"
#include "server_ha_thd_common.h"
#include "ha_sdb_log.h"
#include "ha_sdb_util.h"
#include <cstdlib>

// SQL statements
#define HA_STMT_EXEC_ONLY_IN_MYSQL "SET sequoiadb_execute_only_in_mysql = 1"
#define HA_STMT_CREATE_SLOW_LOG_TABLE \
    "CREATE TABLE sdb_slow_log ("\
    "start_time timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),"\
    "end_time timestamp(6) NOT NULL DEFAULT '0000-00-00 00:00:00.000000',"\
    "query_id varchar(28) DEFAULT NULL,"\
    "digest varchar(32) DEFAULT NULL,"\
    "digest_text longtext,"\
    "user_host mediumtext NOT NULL,"\
    "query_time double NOT NULL,"\
    "lock_time double NOT NULL,"\
    "rows_sent int(11) NOT NULL,"\
    "db varchar(512) NOT NULL,"\
    "last_insert_id int(11) NOT NULL,"\
    "insert_id int(11) NOT NULL,"\
    "instance_id int(10) unsigned NOT NULL,"\
    "instance_url varchar(255) NOT NULL,"\
    "sql_text mediumblob NOT NULL,"\
    "thread_id bigint(21) unsigned NOT NULL,"\
    "connect_count int(11) unsigned NOT NULL,"\
    "connect_time double NOT NULL,"\
    "get_cl_count int(11) unsigned NOT NULL,"\
    "get_cl_time double NOT NULL,"\
    "begin_trans_count int(11) unsigned NOT NULL,"\
    "begin_trans_time double NOT NULL,"\
    "exec_inner_sql_count int(11) unsigned NOT NULL,"\
    "exec_inner_sql_time double NOT NULL,"\
    "find_count int(11) unsigned NOT NULL,"\
    "find_time double NOT NULL,"\
    "aggregate_count int(11) unsigned NOT NULL,"\
    "aggregate_time double NOT NULL,"\
    "get_count_count int(11) unsigned NOT NULL,"\
    "get_count_time double NOT NULL,"\
    "insert_count int(11) unsigned NOT NULL,"\
    "insert_time double NOT NULL,"\
    "update_count int(11) unsigned NOT NULL,"\
    "update_time double NOT NULL,"\
    "delete_count int(11) unsigned NOT NULL,"\
    "delete_time double NOT NULL,"\
    "get_detail_count int(11) unsigned NOT NULL,"\
    "get_detail_time double NOT NULL,"\
    "get_index_stat_count int(11) unsigned NOT NULL,"\
    "get_index_stat_time double NOT NULL,"\
    "ha_retry_times int(11) unsigned NOT NULL,"\
    "ha_sync_wait_time double NOT NULL,"\
    "pushdown_info text"\
    ") ENGINE=SequoiaDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"

PSI_stage_info stage_checking_table = {0, "Checking slow log table structure", 0};

static const int MS_PER_SEC = 1000;
static const int ONE_DAY_SEC = 24 * 60 * 60;
static const char *SLOW_LOG_SHARD_KEY ="end_time";

static int ha_create_and_attach_slow_log_sub_cl(Sdb_conn &sdb_conn,
                                                const char *cs_name,
                                                const char *main_cl_name,
                                                const char *sub_cl_name,
                                                time_t day_sec) {
  int rc = 0;
  bool created_cs = false;
  bool created_cl = false;
  bson::BSONObj create_cl_options;
  char err_buff[HA_BUF_LEN] = {0};
  char cl_full_name_buffer[SDB_CS_NAME_MAX_SIZE + 1 +
                           SDB_CL_NAME_MAX_SIZE + 1] = {0};
  Sdb_cl main_cl;

  try {
    bson::BSONObjBuilder crt_builder;
    crt_builder.append(SDB_CL_ATTR_NO_TRANS, true);
    crt_builder.append(SDB_FIELD_REPLSIZE, 1);
    create_cl_options = crt_builder.obj();

    rc = sdb_conn.create_cl(cs_name, sub_cl_name, create_cl_options,
                            &created_cs, &created_cl);
    if (created_cl) {
      bson::BSONObj attach_options;
      bson::BSONObjBuilder builder;
      bson::BSONObjBuilder low_builder(
          builder.subobjStart(SDB_FIELD_LOW_BOUND));
      low_builder.appendTimestamp(SLOW_LOG_SHARD_KEY, day_sec * MS_PER_SEC, 0);
      low_builder.done();
      bson::BSONObjBuilder up_builder(
          builder.subobjStart(SDB_FIELD_UP_BOUND));
      time_t second_day_ms = (day_sec + ONE_DAY_SEC) * MS_PER_SEC;
      up_builder.appendTimestamp(SLOW_LOG_SHARD_KEY, second_day_ms, 0);
      up_builder.done();
      attach_options = builder.obj();
      sprintf(cl_full_name_buffer, "%s.%s", cs_name, sub_cl_name);

      rc = sdb_conn.get_cl(cs_name, main_cl_name, main_cl);
      HA_RC_CHECK(rc, error,
                  "HA: Failed to get slow log table, sequoiadb error: %s",
                  ha_error_string(sdb_conn, rc, err_buff));
      rc = main_cl.attach_collection(cl_full_name_buffer,
                                     attach_options);
      if (get_sdb_code(rc) == SDB_RELINK_SUB_CL) {
        rc = 0;
      }
      HA_RC_CHECK(rc, error,
                  "HA: Failed to attach slow log table, "
                  "sequoiadb error: %s",
                  ha_error_string(sdb_conn, rc, err_buff));
    }
  } catch (std::bad_alloc &e) {
    sql_print_error("HA: Out of memory when creating slow log table");
    rc = SDB_HA_OOM;
    goto error;
  } catch (std::exception &e) {
    sql_print_error("HA: Unexpected error: %s", e.what());
    rc = SDB_HA_EXCEPTION;
    goto error;
  }
done:
  return rc;
error:
  goto done;
}

static int ha_create_slow_log_table(Sdb_conn &sdb_conn,
                                    const char *cs_name,
                                    const char *main_cl_name,
                                    const char *today_cl_name,
                                    time_t today_sec,
                                    const char *tomorrow_cl_name,
                                    time_t tomorrow_sec) {
  int rc = 0;
  bool created_cs = false;
  bool created_cl = false;
  bson::BSONObj options;
  char err_buff[HA_BUF_LEN] = {0};

  try {
    options = BSON( SDB_FIELD_ISMAINCL << true <<
                    SDB_FIELD_SHARDING_KEY << BSON( SLOW_LOG_SHARD_KEY << 1 ) );
  } catch (std::bad_alloc &e) {
    sql_print_error("HA: Out of memory when creating slow log table");
    rc = SDB_HA_OOM;
    goto error;
  } catch (std::exception &e) {
    sql_print_error("HA: Unexpected error: %s", e.what());
    rc = SDB_HA_EXCEPTION;
    goto error;
  }

  rc = sdb_conn.create_cl(cs_name, main_cl_name, options,
                          &created_cs, &created_cl);
  HA_RC_CHECK(rc, error,
              "HA: Failed to create slow log table, sequoiadb error: %s",
              ha_error_string(sdb_conn, rc, err_buff));
  if (created_cl) {
    rc = ha_create_and_attach_slow_log_sub_cl(sdb_conn,
                                              cs_name,
                                              main_cl_name,
                                              today_cl_name,
                                              today_sec);
    HA_RC_CHECK(rc, error,
                "HA: Failed to create today slow log table, "
                "sequoiadb error: %s",
                ha_error_string(sdb_conn, rc, err_buff));

    rc = ha_create_and_attach_slow_log_sub_cl(sdb_conn,
                                              cs_name,
                                              main_cl_name,
                                              tomorrow_cl_name,
                                              tomorrow_sec);
    HA_RC_CHECK(rc, error,
                "HA: Failed to create tomorrow slow log table, "
                "sequoiadb error: %s",
                ha_error_string(sdb_conn, rc, err_buff));
  }
done:
  return rc;
error:
  goto done;
}

static void ha_get_today_and_tomorrow_info(const char *main_cl_name,
                                           time_t &today_sec,
                                           char *today_cl_name,
                                           time_t &tomorrow_sec,
                                           char *tomorrow_cl_name) {
  struct tm tm_today;
  struct tm tm_tomorrow;

  time_t current_sec = my_time(0);
  localtime_r(&current_sec, &tm_today);

  tm_today.tm_sec = 0;
  tm_today.tm_min = 0;
  tm_today.tm_hour = 0;

  sprintf(today_cl_name, "%s_%04d%02d%02d", main_cl_name,
          tm_today.tm_year + 1900, tm_today.tm_mon + 1, tm_today.tm_mday);

  today_sec = mktime(&tm_today);
  tomorrow_sec = today_sec + ONE_DAY_SEC;

  localtime_r(&tomorrow_sec, &tm_tomorrow);
  sprintf(tomorrow_cl_name, "%s_%04d%02d%02d", main_cl_name,
          tm_tomorrow.tm_year + 1900, tm_tomorrow.tm_mon + 1,
          tm_tomorrow.tm_mday);
}

static int ha_ensure_slow_log_table(bson::BSONObj obj,
                                    Sdb_conn &sdb_conn,
                                    const char *cs_name,
                                    const char *main_cl_name,
                                    const char *today_cl_name,
                                    time_t today_sec,
                                    const char *tomorrow_cl_name,
                                    time_t tomorrow_sec) {
  int rc = 0;
  char err_buff[HA_BUF_LEN] = {0};
  bool has_today_cl = false;
  bool has_tomorrow_cl = false;

  if (!obj.getField(SDB_FIELD_ISMAINCL).booleanSafe() ||
      obj.getField(SDB_FIELD_CATAINFO).type() != bson::Array ||
      obj.getField(SDB_FIELD_SHARDING_KEY).type() != bson::Object ||
      obj.getField(SDB_FIELD_SHARDING_KEY).embeddedObject().
           getField(SLOW_LOG_SHARD_KEY).numberInt() != 1) {
    sql_print_warning("HA: Unexpected slow log table, rebuild it");
    rc = sdb_conn.drop_cl(cs_name, main_cl_name, NULL, true);
    HA_RC_CHECK(rc, error,
                "HA: Failed to drop slow log table, sequoiadb error: %s",
                ha_error_string(sdb_conn, rc, err_buff));

    rc = ha_create_slow_log_table(sdb_conn, cs_name, main_cl_name,
                                  today_cl_name, today_sec,
                                  tomorrow_cl_name, tomorrow_sec);
    HA_RC_CHECK(rc, error,
                "HA: Failed to create slow log table, sequoiadb error: %s",
                ha_error_string(sdb_conn, rc, err_buff));
    goto done;
  }

  {
    bson::BSONObj cata_info =
        obj.getField(SDB_FIELD_CATAINFO).embeddedObject();
    bson::BSONObjIterator it(cata_info);
    while (it.more()) {
      bson::BSONObj sub_cl_info = it.next().Obj();
      bson::BSONElement low = sub_cl_info.getField(SDB_FIELD_LOW_BOUND);
      time_t low_ms = low.Obj().getField(SLOW_LOG_SHARD_KEY).timestampTime();
      time_t low_sec = low_ms / MS_PER_SEC;
      bson::BSONElement up = sub_cl_info.getField(SDB_FIELD_UP_BOUND);
      time_t up_ms = up.Obj().getField(SLOW_LOG_SHARD_KEY).timestampTime();
      time_t up_sec = up_ms / MS_PER_SEC;
      uint &expire_days = opt_sdb_slow_log_expire_days;

      if (up_sec <= (today_sec - (expire_days * ONE_DAY_SEC))) {
        const char *sub_cl_name =
            sub_cl_info.getField(SDB_FIELD_SUBCL_NAME).valuestr();
        sub_cl_name = strchr(sub_cl_name, '.') + 1;
        rc = sdb_conn.drop_cl(cs_name, sub_cl_name, NULL, true);
        if (rc != 0) {
          sql_print_warning("HA: Failed to drop expired slow log table,"
                            "sequoiadb error: %s", 
                            ha_error_string(sdb_conn, rc, err_buff));
          rc = 0;
        }
      }
      if (low_sec <= today_sec && up_sec >= tomorrow_sec) {
        has_today_cl = true;
        continue;
      }
      if (low_sec <= tomorrow_sec &&
          up_sec >= tomorrow_sec + ONE_DAY_SEC) {
        has_tomorrow_cl = true;
        continue;
      }
    }
  }

  if (!has_today_cl) {
    rc = ha_create_and_attach_slow_log_sub_cl(sdb_conn,
                                              cs_name,
                                              main_cl_name,
                                              today_cl_name,
                                              today_sec);
    HA_RC_CHECK(rc, error,
                "HA: Failed to create today slow log table, "
                "sequoiadb error: %s",
                ha_error_string(sdb_conn, rc, err_buff));
  }

  if (!has_tomorrow_cl) {
    rc = ha_create_and_attach_slow_log_sub_cl(sdb_conn,
                                              cs_name,
                                              main_cl_name,
                                              tomorrow_cl_name,
                                              tomorrow_sec);
    HA_RC_CHECK(rc, error,
                "HA: Failed to create tomorrow slow log table, "
                "sequoiadb error: %s",
                ha_error_string(sdb_conn, rc, err_buff));
  }
done:
  return rc;
error:
  goto done;
}

static int sdb_create_mysql_slow_log_table(Sdb_conn &sdb_conn,
                                           const char *group_name,
                                           const char *group_key) {
  int rc = 0;
  char err_buf[HA_BUF_LEN] = {0};
  const char *user = NULL, *host = NULL, *plugin = NULL;
  const char *iv = NULL, *auth_str = NULL;
  const char *cipher_password = NULL, *md5_password = NULL;
  char sql_str[HA_BUF_LEN] = {0};
  char local_host_ip[HA_MAX_IP_LEN + 1] = {0};
  char inst_group_passwd[HA_MAX_PASSWD_LEN + 1] = {0};

  Sdb_cl config_cl;
  bson::BSONObj cond;
  bson::BSONObj obj;

  MYSQL *mysql_conn = NULL;

  rc = sdb_conn.get_cl(group_name, HA_CONFIG_CL, config_cl);
  HA_RC_CHECK(rc, error,
              "HA: Unable to get instance group configuration table: %s, "
              "sequoiadb error: %s",
              HA_CONFIG_CL, ha_error_string(sdb_conn, rc, err_buf));

  try {
    cond = BSON(HA_FIELD_CONFIG_TYPE << HA_CONFIG_TYPE_CONFIG);
  }
  SDB_EXCEPTION_CATCHER(
      rc, "Unexpected error '%d' while loading instance group config", rc);

  if (0 == (rc = config_cl.query(cond))) {
    rc = config_cl.next(obj, true);
  }
  HA_RC_CHECK(rc, error,
              "HA: Failed to get instance group configuration, "
              "sequoiadb error: %s",
              ha_error_string(sdb_conn, rc, err_buf));

  user = obj.getStringField(HA_FIELD_USER);
  host = obj.getStringField(HA_FIELD_HOST);
  plugin = obj.getStringField(HA_FIELD_PLUGIN);
  iv = obj.getStringField(HA_FIELD_IV);
  cipher_password = obj.getStringField(HA_FIELD_CIPHER_PASSWORD);
  auth_str = obj.getStringField(HA_FIELD_AUTH_STRING);
  md5_password = obj.getStringField(HA_FIELD_MD5_PASSWORD);

  // each field values in 'HAInstGroupConfig' can't be empty
  DBUG_ASSERT(strlen(user) && strlen(host) && strlen(plugin));
  DBUG_ASSERT(strlen(iv) && strlen(cipher_password));
  DBUG_ASSERT(strlen(auth_str) && strlen(md5_password));

  rc = ha_get_local_ip_address(local_host_ip, HA_MAX_IP_LEN + 1);
  HA_RC_CHECK(rc, error, "HA: Failed to get local IP address");

  rc = ha_decrypt_inst_group_password(cipher_password, md5_password,
                                      group_key, iv, inst_group_passwd);
  HA_RC_CHECK(rc, error,
              "HA: Failed to decrypt instance group user password");

  mysql_conn = mysql_init(NULL);
  if (NULL == mysql_conn) {
    rc = SDB_HA_EXCEPTION;
    sql_print_warning("HA: Failed to initialize mysql connection to create"
                      " mysql.sdb_slow_log table");
    goto error;
  }

  if (!mysql_real_connect(mysql_conn, local_host_ip, user,
                          inst_group_passwd, HA_MYSQL_DB,
                          mysqld_port, 0, 0)) {
    rc = SDB_HA_EXCEPTION;
    sql_print_warning(
        "HA: MySQL server '%s:%d' is not available for user '%s', "
        "mysql error: %s",
        local_host_ip, mysqld_port, user, mysql_error(mysql_conn));
    goto error;
  }

  if (mysql_real_query(mysql_conn, C_STRING_WITH_LEN(HA_STMT_EXEC_ONLY_IN_MYSQL))) {
    rc = SDB_HA_EXCEPTION;
    sql_print_warning(
        "HA: Failed to set execute_only_in_mysql on, "
        "mysql error: %s", mysql_error(mysql_conn));
    goto error;
  }

  if (mysql_real_query(mysql_conn, C_STRING_WITH_LEN(HA_STMT_CREATE_SLOW_LOG_TABLE))) {
    rc = SDB_HA_EXCEPTION;
    sql_print_warning(
        "HA: Failed to run sql to create sdb slow log table, "
        "mysql error: %s", mysql_error(mysql_conn));
    goto error;
  }

done:
  if (mysql_conn) {
    mysql_close(mysql_conn);
  }
  return rc;
error:
  goto done;
}

void *ha_maintain_slow_log_table(void *arg) {
  static const longlong SLOW_LOG_TABLE_CHECK_INTERVAL = 60 * 60; // sec

  int rc = 0;
  bool mysqld_failed = false;
  Sdb_pool_conn pool_conn(0, false);
  Sdb_conn &sdb_conn = pool_conn;
  char err_buff[HA_BUF_LEN] = {0};
  struct timespec abstime;
  ha_slow_log_table_maintain_thread *slow_log_thread =
      (ha_slow_log_table_maintain_thread *)arg;
  const char *group_name = slow_log_thread->sdb_group_name;
  const char *group_key = slow_log_thread->group_key;
  bool stopped_mutex_locked = false;
  mysql_mutex_t *current_mutex = NULL;
  const char *cs_name = group_name;
  const char *main_cl_name = HA_SLOW_LOG_CL;
  char today_cl_name[SDB_CL_NAME_MAX_SIZE + 1] = {0};
  char tomorrow_cl_name[SDB_CL_NAME_MAX_SIZE + 1] = {0};
  time_t today_sec = 0;
  time_t tomorrow_sec = 0;
  bson::BSONObj obj;
  bool has_sql_table = false;
  size_t path_len = 0;
  char path[FN_REFLEN + 1] = {0};
  time_t last_check_time = 0;
  time_t check_interval_time = SLOW_LOG_TABLE_CHECK_INTERVAL;


  THD *thd = ha_create_thd();
  if (NULL == thd) {
    SDB_LOG_ERROR("HA: Out of memory in 'slow log table maintain' thread");
    ha_kill_mysqld(thd);
    goto done;
  }

  mysqld_failed = ha_wait_for_mysqld_service();
  if (mysqld_failed) {
    SDB_LOG_ERROR(
        "HA: The mysqld process was detected to be terminating, stop 'pending "
        "log replay' thread, please check MySQL startup log");
    ha_kill_mysqld(thd);
    goto done;
  }

  ha_watch_kill_signal(thd, &slow_log_thread->stopped_cond,
                       &slow_log_thread->stopped_mutex);
  current_mutex = &slow_log_thread->stopped_mutex;

  slow_log_thread->thd = thd;

  mysql_mutex_lock(current_mutex);
  stopped_mutex_locked = true;
  slow_log_thread->stopped = false;

  // ensure mysql.sdb_slow_log exist
  build_table_filename(path, sizeof(path) - 1, MYSQL_SCHEMA_NAME.str,
                       SDB_SLOW_LOG_NAME.str, reg_ext, 0);
  if (0 == access(path, F_OK)) {
    has_sql_table = true;
  }

  srand(time(NULL));

  while (!abort_loop) {
    THD_STAGE_INFO(thd, stage_checking_table);

    bool is_sdb_slow_log_on = (opt_slow_log &&
                               (log_output_options & LOG_TABLE) &&
                               opt_sdb_slow_log);
    bool is_check_time = false;
    if (is_sdb_slow_log_on) {
      time_t current_time = my_time(0);
      if (0 == last_check_time || // this is the first time
          (current_time - last_check_time) >= check_interval_time) {
        is_check_time = true;
        last_check_time = current_time;
        int random_offset = (rand() % 121) - 60;
        check_interval_time = SLOW_LOG_TABLE_CHECK_INTERVAL + random_offset;
      }
    } else {
      last_check_time = 0;
    }

    if (is_sdb_slow_log_on && is_check_time) {
      sdb_set_debug_log(thd, sdb_debug_log(NULL));
      SDB_LOG_DEBUG("HA: Check slow log table structure");
      if (!(sdb_conn.is_valid() && sdb_conn.is_authenticated())) {
        rc = sdb_conn.connect();
        if (rc) {
          SDB_LOG_ERROR("HA: Failed to connect sequoiadb, "
                        "error code: %d", rc);
          goto sleep_secs;
        }
      }

      ha_get_today_and_tomorrow_info(main_cl_name,
                                     today_sec, today_cl_name,
                                     tomorrow_sec, tomorrow_cl_name);

      rc = sdb_conn.snapshot(obj, SDB_SNAP_CATALOG, cs_name,
                             main_cl_name, NULL);
      if (SDB_DMS_EOC == get_sdb_code(rc) || HA_ERR_END_OF_FILE == rc) {
        rc = ha_create_slow_log_table(sdb_conn, cs_name, main_cl_name,
                                      today_cl_name, today_sec,
                                      tomorrow_cl_name, tomorrow_sec);
        HA_RC_CHECK(rc, sleep_secs,
                    "HA: Failed to create slow log table, sequoiadb error: %s",
                    ha_error_string(sdb_conn, rc, err_buff));

      } else if (0 == rc) {
        rc = ha_ensure_slow_log_table(obj, sdb_conn,
                                      cs_name, main_cl_name,
                                      today_cl_name, today_sec,
                                      tomorrow_cl_name, tomorrow_sec);
        HA_RC_CHECK(rc, sleep_secs,
                    "HA: Failed to check slow log table, sequoiadb error: %s",
                    ha_error_string(sdb_conn, rc, err_buff));

      } else {
        HA_RC_CHECK(rc, sleep_secs,
                    "HA: Failed to check sdb slow log table status, "
                    "sequoiadb error: %s",
                    ha_error_string(sdb_conn, rc, err_buff));
      }

      if (!has_sql_table) {
        rc = sdb_create_mysql_slow_log_table(sdb_conn, group_name, group_key);
        HA_RC_CHECK(rc, sleep_secs,
                    "HA: Failed to create sdb slow log table, error: %d",
                    rc);
        has_sql_table = true;
      }
    }

  sleep_secs:
    thd->set_time();
    THD_STAGE_INFO(thd, stage_sleeping);
    sdb_set_clock_time_msec(abstime, 1 * MS_PER_SEC);
    rc = mysql_cond_timedwait(&slow_log_thread->stopped_cond,
                              &slow_log_thread->stopped_mutex,
                              &abstime);
    DBUG_ASSERT(rc == 0 || rc == ETIMEDOUT);
    // got a stop signal, stop this loop
    if (0 == rc) {
      SDB_LOG_INFO("HA: Receive stop signal, stop maintain slow log table");
      break;
    }
    rc = 0;
  }
done:
  if (thd) {
    ha_audit_disconnect(thd);
  }
  if (stopped_mutex_locked) {
    mysql_mutex_unlock(current_mutex);
  }
  slow_log_thread->stopped = true;
  ha_thread_end(thd);
  return NULL;
}
