/* Copyright (c) 2018, SequoiaDB and/or its affiliates. All rights reserved.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "server_ha.h"
#include "server_ha_util.h"
#include "server_ha_recover.h"
#include "ha_sdb_lock.h"
#include "ha_sdb_errcode.h"
#include "sdb_conn.h"
#include "sdb_cl.h"

#include <my_config.h>
#include <assert.h>
#include "dlfcn.h"
#include "sql_db.h"
#include "ha_sdb_conf.h"
#include "ha_sdb_thd.h"
#include "ha_sdb_log.h"
#include "sp_head.h"
#include "sql_show.h"
#include "sp.h"
#include <exception>

// thread local key for ha_sql_stmt_info
static thread_local_key_t ha_sql_stmt_info_key;
static my_thread_once_t ha_sql_stmt_info_key_once = MY_THREAD_ONCE_INIT;

static ha_recover_replay_thread ha_thread;

// original instance group name and key, instance group key will be modified
// in 'server_ha_init', so it's invisible to mysql user
static char *ha_inst_group_name = NULL;
static char *ha_inst_group_key = NULL;

static MYSQL_SYSVAR_STR(inst_group_name, ha_inst_group_name,
                        HA_PLUGIN_VAR_OPTIONS,
                        "SQL instance group name. (Default: \"\")"
                        /* 实例组名 */,
                        NULL, NULL, "");

static MYSQL_SYSVAR_STR(inst_group_key, ha_inst_group_key,
                        HA_PLUGIN_VAR_OPTIONS,
                        "Instance group key used to decrypt instance "
                        "group password. (Default: \"\")"
                        /* 实例组秘钥 */,
                        NULL, NULL, "");

static struct st_mysql_sys_var *ha_sys_vars[] = {
    MYSQL_SYSVAR(inst_group_name), MYSQL_SYSVAR(inst_group_key), NULL};

static uchar *cached_record_get_key(ha_cached_record *record, size_t *length,
                                    my_bool not_used MY_ATTRIBUTE((unused))) {
  *length = strlen(record->key);
  return (uchar *)record->key;
}

void free_cached_record_elem(void *record) {
  my_free(record);
}

// called before the worker thread exit
static void destroy_sql_stmt_info(void *sql_stmt_info) {
  ha_sql_stmt_info *sql_info = (ha_sql_stmt_info *)sql_stmt_info;
  if (sql_info->inited && sql_info->sdb_conn) {
    delete sql_info->sdb_conn;
  }
  free(sql_info);
}

// called only once for each worker thread
static void init_sql_stmt_info_key() {
  my_create_thread_local_key(&ha_sql_stmt_info_key, destroy_sql_stmt_info);
}

static ha_sql_stmt_info *get_sql_stmt_info() {
  ha_sql_stmt_info *sql_info = NULL;
  my_thread_once(&ha_sql_stmt_info_key_once, init_sql_stmt_info_key);
  sql_info = (ha_sql_stmt_info *)my_get_thread_local(ha_sql_stmt_info_key);
  if (0 == sql_info) {
    // can't use sdb_my_malloc, because mariadb will check memory
    // it use after shutdown
    sql_info = (ha_sql_stmt_info *)malloc(sizeof(ha_sql_stmt_info));
    if (sql_info) {
      memset(sql_info->err_message, 0, HA_BUF_LEN);
      memset(sql_info, '0', sizeof(ha_sql_stmt_info));
      sql_info->inited = false;
      my_set_thread_local(ha_sql_stmt_info_key, sql_info);
    }
  }
  return sql_info;
}

static void update_sql_stmt_info(ha_sql_stmt_info *sql_info,
                                 unsigned int event_class, const void *ev,
                                 THD *thd) {
  const struct mysql_event_general *event =
      (const struct mysql_event_general *)ev;
  if (!sql_info->inited) {
    sql_info->inited = true;
    sql_info->tables = NULL;
    sql_info->sdb_conn = new Sdb_conn(event->general_thread_id);
    if (sql_info->sdb_conn) {
      SDB_LOG_DEBUG("HA: Init sequoiadb connection");
      sql_info->sdb_conn->set_use_transaction(true);
    }
  } else if (NULL == sql_info->sdb_conn) {
    // if client execute some DDL statements, exit session,
    // ha_sql_stmt_info::sdb_conn will be set to NULL.
    sql_info->tables = NULL;
    sql_info->sdb_conn = new Sdb_conn(event->general_thread_id);
    if (sql_info->sdb_conn) {
      SDB_LOG_DEBUG("HA: Init sequoiadb connection");
      sql_info->sdb_conn->set_use_transaction(true);
    }
  }
}

// start transaction for current SQL statement being executed
static int begin_transaction(THD *thd, ha_sql_stmt_info *sql_info) {
  int rc = 0;
  // set 'transisolation' to RS, the number of trans isolation RS is
  // occupied by MySQL, use number 4 instead, refer to
  // 'convert_to_sdb_isolation'
  static const int HA_TRANS_ISO_RS = 4;
  rc = sql_info->sdb_conn->begin_transaction(HA_TRANS_ISO_RS);
  if (rc) {
    ha_error_string(*sql_info->sdb_conn, rc, sql_info->err_message);
  }
  return rc;
}

static bool is_routine_meta_sql(THD *thd) {
  int sql_command = thd_sql_command(thd);
  bool is_routine = false;
  switch (sql_command) {
    case SQLCOM_CREATE_PROCEDURE:
    case SQLCOM_ALTER_PROCEDURE:
    case SQLCOM_DROP_PROCEDURE:
    case SQLCOM_CREATE_FUNCTION:
    case SQLCOM_ALTER_FUNCTION:
    case SQLCOM_DROP_FUNCTION:
    case SQLCOM_CREATE_TRIGGER:
    case SQLCOM_DROP_TRIGGER:
    case SQLCOM_CREATE_EVENT:
    case SQLCOM_ALTER_EVENT:
    case SQLCOM_DROP_EVENT:
    case SQLCOM_CREATE_SPFUNCTION:
      is_routine = true;
      break;
  }
  return is_routine;
}

static bool is_dcl_meta_sql(THD *thd) {
  int sql_command = thd_sql_command(thd);
  bool is_dcl = false;
  switch (sql_command) {
    case SQLCOM_GRANT:
    case SQLCOM_DROP_USER:
    case SQLCOM_REVOKE:
    case SQLCOM_RENAME_USER:
    case SQLCOM_CREATE_USER:
    case SQLCOM_REVOKE_ALL:
    case SQLCOM_ALTER_USER:
#ifdef IS_MARIADB
    case SQLCOM_DROP_ROLE:
    case SQLCOM_GRANT_ROLE:
    case SQLCOM_CREATE_ROLE:
    case SQLCOM_REVOKE_ROLE:
#endif
      is_dcl = true;
      break;
  }
  return is_dcl;
}

static bool is_meta_sql(THD *thd, const ha_event_general &event) {
  int sql_command = thd_sql_command(thd);
  bool is_meta_sql = false;
  switch (sql_command) {
    case SQLCOM_CREATE_TABLE:
    case SQLCOM_CREATE_INDEX:
    case SQLCOM_ALTER_TABLE:
    case SQLCOM_TRUNCATE:
    case SQLCOM_DROP_TABLE:
    case SQLCOM_DROP_INDEX:
    case SQLCOM_CREATE_DB:
    case SQLCOM_GRANT:
    case SQLCOM_DROP_DB:
    case SQLCOM_CREATE_FUNCTION:
    case SQLCOM_ALTER_DB:
    case SQLCOM_DROP_FUNCTION:
    case SQLCOM_REVOKE:
    case SQLCOM_RENAME_TABLE:
    case SQLCOM_CREATE_USER:
    case SQLCOM_DROP_USER:
    case SQLCOM_RENAME_USER:
    case SQLCOM_REVOKE_ALL:
    case SQLCOM_CREATE_PROCEDURE:
    case SQLCOM_CREATE_SPFUNCTION:
    case SQLCOM_DROP_PROCEDURE:
    case SQLCOM_ALTER_PROCEDURE:
    case SQLCOM_ALTER_FUNCTION:
    case SQLCOM_CREATE_VIEW:
    case SQLCOM_DROP_VIEW:
    case SQLCOM_CREATE_TRIGGER:
    case SQLCOM_DROP_TRIGGER:
    case SQLCOM_CREATE_EVENT:
    case SQLCOM_ALTER_EVENT:
    case SQLCOM_DROP_EVENT:
    case SQLCOM_ALTER_USER:
#ifdef IS_MARIADB
    case SQLCOM_DROP_ROLE:
    case SQLCOM_GRANT_ROLE:
    case SQLCOM_REVOKE_ROLE:
    case SQLCOM_CREATE_ROLE:
#endif
      is_meta_sql = true;
      break;
  }
  return is_meta_sql;
}

static bool is_db_meta_sql(THD *thd) {
  int sql_command = thd_sql_command(thd);
  return (SQLCOM_CREATE_DB == sql_command || SQLCOM_DROP_DB == sql_command ||
          SQLCOM_ALTER_DB == sql_command);
}

// check if current SQL statement has 'temporary' flag
// only for create/drop table
static inline bool has_temporary_table_flag(THD *thd) {
  bool is_temp_table_op = false;
  int sql_command = thd_sql_command(thd);
  is_temp_table_op = (SQLCOM_CREATE_TABLE == sql_command) &&
                     (thd->lex->create_info.options & HA_LEX_CREATE_TMP_TABLE);
#ifdef IS_MYSQL
  is_temp_table_op |=
      (SQLCOM_DROP_TABLE == sql_command && thd->lex->drop_temporary);
#else
  is_temp_table_op |= (SQLCOM_DROP_TABLE == sql_command) &&
                      (thd->lex->create_info.options & HA_LEX_CREATE_TMP_TABLE);
#endif
  return is_temp_table_op;
}

// query table 'HASQLLogState' and get all objects to be dropped in database
// add dropped database info at last
static int get_drop_db_objects(THD *thd, ha_sql_stmt_info *sql_info) {
  DBUG_ENTER("get_drop_db_objects");
  DBUG_ASSERT(sql_info->tables == NULL);
  int rc = 0;
  Sdb_cl log_state_cl;
  bson::BSONObj cond, obj, result;
  Sdb_conn *sdb_conn = sql_info->sdb_conn;
  const char *db_name = thd->lex->name.str;
  ha_table_list *ha_tbl_list = NULL, *ha_tbl_list_tail = NULL;

  rc = ha_get_sql_log_state_cl(*sdb_conn, ha_thread.sdb_group_name,
                               log_state_cl);
  if (rc) {
    ha_error_string(*sdb_conn, rc, sql_info->err_message);
    goto error;
  }

  cond = BSON(HA_FIELD_DB << db_name);
  rc = log_state_cl.query(cond);
  if (rc) {
    ha_error_string(*sdb_conn, rc, sql_info->err_message);
    goto error;
  }

  while (log_state_cl.next(result, false) && !result.isEmpty()) {
    const char *db_name = result.getStringField(HA_FIELD_DB);
    const char *table_name = result.getStringField(HA_FIELD_TABLE);
    const char *op_type = result.getStringField(HA_FIELD_TYPE);
    if (0 == strcmp(op_type, HA_OPERATION_TYPE_DCL)) {
      continue;
    }

    ha_tbl_list = (ha_table_list *)thd_calloc(thd, sizeof(ha_table_list));
    if (ha_tbl_list) {
      ha_tbl_list->db_name = (char *)thd_calloc(thd, strlen(db_name) + 1);
      ha_tbl_list->table_name = (char *)thd_calloc(thd, strlen(table_name) + 1);
      ha_tbl_list->op_type = (char *)thd_calloc(thd, strlen(op_type) + 1);
    }

    if (NULL == ha_tbl_list || NULL == ha_tbl_list->db_name ||
        NULL == ha_tbl_list->table_name || NULL == ha_tbl_list->op_type) {
      rc = HA_ERR_OUT_OF_MEM;
      goto error;
    }
    sprintf((char *)ha_tbl_list->db_name, "%s", db_name);
    sprintf((char *)ha_tbl_list->table_name, "%s", table_name);
    sprintf((char *)ha_tbl_list->op_type, "%s", op_type);
    ha_tbl_list->is_temporary_table = false;
    ha_tbl_list->next = NULL;
    if (!sql_info->tables) {
      sql_info->tables = ha_tbl_list;
      ha_tbl_list_tail = ha_tbl_list;
    } else {
      ha_tbl_list_tail->next = ha_tbl_list;
      ha_tbl_list_tail = ha_tbl_list;
    }
  }
  // at last, add 'drop database ' info
  {
    ha_tbl_list = (ha_table_list *)thd_calloc(thd, sizeof(ha_table_list));
    if (ha_tbl_list) {
      ha_tbl_list->db_name = (char *)thd_calloc(thd, strlen(db_name) + 1);
      ha_tbl_list->table_name = HA_EMPTY_STRING;
      ha_tbl_list->op_type = HA_OPERATION_TYPE_DB;
    }
    if (NULL == ha_tbl_list || NULL == ha_tbl_list->db_name) {
      rc = HA_ERR_OUT_OF_MEM;
      goto error;
    }
    sprintf((char *)ha_tbl_list->db_name, "%s", db_name);
    ha_tbl_list->is_temporary_table = false;
    ha_tbl_list->next = NULL;
    if (!sql_info->tables) {
      sql_info->tables = ha_tbl_list;
    } else {
      ha_tbl_list_tail->next = ha_tbl_list;
    }
  }
done:
  DBUG_RETURN(rc);
error:
  goto done;
}

// try to lock a record in 'HALock' table
// 1. set a shorter 'TransTimeout' before add 'X' lock
// 2. try locking again 2 times after timeout
inline static int try_add_xlock(Sdb_conn &sdb_conn, Sdb_cl &lock_cl,
                                const bson::BSONObj &cond, bson::BSONObj &obj) {
  static const int MAX_TRY_COUNT = 3;
  static const int HA_TEMP_TRANS_TIMEOUT = 15;

  int rc = 0;
  bson::BSONObj attr;
  // set 'TransTimeout' to 15, try 3 times
  attr = BSON(HA_TRANSACTION_TIMEOUT << HA_TEMP_TRANS_TIMEOUT);
  rc = sdb_conn.set_session_attr(attr);
  if (0 == rc) {
    for (int i = 0; i < MAX_TRY_COUNT; i++) {
      rc = lock_cl.upsert(obj, cond);
      if (0 == rc || SDB_TIMEOUT != get_sdb_code(rc)) {
        // add X lock succeeded or extra error
        break;
      } else {
        sleep(1);
      }
    }
    // restore TransTimeout
    attr = BSON(HA_TRANSACTION_TIMEOUT << 60);
    sdb_conn.set_session_attr(attr);
  }
  return rc;
}

// add extra lock for objects involved in current SQL statement
static int pre_lock_objects(THD *thd, ha_sql_stmt_info *sql_info) {
  int rc = 0;
  Sdb_conn *sdb_conn = sql_info->sdb_conn;
  Sdb_cl lock_cl;
  bson::BSONObj cond, obj, result;
  int sql_command = thd_sql_command(thd);
  ha_table_list *ha_tables = sql_info->tables;
  const char *db_name = NULL, *table_name = NULL;

  rc = sdb_conn->get_cl(ha_thread.sdb_group_name, HA_LOCK_CL, lock_cl);
  if (rc) {
    ha_error_string(*sdb_conn, rc, sql_info->err_message);
    goto error;
  }
  if (SQLCOM_CREATE_DB == sql_command || SQLCOM_ALTER_DB == sql_command) {
    // add 'X' lock for current database in 'lock_record' later
    goto done;
  } else if (SQLCOM_DROP_DB == sql_command) {  // if it's 'DROP DATABASE '
    // add 'X' lock for current database
    db_name = thd->lex->name.str;
    table_name = HA_EMPTY_STRING;
    cond = BSON(HA_FIELD_DB << db_name << HA_FIELD_TABLE << table_name
                            << HA_FIELD_TYPE << HA_OPERATION_TYPE_DB);
    obj = BSON("$inc" << BSON(HA_FIELD_VERSION << 1));

    SDB_LOG_DEBUG("HA: Add 'X' lock for '%s:%s'", db_name, table_name);
    rc = lock_cl.upsert(obj, cond);
    if (rc) {
      ha_error_string(*sdb_conn, rc, sql_info->err_message);
      goto error;
    }
  } else if (is_routine_meta_sql(thd) && strlen(sql_info->sp_db_name)) {
    // add 'S' lock for database including this routine
    db_name = sql_info->sp_db_name;
    table_name = HA_EMPTY_STRING;
    cond = BSON(HA_FIELD_DB << db_name << HA_FIELD_TABLE << table_name
                            << HA_FIELD_TYPE << HA_OPERATION_TYPE_DB);

    SDB_LOG_DEBUG("HA: Add 'S' lock for '%s:%s'", db_name, table_name);
    rc = lock_cl.query(cond);
    rc = rc ? rc : lock_cl.next(result, false);
    if (HA_ERR_END_OF_FILE == rc) {
      obj = BSON("$inc" << BSON(HA_FIELD_VERSION << 1));
      rc = lock_cl.upsert(obj, cond);
    }
    if (rc) {
      ha_error_string(*sdb_conn, rc, sql_info->err_message);
      goto error;
    }
    // add 'X' lock for table in 'CREATE TRIGGER'
    if (ha_tables) {
      db_name = ha_tables->db_name;
      table_name = ha_tables->table_name;

      SDB_LOG_DEBUG("HA: Add 'X' lock for '%s:%s'", db_name, table_name);
      cond = BSON(HA_FIELD_DB << db_name << HA_FIELD_TABLE << table_name
                              << HA_FIELD_TYPE << HA_OPERATION_TYPE_TABLE);
      obj = BSON("$inc" << BSON(HA_FIELD_VERSION << 1));
      rc = lock_cl.upsert(obj, cond);
      if (rc) {
        ha_error_string(*sdb_conn, rc, sql_info->err_message);
        goto error;
      }
    }
  } else if (is_dcl_meta_sql(thd) && ha_tables &&
             (0 == thd->lex->type || TYPE_ENUM_PROCEDURE == thd->lex->type ||
              TYPE_ENUM_FUNCTION == thd->lex->type)) {
    const char *op_type = NULL;
    // grant object can be 'TABLE/FUNCTION/PROCEDURE'
    // add 'S' lock for database include 'routine' or table
    db_name = ha_tables->db_name;
    table_name = HA_EMPTY_STRING;
    cond = BSON(HA_FIELD_DB << db_name << HA_FIELD_TABLE << table_name
                            << HA_FIELD_TYPE << HA_OPERATION_TYPE_DB);

    SDB_LOG_DEBUG("HA: Add 'S' lock for '%s:%s'", db_name, table_name);
    rc = lock_cl.query(cond);
    rc = rc ? rc : lock_cl.next(result, false);
    if (HA_ERR_END_OF_FILE == rc) {
      obj = BSON("$inc" << BSON(HA_FIELD_VERSION << 1));
      rc = lock_cl.upsert(obj, cond);
    }
    if (rc) {
      ha_error_string(*sdb_conn, rc, sql_info->err_message);
      goto error;
    }

    // add 'X' lock for granted objects(table or 'routine')
    db_name = ha_tables->db_name;
    table_name = ha_tables->table_name;
    if (TYPE_ENUM_PROCEDURE == thd->lex->type) {
      op_type = HA_ROUTINE_TYPE_PROC;
    } else if (TYPE_ENUM_FUNCTION == thd->lex->type) {
      op_type = HA_ROUTINE_TYPE_FUNC;
    } else {
      op_type = HA_OPERATION_TYPE_TABLE;
    }
    cond = BSON(HA_FIELD_DB << db_name << HA_FIELD_TABLE << table_name
                            << HA_FIELD_TYPE << op_type);
    obj = BSON("$inc" << BSON(HA_FIELD_VERSION << 1));

    SDB_LOG_DEBUG("HA: Add 'X' lock for '%s:%s'", db_name, table_name);
    rc = lock_cl.upsert(obj, cond);
    if (rc) {
      ha_error_string(*sdb_conn, rc, sql_info->err_message);
      goto error;
    }
  } else {
    // add 'S' lock for databases
    for (; ha_tables; ha_tables = ha_tables->next) {
      const char *db_name = ha_tables->db_name;
      const char *table_name = HA_EMPTY_STRING;
      cond = BSON(HA_FIELD_DB << db_name << HA_FIELD_TABLE << table_name
                              << HA_FIELD_TYPE << HA_OPERATION_TYPE_DB);

      SDB_LOG_DEBUG("HA: Add 'S' lock for '%s:%s'", db_name, table_name);
      rc = lock_cl.query(cond);
      rc = rc ? rc : lock_cl.next(result, false);
      if (HA_ERR_END_OF_FILE == rc) {
        obj = BSON("$inc" << BSON(HA_FIELD_VERSION << 1));
        rc = lock_cl.upsert(obj, cond);
      }
      if (rc) {
        ha_error_string(*sdb_conn, rc, sql_info->err_message);
        goto error;
      }
    }
  }
done:
  return rc;
error:
  goto done;
}

// lock records in 'HALock' tables for current SQL statement
// for example: 'drop table db1.t1'
// 1. pre_lock_record: add 'S' lock for db1
// 2. lock_record: add 'X' lock for db1:t1
static int lock_objects(THD *thd, ha_sql_stmt_info *sql_info) {
  int rc = 0;
  Sdb_conn *sdb_conn = sql_info->sdb_conn;
  Sdb_cl lock_cl;
  ha_table_list *tables = sql_info->tables;
  bson::BSONObj cond, obj;
  int sql_command = thd_sql_command(thd);

  rc = ha_get_lock_cl(*sdb_conn, ha_thread.sdb_group_name, lock_cl);
  if (rc) {
    ha_error_string(*sdb_conn, rc, sql_info->err_message);
    goto error;
  }

  // add extra lock for objects involved in current SQL statement
  rc = pre_lock_objects(thd, sql_info);
  if (rc) {
    goto error;
  }

  for (; tables; tables = tables->next) {
    const char *db_name = tables->db_name;
    const char *table_name = tables->table_name;
    const char *op_type = tables->op_type;
    if (!db_name || !table_name || 0 == strlen(db_name)) {
      // handle 'CREATE VIEW bug22108567_v1 AS SELECT 1 FROM (SELECT 1) AS D1'
      // 'drop function if exists' without select database report no errors
      continue;
    }

    cond = BSON(HA_FIELD_DB << db_name << HA_FIELD_TABLE << table_name
                            << HA_FIELD_TYPE << op_type);
    obj = BSON("$inc" << BSON(HA_FIELD_VERSION << 1));

    SDB_LOG_DEBUG("HA: Add 'X' lock for '%s:%s'", db_name, table_name);
    rc = try_add_xlock(*sql_info->sdb_conn, lock_cl, cond, obj);
    if (rc) {
      ha_error_string(*sdb_conn, rc, sql_info->err_message);
      goto error;
    }
  }
done:
  return rc;
error:
  goto done;
}

ha_cached_record *get_cached_record(HASH &cache,
                                    const char *cached_record_key) {
  DBUG_ENTER("get_inst_state_sql_id");
  void *cached_record = NULL;
  uchar *key = NULL;
  key = (uchar *)cached_record_key;
  cached_record = my_hash_search(&cache, key, strlen(cached_record_key));
  DBUG_RETURN((ha_cached_record *)cached_record);
}

// wait instance state to be updated to lastest state by replay thread
static int wait_object_updated_to_lastest(
    const char *db_name, const char *table_name, const char *op_type,
    Sdb_cl &log_state_cl, ha_sql_stmt_info *sql_info, THD *thd) {
  DBUG_ENTER("wait_for_instance_state");
  static const int WAIT_REPLAY_THREAD_TIMEOUT = 30;

  int sql_id = HA_INVALID_SQL_ID, rc = 0;
  bson::BSONObj cond, result;
  Sdb_conn *sdb_conn = sql_info->sdb_conn;
  ha_cached_record *cached_record = NULL;
  char cached_record_key[HA_MAX_CACHED_RECORD_KEY_LEN] = {0};
  int sleep_secs = 0;

  // get latest SQL id from 'HASQLLogState'
  cond = BSON(HA_FIELD_DB << db_name << HA_FIELD_TABLE << table_name
                          << HA_FIELD_TYPE << op_type);
  rc = log_state_cl.query(cond);
  rc = rc ? rc : log_state_cl.next(result, false);
  if (0 == rc) {
    sql_id = result.getIntField(HA_FIELD_SQL_ID);
    DBUG_ASSERT(sql_id >= 0);
  } else if (HA_ERR_END_OF_FILE == rc) {
    // can't find global state in 'HASQLLogState'
    rc = 0;
    goto done;
  }
  if (rc) {
    ha_error_string(*sdb_conn, rc, sql_info->err_message);
    goto error;
  }

  // get local instance state from cached instance state
  snprintf(cached_record_key, HA_MAX_CACHED_RECORD_KEY_LEN, "%s-%s-%s", db_name,
           table_name, op_type);
  do {
    cached_record =
        get_cached_record(ha_thread.inst_state_cache, cached_record_key);
    // if 'db_name:table_name:op_type' does not exists
    if (!abort_loop && !cached_record) {
      sleep(1);
      sleep_secs++;
    }
  } while (!abort_loop && !cached_record &&
           sleep_secs < WAIT_REPLAY_THREAD_TIMEOUT && !thd_killed(thd));

  if (sleep_secs >= WAIT_REPLAY_THREAD_TIMEOUT) {
    rc = HA_ERR_WAIT_TIMEOUT;
    goto error;
  } else if (thd_killed(thd) || abort_loop) {
    rc = HA_ERR_ABORT_BY_USER;
    goto error;
  }

  // if local sql_id less than global sql id on table 'table_name'
  sleep_secs = 0;
  SDB_LOG_DEBUG("HA: Wait for '%s' state, cached SQL ID: %d, global SQL ID: %d",
                cached_record_key, cached_record->sql_id, sql_id);
  while (!abort_loop && cached_record && cached_record->sql_id < sql_id &&
         sleep_secs < WAIT_REPLAY_THREAD_TIMEOUT && !thd_killed(thd)) {
    sleep(1);
    sleep_secs++;
  }

  if (sleep_secs >= WAIT_REPLAY_THREAD_TIMEOUT) {
    rc = HA_ERR_WAIT_TIMEOUT;
    goto error;
  } else if (thd_killed(thd) || abort_loop) {
    rc = HA_ERR_ABORT_BY_USER;
    goto error;
  }
done:
  DBUG_RETURN(rc);
error:
  goto done;
}

// check if operations on related tables and databases are latest
static int pre_wait_objects_updated_to_lastest(THD *thd,
                                               ha_sql_stmt_info *sql_info) {
  int rc = 0;
  Sdb_conn *sdb_conn = sql_info->sdb_conn;
  Sdb_cl log_state_cl, inst_state_cl;
  const char *db_name = NULL, *table_name = NULL, *op_type = NULL;
  ha_table_list *ha_tables = sql_info->tables;

  rc = sdb_conn->get_cl(ha_thread.sdb_group_name, HA_SQL_LOG_STATE_CL,
                        log_state_cl);
  if (rc) {
    goto sdb_error;
  }
  rc = sdb_conn->get_cl(ha_thread.sdb_group_name, HA_INSTANCE_STATE_CL,
                        inst_state_cl);
  if (rc) {
    goto sdb_error;
  }

  if (is_db_meta_sql(thd)) {
    db_name = thd->lex->name.str;
    table_name = HA_EMPTY_STRING;
    op_type = HA_OPERATION_TYPE_DB;
    rc = wait_object_updated_to_lastest(db_name, table_name, op_type,
                                        log_state_cl, sql_info, thd);
    if (0 == rc && SQLCOM_DROP_DB == thd_sql_command(thd)) {
      rc = get_drop_db_objects(thd, sql_info);
    }
    if (rc) {
      goto error;
    }
  } else if (is_routine_meta_sql(thd) && strlen(sql_info->sp_db_name)) {
    db_name = sql_info->sp_db_name;
    table_name = HA_EMPTY_STRING;
    op_type = HA_OPERATION_TYPE_DB;
    rc = wait_object_updated_to_lastest(db_name, table_name, op_type,
                                        log_state_cl, sql_info, thd);
    // if table != NULL means current operation is create trigger on table
    if (ha_tables && 0 == rc) {
      db_name = ha_tables->db_name;
      table_name = ha_tables->table_name;
      op_type = HA_OPERATION_TYPE_TABLE;
      rc = wait_object_updated_to_lastest(db_name, table_name, op_type,
                                          log_state_cl, sql_info, thd);
    }
    if (rc) {
      goto error;
    }
  } else if (is_dcl_meta_sql(thd) && ha_tables &&
             (0 == thd->lex->type || TYPE_ENUM_PROCEDURE == thd->lex->type ||
              TYPE_ENUM_FUNCTION == thd->lex->type)) {
    // lex->type == 0 means that grant object is table
    db_name = ha_tables->db_name;
    table_name = HA_EMPTY_STRING;
    op_type = HA_OPERATION_TYPE_DB;
    rc = wait_object_updated_to_lastest(db_name, table_name, op_type,
                                        log_state_cl, sql_info, thd);
    if (rc) {
      goto error;
    }

    db_name = ha_tables->db_name;
    table_name = ha_tables->table_name;
    if (TYPE_ENUM_PROCEDURE == thd->lex->type) {
      op_type = HA_ROUTINE_TYPE_PROC;
    } else if (TYPE_ENUM_FUNCTION == thd->lex->type) {
      op_type = HA_ROUTINE_TYPE_FUNC;
    } else {
      op_type = HA_OPERATION_TYPE_TABLE;
    }
    rc = wait_object_updated_to_lastest(db_name, table_name, op_type,
                                        log_state_cl, sql_info, thd);
    if (rc) {
      goto error;
    }
  } else if (!is_db_meta_sql(thd) && ha_tables) {
    do {
      db_name = ha_tables->db_name;
      table_name = HA_EMPTY_STRING;
      op_type = HA_OPERATION_TYPE_DB;
      if (db_name) {
        rc = wait_object_updated_to_lastest(db_name, table_name, op_type,
                                            log_state_cl, sql_info, thd);
      }
      ha_tables = ha_tables->next;
    } while (ha_tables && (0 == rc));
  }
done:
  return rc;
sdb_error:
  ha_error_string(*sdb_conn, rc, sql_info->err_message);
  goto done;
error:
  goto done;
}

// check and wait for current instance to be updated by 'HA' thread
// to the lastest state
static int wait_objects_updated_to_lastest(THD *thd,
                                           ha_sql_stmt_info *sql_info) {
  int rc = 0;
  Sdb_conn *sdb_conn = sql_info->sdb_conn;
  Sdb_cl sql_log_state_cl, inst_state_cl;
  ha_table_list *ha_tables = sql_info->tables;

  // get sql log state handle
  rc = ha_get_sql_log_state_cl(*sql_info->sdb_conn, ha_thread.sdb_group_name,
                               sql_log_state_cl);
  if (rc) {
    ha_error_string(*sdb_conn, rc, sql_info->err_message);
    goto error;
  }

  rc = ha_get_instance_state_cl(*sql_info->sdb_conn, ha_thread.sdb_group_name,
                                inst_state_cl);
  if (rc) {
    ha_error_string(*sdb_conn, rc, sql_info->err_message);
    goto error;
  }

  rc = pre_wait_objects_updated_to_lastest(thd, sql_info);
  if (rc) {
    goto error;
  }

  // check if operations on tables for current instance is latest
  for (; ha_tables; ha_tables = ha_tables->next) {
    const char *db_name = ha_tables->db_name;
    const char *table_name = ha_tables->table_name;
    const char *op_type = ha_tables->op_type;
    if (!db_name || !table_name || 0 == strlen(db_name)) {
      // handle 'CREATE VIEW bug22108567_v1 AS SELECT 1 FROM (SELECT 1) AS D1'
      // 'drop function if exist' without select db report no errors
      break;
    }
    rc = wait_object_updated_to_lastest(db_name, table_name, op_type,
                                        sql_log_state_cl, sql_info, thd);
    if (rc) {
      goto error;
    }
  }
done:
  return rc;
error:
  goto done;
}

static bool build_full_table_name(String &full_name, const char *db_name,
                                  const char *table_name) {
  char quoted_name_buf[NAME_LEN * 2 + 3] = {0};
  bool error = 0;
  ha_quote_name(db_name, quoted_name_buf);
  error |= full_name.append(quoted_name_buf);
  error |= full_name.append('.');

  memset(quoted_name_buf, 0, NAME_LEN * 2 + 3);
  ha_quote_name(table_name, quoted_name_buf);
  error |= full_name.append(quoted_name_buf);
}

// check if the dropping table exists, SQL like 'drop table [if exists]
// t1,t2,t3' if 't2' does not exist, the error or warning will be "Unknown table
// 'db1.t2'" in this situation, SQL log "DROP TABLE `db1`.`t2`" can't be written
// into sequoiadb
static bool dropping_table_exists(THD *thd, const char *db_name,
                                  const char *table_name) {
  bool table_exists = true;
  char unknown_table_buf[NAME_LEN * 2 + 20] = {0};
  String wrong_table(unknown_table_buf, NAME_LEN * 4 + 20, system_charset_info);
  wrong_table.length(0);

  if (!thd->is_error()) {
    // handle drop table if exists
    wrong_table.append("Unknown table '");
    wrong_table.append(db_name);
    wrong_table.append('.');
    wrong_table.append(table_name);
    wrong_table.append("'");
    if (thd->get_stmt_da()->has_sql_condition(wrong_table.c_ptr(),
                                              wrong_table.length())) {
      table_exists = false;
    }
  } else if (sdb_has_sql_condition(thd, ER_BAD_TABLE_ERROR)
#ifdef IS_MARIADB
             || sdb_has_sql_condition(thd, ER_UNKNOWN_VIEW)
#endif
  ) {
    wrong_table.append(db_name);
    wrong_table.append('.');
    wrong_table.append(table_name);
#ifdef IS_MYSQL
    const char *err_msg = thd->get_stmt_da()->message_text();
#else
    const char *err_msg = thd->get_stmt_da()->message();
#endif
    if (strstr(err_msg, wrong_table.c_ptr_safe())) {
      table_exists = false;
    }
  }
  return table_exists;
}

// check if current execution has 'XXX not exist' or 'XXX already exists'
// warnings
inline static bool have_exist_warning(THD *thd) {
  bool have_warning = false;
  ulong warn_count = 0;
#ifdef IS_MYSQL
  warn_count = thd->get_stmt_da()->current_statement_cond_count();
#else
  warn_count = thd->get_stmt_da()->current_statement_warn_count();
#endif
  if (warn_count) {
    have_warning = sdb_has_sql_condition(thd, ER_DB_CREATE_EXISTS) ||
                   sdb_has_sql_condition(thd, ER_DB_DROP_EXISTS) ||
                   sdb_has_sql_condition(thd, ER_SP_ALREADY_EXISTS) ||
                   sdb_has_sql_condition(thd, ER_SP_DOES_NOT_EXIST) ||
                   sdb_has_sql_condition(thd, ER_TRG_ALREADY_EXISTS) ||
                   sdb_has_sql_condition(thd, ER_TRG_DOES_NOT_EXIST) ||
                   sdb_has_sql_condition(thd, ER_EVENT_ALREADY_EXISTS) ||
                   sdb_has_sql_condition(thd, ER_EVENT_DOES_NOT_EXIST);
#ifdef IS_MYSQL
    have_warning = have_warning ||
                   sdb_has_sql_condition(thd, ER_USER_ALREADY_EXISTS) ||
                   sdb_has_sql_condition(thd, ER_USER_DOES_NOT_EXIST);
#else
    have_warning = have_warning ||
                   sdb_has_sql_condition(thd, ER_USER_CREATE_EXISTS) ||
                   sdb_has_sql_condition(thd, ER_USER_DROP_EXISTS) ||
                   sdb_has_sql_condition(thd, ER_ROLE_CREATE_EXISTS) ||
                   sdb_has_sql_condition(thd, ER_ROLE_DROP_EXISTS);
#endif
  }
  return have_warning;
}

static inline bool is_temporary_table(THD *thd, const char *db_name,
                                      const char *table_name) {
  TABLE *table = NULL;
#ifdef IS_MYSQL
  table = find_temporary_table(thd, db_name, table_name);
#else
  THD::Temporary_table_state state = THD::TMP_TABLE_NOT_IN_USE;
  table = thd->find_temporary_table(db_name, table_name, state);
#endif
  return table != NULL;
}

// build set session SQL statement for replaying thread
static inline void build_session_attributes(THD *thd, char *session_attrs) {
  int sql_command = thd_sql_command(thd), end = 0;

  // set character set for current sql, refer to mysql binlog
  end += snprintf(
      session_attrs + end, HA_MAX_SESSION_ATTRS_LEN,
      "SET "
      "@@session.character_set_client=%d,@@session.collation_connection="
      "%d,@@session.collation_server=%d",
      thd->variables.character_set_client->number,
      thd->variables.collation_connection->number,
      thd->variables.collation_server->number);
  if (SQLCOM_CREATE_DB == sql_command) {
    end += snprintf(session_attrs + end, HA_MAX_SESSION_ATTRS_LEN,
                    ",@@session.collation_database=%d",
                    thd->variables.collation_database->number);
  }

  // set sql_mode
  end += snprintf(session_attrs + end, HA_MAX_SESSION_ATTRS_LEN,
                  ",@@session.sql_mode=%lld", thd->variables.sql_mode);

  // set 'explicit_defaults_for_timestamp' system variable
  if (SQLCOM_CREATE_TABLE == sql_command || SQLCOM_ALTER_TABLE == sql_command) {
#ifdef IS_MYSQL
    end += snprintf(session_attrs + end, HA_MAX_SESSION_ATTRS_LEN,
                    ",@@session.explicit_defaults_for_timestamp=%d",
                    thd->variables.explicit_defaults_for_timestamp);
#endif
  }
  // set default_storage_engine for 'create table '
  if (SQLCOM_CREATE_TABLE == sql_command) {
    end += snprintf(session_attrs + end, HA_MAX_SESSION_ATTRS_LEN,
                    ",@@session.default_storage_engine=%s",
                    plugin_name(thd->variables.table_plugin)->str);
  }
}

// 1. write SQL into 'HASQLLog'
// 2. update 'HASQLLogState' and 'HAInstanceState'
static int write_sql_log_and_states(THD *thd, ha_sql_stmt_info *sql_info,
                                    const ha_event_general &event) {
  int rc = 0, sql_id = -1;
  Sdb_conn *sdb_conn = sql_info->sdb_conn;
  Sdb_cl sql_log_cl, sql_log_state_cl, inst_state_cl, lock_cl;
  ha_table_list *ha_tables = sql_info->tables;
  bson::BSONObj obj, result, cond, hint;
  int sql_command = thd_sql_command(thd);
  char quoted_name_buf[NAME_LEN * 2 + 3] = {0};
  char session_attrs[HA_MAX_SESSION_ATTRS_LEN] = {0};
  int client_charset_num = -1;
  String general_query, query;
  query.set_charset(thd->variables.character_set_client);
  char cached_record_key[NAME_LEN * 2 + 20] = {0};
  bool oom = false;  // out of memory while building a string

  oom = general_query.append(event.general_query, event.general_query_length);
  oom |= general_query.append('\0');
  if (oom) {
    rc = HA_ERR_OUT_OF_MEM;
    goto error;
  }

  rc = sdb_conn->get_cl(ha_thread.sdb_group_name, HA_SQL_LOG_CL, sql_log_cl);
  if (rc) {
    goto sdb_error;
  }

  rc = sdb_conn->get_cl(ha_thread.sdb_group_name, HA_SQL_LOG_STATE_CL,
                        sql_log_state_cl);
  if (rc) {
    goto sdb_error;
  }

  rc = sdb_conn->get_cl(ha_thread.sdb_group_name, HA_INSTANCE_STATE_CL,
                        inst_state_cl);
  if (rc) {
    goto sdb_error;
  }

  build_session_attributes(thd, session_attrs);

  // the following commands change the original SQL statement, set
  // 'ClientCharset' field, replay thread change SQL character code
  // after found that this field value is greater than 0
  if (SQLCOM_CREATE_DB == sql_command || SQLCOM_DROP_DB == sql_command ||
      SQLCOM_DROP_TABLE == sql_command || SQLCOM_DROP_VIEW == sql_command ||
      SQLCOM_RENAME_TABLE == sql_command) {
    client_charset_num = thd->variables.character_set_client->number;
  }

  for (; ha_tables; ha_tables = ha_tables->next) {
    const char *db_name = ha_tables->db_name;
    const char *table_name = ha_tables->table_name;
    const char *op_type = ha_tables->op_type;
    query.length(0);

    // dropping function(with 'if exists') without setting database report no
    // errors, so the database is unknown, skip this situation
    if (!db_name || !table_name || 0 == strlen(db_name)) {
      continue;
    }

    if (SQLCOM_DROP_DB == sql_command) {
      if (0 != strcmp(op_type, HA_OPERATION_TYPE_DB)) {
        // if it's objects in current database
        continue;
      }
      oom = query.append("DROP DATABASE IF EXISTS ");
      ha_quote_name(db_name, quoted_name_buf);
      oom |= query.append(quoted_name_buf);
    } else if (SQLCOM_CREATE_DB == sql_command) {
      oom = query.append("CREATE DATABASE IF NOT EXISTS ");
      ha_quote_name(db_name, quoted_name_buf);
      oom |= query.append(quoted_name_buf);
    } else {
      // skip temporary tables for drop table sql
      if (SQLCOM_DROP_TABLE == sql_command) {
        // skip temporary table
        if (ha_tables->is_temporary_table) {
          continue;
        }
      }

      // decompose 'SQLCOM_DROP_TABLE' command into multiple 'DROP TABLE'
      // commands
      if (SQLCOM_DROP_TABLE == sql_command || SQLCOM_DROP_VIEW == sql_command) {
        // check if the dropping object exists
        if (!dropping_table_exists(thd, db_name, table_name)) {
          continue;
        }

        if (SQLCOM_DROP_TABLE == sql_command) {
          oom = query.append("DROP TABLE IF EXISTS ");
        } else {
          oom |= query.append("DROP VIEW IF EXISTS ");
        }
        oom |= build_full_table_name(query, db_name, table_name);
      } else if (SQLCOM_RENAME_TABLE == sql_command) {
        // move to the next table
        ha_tables = ha_tables->next;

        // skip rename temporary table, just for mariadb, can't rename temporary
        // table in mysql
#ifdef IS_MARIADB
        if (is_temporary_table(thd, db_name, table_name)) {
          continue;
        }
#endif
        const char *new_db_name = ha_tables->db_name;
        const char *new_tbl_name = ha_tables->table_name;
        oom = query.append("RENAME TABLE ");
        oom |= build_full_table_name(query, db_name, table_name);
        oom |= query.append(" TO ");
        oom |= build_full_table_name(query, new_db_name, new_tbl_name);
      } else if (have_exist_warning(thd)) {
        // if thd have 'not exist' or 'already exist' warning
        continue;
      } else {
        oom = query.append(general_query);
      }
    }
    if (oom) {
      rc = HA_ERR_OUT_OF_MEM;
      goto error;
    }

    // write sql info into 'HASQLLog' table
    obj = BSON(HA_FIELD_DB << db_name << HA_FIELD_TABLE << table_name
                           << HA_FIELD_TYPE << op_type << HA_FIELD_SQL
                           << query.c_ptr_safe() << HA_FIELD_OWNER
                           << ha_thread.instance_id << HA_FIELD_SESSION_ATTRS
                           << session_attrs << HA_FIELD_CLIENT_CHARSET_NUM
                           << client_charset_num);
    rc = sql_log_cl.insert(obj, hint, 0, &result);
    if (rc) {
      goto sdb_error;
    }

    // write 'HASQLLogState'
    sql_id = result.getIntField(SDB_FIELD_LAST_GEN_ID);
    DBUG_ASSERT(sql_id > 0);

    cond = BSON(HA_FIELD_DB << db_name << HA_FIELD_TABLE << table_name
                            << HA_FIELD_TYPE << op_type);
    obj = BSON("$set" << BSON(HA_FIELD_SQL_ID << sql_id));
    rc = sql_log_state_cl.upsert(obj, cond);
    if (rc) {
      goto sdb_error;
    }

    // write 'HAInstanceState'
    cond = BSON(HA_FIELD_INSTANCE_ID << ha_thread.instance_id << HA_FIELD_DB
                                     << db_name << HA_FIELD_TABLE << table_name
                                     << HA_FIELD_TYPE << op_type);
    obj = BSON("$set" << BSON(HA_FIELD_SQL_ID << sql_id));
    rc = inst_state_cl.upsert(obj, cond);
    if (rc) {
      goto sdb_error;
    }

    // update cached instance state
    snprintf(cached_record_key, NAME_LEN * 2 + 20, "%s-%s-%s", db_name,
             table_name, op_type);
    rc = ha_update_cached_record(ha_thread.inst_state_cache,
                                 HA_KEY_MEM_INST_STATE_CACHE, cached_record_key,
                                 sql_id);
    if (rc) {
      goto error;
    }

    if (SQLCOM_CREATE_TABLE == sql_command ||
        SQLCOM_CREATE_VIEW == sql_command) {
      // sql like 'create table/view' may have multi tables in its table_list
      break;
    }
  }
done:
  return rc;
sdb_error:
  // get sequoiadb error string
  ha_error_string(*sdb_conn, rc, sql_info->err_message);
error:
  goto done;
}

// set ha_table_list.is_temporary_table if current
// table is a temporary table
void mark_temporary_tables(THD *thd, ha_table_list *tables) {
  DBUG_ENTER("mark_temporary_tables");
  const char *db_name = NULL, *table_name = NULL;
  bool is_temp_table = false;
  for (ha_table_list *table_list = tables; table_list;
       table_list = table_list->next) {
    db_name = table_list->db_name;
    table_name = table_list->table_name;
    is_temp_table = is_temporary_table(thd, db_name, table_name);
    // mark temporary table by setting 'ha_table_list::is_temporary_table'
    if (is_temp_table) {
      DBUG_PRINT("info", ("found temporary table %s:%s", db_name, table_name));
      table_list->is_temporary_table = true;
    }
  }
  DBUG_VOID_RETURN;
}

// get objects involved in current SQL statement
static int get_sql_objects(THD *thd, ha_sql_stmt_info *sql_info) {
  DBUG_ENTER("get_sql_objects");
  int rc = 0;
  int sql_command = thd_sql_command(thd);

  DBUG_ASSERT(NULL == sql_info->tables);
  if (SQLCOM_DROP_DB == sql_command) {
    // 'DROP database ' get records from 'HASQLLogState' after add 'X' lock
    // on database
  } else if (SQLCOM_CREATE_DB == sql_command ||
             SQLCOM_ALTER_DB == sql_command) {
    sql_info->tables = (ha_table_list *)thd_calloc(thd, sizeof(ha_table_list));
    if (NULL == sql_info->tables) {
      rc = HA_ERR_OUT_OF_MEM;
      goto error;
    }
    sql_info->tables->db_name = thd->lex->name.str;
    sql_info->tables->table_name = HA_EMPTY_STRING;
    sql_info->tables->op_type = HA_OPERATION_TYPE_DB;
    sql_info->tables->is_temporary_table = false;
    sql_info->tables->next = NULL;
  } else if (is_routine_meta_sql(thd)) {
    // routine include procedure, function, trigger and event
    memset(sql_info->sp_db_name, 0, NAME_LEN + 1);
    memset(sql_info->sp_name, 0, NAME_LEN + 1);
    // set sp database and sp_name
    if (thd->lex->sphead) {
      if (thd->lex->sphead->m_db.str) {
        sprintf(sql_info->sp_db_name, "%s", thd->lex->sphead->m_db.str);
      }
      sprintf(sql_info->sp_name, "%s", thd->lex->sphead->m_name.str);
    } else if (thd->lex->spname) {
      if (thd->lex->spname->m_db.str) {
        sprintf(sql_info->sp_db_name, "%s", thd->lex->spname->m_db.str);
      }
      sprintf(sql_info->sp_name, "%s", thd->lex->spname->m_name.str);
    }
    DBUG_ASSERT(strlen(sql_info->sp_name) != 0);
    DBUG_ASSERT(strlen(sql_info->sp_db_name) != 0);

    sql_info->tables = (ha_table_list *)thd_calloc(thd, sizeof(ha_table_list));
    if (NULL == sql_info->tables) {
      rc = HA_ERR_OUT_OF_MEM;
      goto error;
    }

    sql_info->tables->db_name = sql_info->sp_db_name;
    sql_info->tables->table_name = sql_info->sp_name;
    switch (sql_command) {
      case SQLCOM_CREATE_PROCEDURE:
      case SQLCOM_ALTER_PROCEDURE:
      case SQLCOM_DROP_PROCEDURE:
        sql_info->tables->op_type = HA_ROUTINE_TYPE_PROC;
        break;
      case SQLCOM_CREATE_FUNCTION:
      case SQLCOM_DROP_FUNCTION:
      case SQLCOM_CREATE_SPFUNCTION:
      case SQLCOM_ALTER_FUNCTION:
        sql_info->tables->op_type = HA_ROUTINE_TYPE_FUNC;
        break;
      case SQLCOM_CREATE_TRIGGER:
      case SQLCOM_DROP_TRIGGER:
        sql_info->tables->op_type = HA_ROUTINE_TYPE_TRIG;
      case SQLCOM_CREATE_EVENT:
      case SQLCOM_ALTER_EVENT:
      case SQLCOM_DROP_EVENT:
        sql_info->tables->op_type = HA_ROUTINE_TYPE_EVENT;
        break;
      default:
        DBUG_ASSERT(0);
        break;
    }
    sql_info->tables->is_temporary_table = false;
    sql_info->tables->next = NULL;
  } else if (is_dcl_meta_sql(thd)) {
    const char *db_name = sdb_thd_db(thd);
    sql_info->tables = (ha_table_list *)thd_calloc(thd, sizeof(ha_table_list));
    if (NULL == sql_info->tables) {
      rc = HA_ERR_OUT_OF_MEM;
      goto error;
    }
    sql_info->tables->db_name = db_name ? db_name : HA_MYSQL_DB;
    sql_info->tables->table_name = HA_EMPTY_STRING;
    sql_info->tables->op_type = HA_OPERATION_TYPE_DCL;
    sql_info->tables->is_temporary_table = false;
    sql_info->tables->next = NULL;
  } else {
    TABLE_LIST *tables = sdb_lex_first_select(thd)->get_table_list();
    ha_table_list *ha_tbl_list = NULL, *ha_tbl_list_tail = NULL;
    for (TABLE_LIST *tbl = tables; tbl; tbl = tbl->next_local) {
      ha_tbl_list = (ha_table_list *)thd_calloc(thd, sizeof(ha_table_list));
      if (ha_tbl_list == NULL) {
        rc = HA_ERR_OUT_OF_MEM;
        goto error;
      }
      ha_tbl_list->db_name = C_STR(tbl->db);
      ha_tbl_list->table_name = C_STR(tbl->table_name);
      if (!ha_tbl_list->db_name || !ha_tbl_list->table_name) {
        continue;
      }

      ha_tbl_list->op_type = HA_OPERATION_TYPE_TABLE;
      ha_tbl_list->is_temporary_table = false;
      ha_tbl_list->next = NULL;
      if (!sql_info->tables) {
        sql_info->tables = ha_tbl_list;
        ha_tbl_list_tail = ha_tbl_list;
      } else {
        ha_tbl_list_tail->next = ha_tbl_list;
        ha_tbl_list_tail = ha_tbl_list;
      }
    }
    // mark temporary table for SQL like 'rename/alter/drop/truncate table'
    if (SQLCOM_DROP_TABLE == sql_command || SQLCOM_ALTER_TABLE == sql_command ||
        SQLCOM_RENAME_TABLE == sql_command || SQLCOM_TRUNCATE == sql_command) {
      mark_temporary_tables(thd, sql_info->tables);
    }
    DBUG_ASSERT(sql_info->tables != NULL);
  }
done:
  DBUG_RETURN(rc);
error:
  goto done;
}

// set mariadb THD::killed state to KILL_QUERY
static void set_abort_query_flag(THD *thd) {
#ifdef IS_MARIADB
  thd->set_killed(KILL_QUERY);
#else
  // thd->killed = THD::KILL_QUERY;
#endif
}

static void init_ha_event_general(ha_event_general &ha_event, const void *ev,
                                  ha_event_class_t event_class) {
  ha_event.general_error_code = -1;
  if (MYSQL_AUDIT_CONNECTION_CLASS == event_class) {
    const struct mysql_event_connection *event =
        (const struct mysql_event_connection *)ev;
    ha_event.event_subclass = (unsigned int)event->event_subclass;
    ha_event.general_error_code = event->status;
  } else if (MYSQL_AUDIT_GENERAL_CLASS == event_class) {
    const struct mysql_event_general *event =
        (const struct mysql_event_general *)ev;
    ha_event.event_subclass = (unsigned int)event->event_subclass;
    ha_event.general_error_code = event->general_error_code;
    ha_event.general_thread_id = event->general_thread_id;
#ifdef IS_MYSQL
    ha_event.general_query = event->general_query.str;
    ha_event.general_query_length = event->general_query.length;
#else
    ha_event.general_query = event->general_query;
    ha_event.general_query_length = event->general_query_length;
#endif
  }
#ifdef IS_MYSQL
  if (MYSQL_AUDIT_QUERY_CLASS == event_class) {
    const struct mysql_event_query *event =
        (const struct mysql_event_query *)ev;
    ha_event.event_subclass = (unsigned int)event->event_subclass;
    ha_event.general_error_code = event->status;
    ha_event.general_query = event->query.str;
    ha_event.general_query_length = event->query.length;
  }
#endif
}

static inline bool need_prepare(const ha_event_general &event, bool is_trans_on,
                                int sql_command, ha_event_class_t event_class) {
  bool conds = !is_trans_on;
#ifdef IS_MARIADB
  if (MYSQL_AUDIT_GENERAL_CLASS == event_class) {
    conds = conds && (HA_MYSQL_AUDIT_SDB_DDL == event.event_subclass);
  } else if (MYSQL_AUDIT_TABLE_CLASS == event_class) {
    // set conds to false if event class is MYSQL_AUDIT_TABLE_CLASS
    conds = false;
  }
#else
  if (MYSQL_AUDIT_GENERAL_CLASS == event_class) {
    conds = conds && (MYSQL_AUDIT_GENERAL_STATUS != event.event_subclass);
    conds = conds && (MYSQL_AUDIT_GENERAL_ERROR != event.event_subclass);
    conds = conds && (MYSQL_AUDIT_GENERAL_RESULT != event.event_subclass);
  } else if (MYSQL_AUDIT_QUERY_CLASS == event_class) {
    conds = conds && (MYSQL_AUDIT_QUERY_START == event.event_subclass ||
                      MYSQL_AUDIT_QUERY_NESTED_START == event.event_subclass);
  }
#endif
  return conds && (0 == event.general_error_code);
}

static inline bool need_complete(const ha_event_general &event,
                                 bool is_trans_on, int sql_command,
                                 ha_event_class_t event_class) {
  bool conds = is_trans_on;
#ifdef IS_MARIADB
  conds = conds && (MYSQL_AUDIT_GENERAL_STATUS == event.event_subclass ||
                    MYSQL_AUDIT_GENERAL_RESULT == event.event_subclass);
  // for mariadb the final event class must be MYSQL_AUDIT_GENERAL_CLASS
  conds = conds && (MYSQL_AUDIT_GENERAL_CLASS == event_class);
#else
  bool result = MYSQL_AUDIT_GENERAL_CLASS == event_class &&
                (MYSQL_AUDIT_GENERAL_STATUS == event.event_subclass ||
                 MYSQL_AUDIT_GENERAL_RESULT == event.event_subclass);
  result =
      result || (MYSQL_AUDIT_QUERY_CLASS == event_class &&
                 (MYSQL_AUDIT_QUERY_STATUS_END == event.event_subclass ||
                  MYSQL_AUDIT_QUERY_NESTED_STATUS_END == event.event_subclass));
  conds = conds && result;
#endif
  return conds;
}

// in mysql, event is executed by create an procedure in mysql
static inline bool is_event_dispatch_execution(THD *thd) {
  bool event_dispatch = false;
#ifdef IS_MYSQL
  event_dispatch = (thd->lex->sphead) &&
                   (SP_TYPE_EVENT == thd->lex->sphead->m_type) &&
                   (SQLCOM_CREATE_PROCEDURE == thd->lex->sql_command);
#endif
  return event_dispatch;
}

// convert sql like 'create table xxx as select xxx/like xxx' to
// 'create table xxx(...)', no effect on other SQL statements
int fix_create_table_sql(THD *thd, ha_event_class_t event_class,
                         ha_event_general &event, String &query) {
  TABLE_LIST *table_list = sdb_lex_first_select(thd)->get_table_list();
  int sql_command = thd_sql_command(thd);
  int rc = 0;
  query.set_charset(thd->variables.character_set_client);
  query.length(0);
  DBUG_ENTER("fix_create_table_sql");

#ifdef IS_MYSQL
  if (SQLCOM_CREATE_TABLE == sql_command && table_list &&
      MYSQL_AUDIT_QUERY_CLASS == event_class &&
      (MYSQL_AUDIT_QUERY_STATUS_END == event.event_subclass ||
       MYSQL_AUDIT_QUERY_NESTED_STATUS_END == event.event_subclass)) {
    if (table_list->table) {
      // deal with SQL like 'create table as select xxx...'
      rc = store_create_info(thd, table_list, &query, &thd->lex->create_info,
                             TRUE);
      event.general_query = query.ptr();
      event.general_query_length = query.length();
    } else if (table_list->next_local) {
      // deal with SQL like 'create table like xxx'
      TABLE_LIST tables;
      tables.init_one_table(
          table_list->db, table_list->db_length, table_list->table_name,
          table_list->table_name_length, C_STR(table_list->alias),
          TL_READ_WITH_SHARED_LOCKS);
      rc = open_and_lock_tables(thd, &tables, MYSQL_LOCK_IGNORE_TIMEOUT);
      if (rc) {
        ha_sql_stmt_info *sql_info = get_sql_stmt_info();
        snprintf(sql_info->err_message, HA_BUF_LEN,
                 "Can't open and lock table '%s:%s'", table_list->db,
                 table_list->table_name);
        goto error;
      }
      table_list->table = tables.table;
      rc = store_create_info(thd, table_list, &query, &thd->lex->create_info,
                             TRUE);
      event.general_query = query.ptr();
      event.general_query_length = query.length();
    }
    DBUG_ASSERT(rc == 0);
  }
#else
  if (SQLCOM_CREATE_TABLE == sql_command && table_list &&
      (MYSQL_AUDIT_GENERAL_RESULT == event.event_subclass ||
       MYSQL_AUDIT_GENERAL_STATUS == event.event_subclass)) {
    if (!table_list->table && table_list->next_local) {
      // deal with SQL like "create table t1 like t2" or "create or replace
      // table like".
      TABLE_LIST tables;
      tables.init_one_table(&table_list->db, &table_list->table_name, 0,
                            TL_READ_WITH_SHARED_LOCKS);
      rc = open_and_lock_tables(thd, &tables, FALSE, MYSQL_LOCK_IGNORE_TIMEOUT);
      if (rc) {
        ha_sql_stmt_info *sql_info = get_sql_stmt_info();
        snprintf(sql_info->err_message, HA_BUF_LEN,
                 "Can't open and lock table '%s:%s'", table_list->db.str,
                 table_list->table_name.str);
        goto error;
      }
      table_list->table = tables.table;
      rc = show_create_table(thd, table_list, &query, &thd->lex->create_info,
                             WITH_DB_NAME);
      event.general_query = query.ptr();
      event.general_query_length = query.length();
    } else if (thd->open_tables) {
      TABLE_LIST tables;
      tables.reset();
      tables.table = thd->open_tables;
      // deal with SQL like "create table as select " or "create or replace
      // table as select"
      rc = show_create_table(thd, &tables, &query, &thd->lex->create_info,
                             WITH_DB_NAME);
      event.general_query = query.ptr();
      event.general_query_length = query.length();
    }
    DBUG_ASSERT(rc == 0);
  }
#endif
done:
  DBUG_RETURN(rc);
error:
  rc = HA_ERR_FIX_CREATE_TABLE;
  goto done;
}

// check if current SQL statement can be written into sequoiadb
bool can_write_sql_log(THD *thd, ha_sql_stmt_info *sql_info, int error_code) {
  int sql_command = thd_sql_command(thd);
  bool can_write_log = true;

  if (SQLCOM_ALTER_TABLE == sql_command || SQLCOM_TRUNCATE == sql_command) {
    if (sql_info->tables->is_temporary_table) {
      can_write_log = false;
    }
  }
  if (can_write_log &&
      (ER_BAD_TABLE_ERROR == error_code || ER_WRONG_OBJECT == error_code
#ifdef IS_MARIADB
       || ER_UNKNOWN_VIEW == error_code
#endif
       ) &&
      (SQLCOM_DROP_TABLE == sql_command || SQLCOM_DROP_VIEW == sql_command)) {
    // if drop tables/views partial success
  } else if (can_write_log && 0 == error_code &&
             SQLCOM_CREATE_TABLE == sql_command) {
    // handle 'create table if not exists', if get warning message 'table
    // exists' in thd, ignore this sql
    can_write_log = !sdb_has_sql_condition(thd, ER_TABLE_EXISTS_ERROR);
  } else if (error_code) {
    can_write_log = false;
  }
  return can_write_log;
}

// set THD::Diagnostics_area::m_can_overwrite_status for mariadb, if this flag
// is false, my_printf_error can't be used for printing errors
inline static void set_overwrite_status(THD *thd, ha_event_general &event,
                                        bool status) {
#ifdef IS_MARIADB
  if (MYSQL_AUDIT_GENERAL_RESULT == event.event_subclass ||
      MYSQL_AUDIT_GENERAL_STATUS == event.event_subclass) {
    thd->get_stmt_da()->set_overwrite_status(status);
  }
#endif
}

static void handle_error(int error, ha_sql_stmt_info *sql_info) {
  switch (error) {
    case HA_ERR_OUT_OF_MEM:
      my_printf_error(HA_ERR_EXEC_SQL,
                      "HA: Out of memory while persisting SQL log", MYF(0));
      sql_print_error("HA: Out of memory while persisting SQL log");
      break;
    case HA_ERR_ABORT_BY_USER:
      my_printf_error(HA_ERR_EXEC_SQL, "HA: Query has been aborted by user",
                      MYF(0));
      break;
    case HA_ERR_WAIT_TIMEOUT:
      my_printf_error(HA_ERR_EXEC_SQL,
                      "HA: There are SQL statements on releated objects have "
                      "not been executed by replay thread, please try it later",
                      MYF(0));
      break;
    case HA_ERR_FIX_CREATE_TABLE:
      my_printf_error(HA_ERR_EXEC_SQL, "HA: %s, please check error log", MYF(0),
                      sql_info->err_message);
      break;
    case HA_ERR_EXCEPTION:
    default:
      my_printf_error(HA_ERR_EXEC_SQL, "HA: %s", MYF(0), sql_info->err_message);
      break;
  }
}

// entry of audit plugin
static int persist_sql_stmt(THD *thd, ha_event_class_t event_class,
                            const void *ev) {
  DBUG_ENTER("persist_sql_stmt");
  DBUG_PRINT("info", ("event: %d, subevent: %d", (int)event_class, *(int *)ev));
  int rc = 0, sql_command = -1;
  bool is_trans_on = false, need_kill_mysqld = false;
  String create_query;
  create_query.set_charset(system_charset_info);
  create_query.length(0);
  ha_sql_stmt_info *sql_info = NULL;
  ha_event_general event;

  // do nothing for following situation
  // 1. HA function is not turned on;
  // 2. initialization of SQL instance;
  // 3. mysql service is starting;
  // 4. SQL statement has 'temporary' flag, eg: 'create temporary table'
  if (!thd || !ha_thread.is_open || opt_bootstrap || !mysqld_server_started ||
      has_temporary_table_flag(thd)) {
    goto done;
  }
  sql_command = thd_sql_command(thd);
  // convert mysql or mariadb event to HA event
  init_ha_event_general(event, ev, event_class);

  if (NULL == (sql_info = get_sql_stmt_info())) {
    rc = HA_ERR_OUT_OF_MEM;
    goto error;
  }

  // destroy sequoiadb connection at end of client session
  if (sql_info->inited && MYSQL_AUDIT_CONNECTION_CLASS == event_class &&
      MYSQL_AUDIT_CONNECTION_DISCONNECT == event.event_subclass) {
    if (sql_info->sdb_conn) {
      delete sql_info->sdb_conn;
      sql_info->sdb_conn = NULL;
      SDB_LOG_DEBUG("HA: Destroy sequoiadb connection");
    }
    goto done;
  }

  // 1. do nothing if current SQL statement is not metadata operation.
  // 2. In mysql, event is executed by creating a procedure, execution of
  //    event can't be written into sequoiadb
  if (!is_meta_sql(thd, event) || is_event_dispatch_execution(thd)) {
    // TODO: handle DML query
    goto done;
  } else if (!ha_thread.recover_finished) {  // current instance is recovering
    struct timespec abstime;
    static const int WAIT_TIMEOUT = 30;
    sdb_set_timespec(abstime, WAIT_TIMEOUT);
    // can't prevent statements to be executed by HA thread
    if (sdb_execute_only_in_mysql(thd)) {
      // if its statement executed by source command
      goto done;
    } else {
      SDB_LOG_INFO("HA: Waiting for completion of recover process");
      // other clients must wait for completion of recover process. It's not
      // convient for automated testing if report error here.
      mysql_mutex_lock(&ha_thread.recover_finished_mutex);
      rc = mysql_cond_timedwait(&ha_thread.recover_finished_cond,
                                &ha_thread.recover_finished_mutex, &abstime);
      mysql_mutex_unlock(&ha_thread.recover_finished_mutex);
      if (rc) {
        rc = HA_ERR_EXCEPTION;
        sql_print_error(
            "HA: Instance is in recovering state. Current query "
            "can't be executed, please try it later");
        snprintf(sql_info->err_message, HA_BUF_LEN,
                 "Instance is in recovering state. Current query can't "
                 "be executed, please try it later");
        goto error;
      }
      SDB_LOG_DEBUG("HA: Metadata recover finished, start execute: %s",
                    sdb_thd_query(thd));
      if (thd_killed(thd) || ha_thread.stopped) {
        rc = HA_ERR_ABORT_BY_USER;
        goto error;
      }
    }
  } else if (sdb_execute_only_in_mysql(thd)) {
    // replay SQL generated by other instances
    goto done;
  }

  // in mariadb, THD::Diagnostics_area::m_can_overwrite_status must
  // set to true for using 'my_printf_error' when event_class is
  // MYSQL_AUDIT_GENERAL_STATUS or  MYSQL_AUDIT_GENERAL_RESULT
  set_overwrite_status(thd, event, true);

  // update SQL statement info according to ev
  update_sql_stmt_info(sql_info, event_class, ev, thd);
  if (NULL == sql_info->sdb_conn) {
    rc = HA_ERR_OUT_OF_MEM;
    goto error;
  }

  is_trans_on = sql_info->sdb_conn->is_transaction_on();

  if (need_prepare(event, is_trans_on, sql_command, event_class)) {
    DBUG_ASSERT(sql_info->tables == NULL);
    try {
      SDB_LOG_DEBUG("HA: At the beginning of persisting SQL: %s, thread: %p",
                    sdb_thd_query(thd), thd);
      SDB_LOG_DEBUG("HA: SQL command: %d, event: %d, subevent: %d, thread: %p",
                    sql_command, event_class, *((int *)ev), thd);
      rc = begin_transaction(thd, sql_info);
      if (rc) {
        goto error;
      }

      rc = get_sql_objects(thd, sql_info);
      if (rc) {
        goto error;
      }

      rc = lock_objects(thd, sql_info);
      if (rc) {
        goto error;
      }

      rc = wait_objects_updated_to_lastest(thd, sql_info);
      if (rc) {
        goto error;
      }
    } catch (std::bad_alloc &e) {
      rc = HA_ERR_OUT_OF_MEM;
      goto error;
    } catch (std::exception &e) {
      rc = HA_ERR_EXCEPTION;
      snprintf(sql_info->err_message, HA_BUF_LEN, "HA: Unexpected error: %s",
               e.what());
      goto error;
    }
  }

  if (need_complete(event, is_trans_on, sql_command, event_class)) {
    SDB_LOG_DEBUG("HA: At the end of persisting SQL: %s, thread: %p",
                  sdb_thd_query(thd), thd);
    try {
      if (can_write_sql_log(thd, sql_info, event.general_error_code)) {
        // rebuild create table SQL statement if necessary
        rc = fix_create_table_sql(thd, event_class, event, create_query);
        rc = rc ? rc : write_sql_log_and_states(thd, sql_info, event);

        // if SQL statement executes successfully but write SQL log into
        // sequoiadb failed, kill current instances process
        sql_info->tables = NULL;
        if (rc) {
          need_kill_mysqld = true;
          goto error;
        }

        rc = sql_info->sdb_conn->commit_transaction();
        if (rc) {
          need_kill_mysqld = true;
          ha_error_string(*sql_info->sdb_conn, rc, sql_info->err_message);
          goto error;
        }
      } else {
        // Note: 'ha_sql_stmt_info::tables' can only be set to NULL in the
        // final step(see the entry conditions).
        sql_info->tables = NULL;
        rc = sql_info->sdb_conn->rollback_transaction();
        if (rc) {
          ha_error_string(*sql_info->sdb_conn, rc, sql_info->err_message);
          goto error;
        }
      }
    } catch (std::bad_alloc &e) {
      rc = HA_ERR_OUT_OF_MEM;
      need_kill_mysqld = true;
      goto error;
    } catch (std::exception &e) {
      rc = HA_ERR_EXCEPTION;
      need_kill_mysqld = true;
      snprintf(sql_info->err_message, HA_BUF_LEN, "HA: Unexpected error: %s",
               e.what());
      goto error;
    }
  }
done:
  // restore THD::m_overwrite_status for mariadb
  set_overwrite_status(thd, event, false);
  DBUG_RETURN(rc);
error:
  set_abort_query_flag(thd);
  handle_error(rc, sql_info);
  if (need_kill_mysqld) {
    ha_kill_mysqld(thd);
  }
  goto done;
}

// entry of Mariadb audit plugin
static void persist_mariadb_stmt(THD *thd, ha_event_class_t event_class,
                                 const void *ev) {
  persist_sql_stmt(thd, event_class, ev);
}

// entry of MySQL audit plugin
static int persist_mysql_stmt(THD *thd, ha_event_class_t event_class,
                              const void *ev) {
  return persist_sql_stmt(thd, event_class, ev);
}

// HA plugin initialization entry
static int server_ha_init(void *p) {
  DBUG_ENTER("server_ha_init");

  // build instance group collection space name
  int len = strlen(HA_INST_GROUP_PREFIX) + strlen(ha_inst_group_name);
  if (len > HA_MAX_INST_GROUP_NAME_LEN - 1) {
    sql_print_error("HA: Instance group name '%s' is too long",
                    ha_inst_group_name);
    DBUG_RETURN(HA_ERR_HA_INIT);
  }
  snprintf(ha_thread.sdb_group_name, HA_MAX_INST_GROUP_NAME_LEN, "%s%s",
           HA_INST_GROUP_PREFIX, ha_inst_group_name);

  // copy instance group key into ha_thread.group_key
  // then set instance group key to '*'
  if (strlen(ha_inst_group_key) > HA_MAX_KEY_LEN) {
    sql_print_error("HA: Instance group key is too long");
    DBUG_RETURN(HA_ERR_HA_INIT);
  }
  snprintf(ha_thread.group_key, HA_MAX_KEY_LEN + 1, "%s", ha_inst_group_key);
  for (uint i = 0; i < strlen(ha_inst_group_key); i++) {
    ha_inst_group_key[i] = '*';
  }
  if (strlen(ha_inst_group_key) > HA_MAX_SHOW_LEN) {
    ha_inst_group_key[HA_MAX_SHOW_LEN] = '\0';
  }

  ha_thread.stopped = false;
  ha_thread.recover_finished = false;
  ha_thread.is_open = false;

  if (strlen(ha_inst_group_name) && !opt_bootstrap) {
    // create HA thread
    ha_thread.instance_id = HA_INVALID_INST_ID;
    ha_thread.group_name = ha_inst_group_name;

    sdb_mysql_cond_init(HA_KEY_COND_RECOVER_FINISHED,
                        &ha_thread.recover_finished_cond, NULL);
    sdb_mysql_cond_init(HA_KEY_COND_REPLAY_STOPPED,
                        &ha_thread.replay_stopped_cond, NULL);
    mysql_mutex_init(HA_KEY_MUTEX_RECOVER_FINISHED,
                     &ha_thread.recover_finished_mutex, MY_MUTEX_INIT_FAST);
    mysql_mutex_init(HA_KEY_MUTEX_REPLAY_STOPPED,
                     &ha_thread.replay_stopped_mutex, MY_MUTEX_INIT_FAST);
    my_thread_attr_init(&ha_thread.thread_attr);
    if (sdb_hash_init(&ha_thread.inst_state_cache, system_charset_info, 32, 0,
                      0, (my_hash_get_key)cached_record_get_key,
                      free_cached_record_elem, 0,
                      HA_KEY_MEM_INST_STATE_CACHE)) {
      sql_print_error(
          "HA: Out of memory while initializing instance state cache");
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    }
    if (mysql_thread_create(HA_KEY_HA_THREAD, &ha_thread.thread,
                            &ha_thread.thread_attr, ha_recover_and_replay,
                            (void *)(&ha_thread))) {
      sql_print_error("HA: Out of memory while creating 'HA' thread");
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    }
    ha_thread.is_open = true;
  }
  DBUG_RETURN(0);
}

// HA plugin destruction entry
static int server_ha_deinit(void *p __attribute__((unused))) {
  DBUG_ENTER("server_ha_deinit");

  // wake up replay thread, there is no effect if
  // replay thread already exit
  if (strlen(ha_inst_group_name) && !opt_bootstrap) {
    mysql_mutex_lock(&ha_thread.replay_stopped_mutex);
    mysql_cond_signal(&ha_thread.replay_stopped_cond);
    mysql_mutex_unlock(&ha_thread.replay_stopped_mutex);
  }

  // wait for replay thread to end
  while (ha_thread.is_open && !ha_thread.stopped) {
    my_sleep(20000);
  }

  // destroy mutexes and conds
  if (strlen(ha_inst_group_name) && !opt_bootstrap) {
    mysql_cond_destroy(&ha_thread.recover_finished_cond);
    mysql_mutex_destroy(&ha_thread.recover_finished_mutex);
    mysql_cond_destroy(&ha_thread.replay_stopped_cond);
    mysql_mutex_destroy(&ha_thread.replay_stopped_mutex);
    my_thread_attr_destroy(&ha_thread.thread_attr);
    my_hash_reset(&ha_thread.inst_state_cache);
    my_hash_free(&ha_thread.inst_state_cache);
  }

  DBUG_RETURN(0);
}

// declaration of HA plugin
#ifdef IS_MARIADB
static struct st_mysql_audit mariadb_descriptor = {
    MYSQL_AUDIT_INTERFACE_VERSION,
    NULL,
    persist_mariadb_stmt,
    {MYSQL_AUDIT_GENERAL_CLASSMASK}};

mysql_declare_plugin(server_ha){MYSQL_AUDIT_PLUGIN,
                                &mariadb_descriptor,
                                "server_ha",
                                "Mark (SequoiaDB Corporation)",
                                "MySQL&Mariadb ha module for sequoiadb",
                                PLUGIN_LICENSE_GPL,
                                server_ha_init,
                                server_ha_deinit,
                                PLUGIN_VERSION,
                                0,
                                ha_sys_vars,
                                NULL,
                                0} mysql_declare_plugin_end;

static struct st_mysql_audit maria_descriptor = {
    MYSQL_AUDIT_INTERFACE_VERSION,
    NULL,
    persist_mariadb_stmt,
    {MYSQL_AUDIT_GENERAL_CLASSMASK | MYSQL_AUDIT_TABLE_CLASSMASK |
     MYSQL_AUDIT_CONNECTION_CLASSMASK}};

maria_declare_plugin(server_ha){
    MYSQL_AUDIT_PLUGIN,
    &maria_descriptor,
    "server_ha",
    "Mark (SequoiaDB Corporation)",
    "MySQL&Mariadb ha module for sequoiadb",
    PLUGIN_LICENSE_GPL,
    server_ha_init,
    server_ha_deinit,
    PLUGIN_VERSION,
    0,
    ha_sys_vars,
    PLUGIN_STR_VERSION,
    MariaDB_PLUGIN_MATURITY_STABLE} maria_declare_plugin_end;
#else
static struct st_mysql_audit mysql_descriptor = {
    MYSQL_AUDIT_INTERFACE_VERSION,
    NULL,
    persist_mysql_stmt,
    {(unsigned long)MYSQL_AUDIT_GENERAL_ALL,
     (unsigned long)MYSQL_AUDIT_CONNECTION_ALL, 0,
     0, /* This event class is currently not supported. */
     0, /* This event class is currently not supported. */
     (unsigned long)MYSQL_AUDIT_GLOBAL_VARIABLE_ALL,
     (unsigned long)MYSQL_AUDIT_SERVER_STARTUP_ALL,
     (unsigned long)MYSQL_AUDIT_SERVER_SHUTDOWN_ALL, 0,
     (unsigned long)MYSQL_AUDIT_QUERY_ALL,
     (unsigned long)MYSQL_AUDIT_STORED_PROGRAM_ALL}};

mysql_declare_plugin(server_ha){MYSQL_AUDIT_PLUGIN,
                                &mysql_descriptor,
                                "server_ha",
                                "Mark (SequoiaDB Corporation)",
                                "MySQL&Mariadb ha module for sequoiadb server",
                                PLUGIN_LICENSE_GPL,
                                server_ha_init,
                                server_ha_deinit,
                                PLUGIN_VERSION,
                                0,
                                ha_sys_vars,
                                NULL,
                                0} mysql_declare_plugin_end;
#endif /* IS_MARIADB */

void __attribute__((constructor)) server_ha_plugin_init(void) {
  return;
}
