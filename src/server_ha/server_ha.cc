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
#include "event_parse_data.h"
#include <exception>
#include "events.h"
#include "tztime.h"
#include "sql_time.h"

// thread local key for ha_sql_stmt_info
thread_local_key_t ha_sql_stmt_info_key;
static my_thread_once_t ha_sql_stmt_info_key_once = MY_THREAD_ONCE_INIT;

static ha_recover_replay_thread ha_thread;

// original instance group name and key, instance group key will be modified
// in 'server_ha_init', so it's invisible to mysql user
char *ha_inst_group_name = NULL;
static char *ha_inst_group_key = NULL;
static uint ha_wait_replay_timeout = HA_WAIT_REPLAY_TIMEOUT_DEFAULT;
static uint ha_wait_recover_timeout = HA_WAIT_REPLAY_TIMEOUT_DEFAULT;

static MYSQL_SYSVAR_STR(inst_group_name, ha_inst_group_name,
                        PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_MEMALLOC |
                            PLUGIN_VAR_READONLY,
                        "SQL instance group name. (Default: \"\")"
                        /*实例组名。*/,
                        NULL, NULL, "");

static MYSQL_SYSVAR_STR(inst_group_key, ha_inst_group_key,
                        PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_MEMALLOC |
                            PLUGIN_VAR_READONLY,
                        "Instance group key used to decrypt instance "
                        "group password. (Default: \"\")"
                        /*实例组秘钥, 用于解密实例组用户密码。*/,
                        NULL, NULL, "");

static MYSQL_SYSVAR_UINT(wait_replay_timeout, ha_wait_replay_timeout,
                         PLUGIN_VAR_OPCMDARG,
                         "Timeout waiting for playback thread. (Default: 30)"
                         /*等待回放线程超时时间[0, 3600]，单位：sec。*/,
                         NULL, NULL, HA_WAIT_REPLAY_TIMEOUT_DEFAULT, 0, 3600,
                         0);

static MYSQL_SYSVAR_UINT(wait_recover_timeout, ha_wait_recover_timeout,
                         PLUGIN_VAR_OPCMDARG,
                         "Timeout waiting for instance recovery. (Default: 30)"
                         /*等待实例启动恢复超时时间[0, 3600]，单位：sec。*/,
                         NULL, NULL, HA_WAIT_REPLAY_TIMEOUT_DEFAULT, 0, 3600,
                         0);

struct st_mysql_sys_var *ha_sys_vars[] = {
    MYSQL_SYSVAR(inst_group_name), MYSQL_SYSVAR(inst_group_key),
    MYSQL_SYSVAR(wait_replay_timeout), MYSQL_SYSVAR(wait_recover_timeout),
    NULL};

bool ha_is_open() {
  return (ha_inst_group_name && 0 != strlen(ha_inst_group_name));
}

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

static int get_sql_stmt_info(ha_sql_stmt_info **sql_info) {
  int rc = 0;
  my_thread_once(&ha_sql_stmt_info_key_once, init_sql_stmt_info_key);
  *sql_info = (ha_sql_stmt_info *)my_get_thread_local(ha_sql_stmt_info_key);
  if (NULL == *sql_info) {
    // can't use sdb_my_malloc, because mariadb will check memory
    // it use after shutdown
    *sql_info = (ha_sql_stmt_info *)malloc(sizeof(ha_sql_stmt_info));
    if (*sql_info) {
      memset(*sql_info, 0, sizeof(ha_sql_stmt_info));

      (*sql_info)->inited = false;
      (*sql_info)->is_result_set_started = false;
      (*sql_info)->last_instr_lex = NULL;

      if (sdb_hash_init(&((*sql_info)->dml_checked_objects),
                        system_charset_info, 32, 0, 0,
                        (my_hash_get_key)cached_record_get_key,
                        free_cached_record_elem, 0, PSI_INSTRUMENT_ME)) {
        return SDB_HA_OOM;
      }
      my_set_thread_local(ha_sql_stmt_info_key, *sql_info);
    } else {
      rc = SDB_HA_OOM;
    }
  }
  return rc;
}

static int update_sql_stmt_info(ha_sql_stmt_info *sql_info, ulong thread_id) {
  int rc = 0;
  if (!sql_info->inited) {
    sql_info->inited = true;
    sql_info->tables = NULL;
    sql_info->dml_retry_flag = false;
    sql_info->is_result_set_started = false;
    sql_info->last_instr_lex = NULL;
    if (sdb_hash_init(&(sql_info->dml_checked_objects), system_charset_info, 32,
                      0, 0, (my_hash_get_key)cached_record_get_key,
                      free_cached_record_elem, 0, PSI_INSTRUMENT_ME)) {
      return SDB_HA_OOM;
    }
    sql_info->sdb_conn = new (std::nothrow) Sdb_conn(thread_id, true);
    if (likely(sql_info->sdb_conn)) {
      SDB_LOG_DEBUG("HA: Init sequoiadb connection");
    } else {
      rc = SDB_HA_OOM;
    }
  } else if (NULL == sql_info->sdb_conn) {
    // if client execute some DDL statements, exit session,
    // ha_sql_stmt_info::sdb_conn will be set to NULL.
    sql_info->tables = NULL;
    sql_info->dml_retry_flag = false;
    sql_info->is_result_set_started = false;
    sql_info->last_instr_lex = NULL;
    if (sdb_hash_init(&(sql_info->dml_checked_objects), system_charset_info, 32,
                      0, 0, (my_hash_get_key)cached_record_get_key,
                      free_cached_record_elem, 0, PSI_INSTRUMENT_ME)) {
      return SDB_HA_OOM;
    }
    sql_info->sdb_conn = new (std::nothrow) Sdb_conn(thread_id, true);
    if (likely(sql_info->sdb_conn)) {
      SDB_LOG_DEBUG("HA: Init sequoiadb connection");
    } else {
      rc = SDB_HA_OOM;
    }
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

static inline bool is_package_meta_sql(THD *thd) {
  bool is_package_op = false;
#ifdef IS_MARIADB
  int sql_command = thd_sql_command(thd);
  switch (sql_command) {
    case SQLCOM_CREATE_PACKAGE:
    case SQLCOM_CREATE_PACKAGE_BODY:
    case SQLCOM_DROP_PACKAGE:
    case SQLCOM_DROP_PACKAGE_BODY:
      is_package_op = true;
      break;
  }
#endif
  return is_package_op;
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
    case SQLCOM_ALTER_TABLESPACE:
    case SQLCOM_CREATE_SERVER:
    case SQLCOM_DROP_SERVER:
    case SQLCOM_ALTER_SERVER:
#ifdef IS_MARIADB
    case SQLCOM_CREATE_PACKAGE:
    case SQLCOM_DROP_PACKAGE:
    case SQLCOM_CREATE_PACKAGE_BODY:
    case SQLCOM_DROP_PACKAGE_BODY:
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

// query table 'HAObjectState' and get all objects to be dropped in database
// add dropped database info at last
static int get_drop_db_objects(THD *thd, ha_sql_stmt_info *sql_info) {
  DBUG_ENTER("get_drop_db_objects");
  DBUG_ASSERT(sql_info->tables == NULL);
  int rc = 0;
  Sdb_cl obj_state_cl;
  bson::BSONObj cond, obj, result;
  Sdb_conn *sdb_conn = sql_info->sdb_conn;
  const char *db_name = thd->lex->name.str;
  ha_table_list *ha_tbl_list = NULL, *ha_tbl_list_tail = NULL;

  rc =
      ha_get_object_state_cl(*sdb_conn, ha_thread.sdb_group_name, obj_state_cl);
  if (rc) {
    ha_error_string(*sdb_conn, rc, sql_info->err_message);
    goto error;
  }

  cond = BSON(HA_FIELD_DB << db_name);
  rc = obj_state_cl.query(cond);
  if (rc) {
    ha_error_string(*sdb_conn, rc, sql_info->err_message);
    goto error;
  }

  while (!obj_state_cl.next(result, false) && !result.isEmpty()) {
    const char *db_name = result.getStringField(HA_FIELD_DB);
    const char *table_name = result.getStringField(HA_FIELD_TABLE);
    const char *op_type = result.getStringField(HA_FIELD_TYPE);
    if ((0 == strlen(table_name) &&
         0 == strcmp(op_type, HA_OPERATION_TYPE_DB))) {
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
      rc = SDB_HA_OOM;
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
      rc = SDB_HA_OOM;
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

// add 'S' lock for an object
inline static int add_slock(Sdb_cl &lock_cl, const char *db_name,
                            const char *table_name, const char *op_type,
                            ha_sql_stmt_info *sql_info) {
  int rc = 0;
  bson::BSONObj cond, obj, result;
  cond = BSON(HA_FIELD_DB << db_name << HA_FIELD_TABLE << table_name
                          << HA_FIELD_TYPE << op_type);
  rc = lock_cl.query(cond);
  rc = rc ? rc : lock_cl.next(result, false);
  if (rc && HA_ERR_END_OF_FILE != rc) {
    ha_error_string(*sql_info->sdb_conn, rc, sql_info->err_message);
  }
  return rc;
}

// add 'X' lock for an object
inline static int add_xlock(Sdb_cl &lock_cl, const char *db_name,
                            const char *table_name, const char *op_type,
                            ha_sql_stmt_info *sql_info) {
  int rc = 0;
  bson::BSONObj cond, obj;
  cond = BSON(HA_FIELD_DB << db_name << HA_FIELD_TABLE << table_name
                          << HA_FIELD_TYPE << op_type);
  obj = BSON("$inc" << BSON(HA_FIELD_VERSION << 1));
  rc = lock_cl.upsert(obj, cond);
  if (rc) {
    ha_error_string(*sql_info->sdb_conn, rc, sql_info->err_message);
  }
  return rc;
}

// try to lock a record in 'HALock' table
// 1. set a shorter 'TransTimeout' before add 'X' lock
// 2. try locking again 2 times after timeout
inline static int try_add_xlock(Sdb_conn &sdb_conn, Sdb_cl &lock_cl,
                                const bson::BSONObj &cond, bson::BSONObj &obj) {
  static const int MAX_TRY_COUNT = 3;

  int rc = 0;
  int save_trans_timeout = -1;
  int tmp_trans_timeout = -1;
  bson::BSONObj attr;
  rc = sdb_conn.get_session_attr(attr);
  if (rc) {
    goto error;
  }
  save_trans_timeout = attr.getIntField(HA_TRANSACTION_TIMEOUT);
  DBUG_ASSERT(save_trans_timeout >= 0);

  // set 'TransTimeout', try 3 times at most
  tmp_trans_timeout = save_trans_timeout / MAX_TRY_COUNT;
  if (0 == tmp_trans_timeout) {
    // set min 'TransTimeout' if tmp_trans_timeout is zero
    tmp_trans_timeout = 1;
  }
  attr = BSON(HA_TRANSACTION_TIMEOUT << tmp_trans_timeout);
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
    attr = BSON(HA_TRANSACTION_TIMEOUT << save_trans_timeout);
    sdb_conn.set_session_attr(attr);
  }
done:
  return rc;
error:
  goto done;
}

// add extra lock for objects involved in current SQL statement
static int pre_lock_objects(THD *thd, ha_sql_stmt_info *sql_info) {
  int rc = 0;
  Sdb_conn *sdb_conn = sql_info->sdb_conn;
  Sdb_cl lock_cl;
  bson::BSONObj cond, obj, result;
  int sql_command = thd_sql_command(thd);
  const char *db_name = NULL, *table_name = NULL, *op_type = NULL;

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
    op_type = HA_OPERATION_TYPE_DB;
    SDB_LOG_DEBUG("HA: Add 'X' lock for database '%s'", db_name);
    rc = add_xlock(lock_cl, db_name, table_name, op_type, sql_info);
    if (rc) {
      goto error;
    }
  } else if (is_routine_meta_sql(thd) && strlen(sql_info->sp_db_name)) {
    // add 'S' lock for database including this routine
    for (ha_table_list *ha_tables = sql_info->tables; ha_tables;
         ha_tables = ha_tables->next) {
      db_name = ha_tables->db_name;
      table_name = HA_EMPTY_STRING;
      op_type = HA_OPERATION_TYPE_DB;
      SDB_LOG_DEBUG("HA: Add 'S' lock for database '%s'", db_name);
      rc = add_slock(lock_cl, db_name, table_name, op_type, sql_info);
      if (HA_ERR_END_OF_FILE == rc) {
        SDB_LOG_DEBUG("HA: Failed to add 'S' lock, add 'X' lock for '%s:%s'",
                      db_name, table_name);
        rc = add_xlock(lock_cl, db_name, table_name, op_type, sql_info);
      }
      if (rc) {
        goto error;
      }
    }
  } else if (is_dcl_meta_sql(thd) && sql_info->tables &&
             (0 == thd->lex->type || TYPE_ENUM_PROCEDURE == thd->lex->type ||
              TYPE_ENUM_FUNCTION == thd->lex->type)) {
    // grant object can be 'TABLE/FUNCTION/PROCEDURE'
    // add 'S' lock for database include 'routine' or table
    for (ha_table_list *ha_tables = sql_info->tables; ha_tables;
         ha_tables = ha_tables->next) {
      db_name = ha_tables->db_name;
      table_name = HA_EMPTY_STRING;
      op_type = HA_OPERATION_TYPE_DB;
      SDB_LOG_DEBUG("HA: Add 'S' lock for database '%s'", db_name);
      rc = add_slock(lock_cl, db_name, table_name, op_type, sql_info);
      if (HA_ERR_END_OF_FILE == rc) {
        SDB_LOG_DEBUG("HA: Failed to add 'S' lock, add 'X' lock for '%s:%s'",
                      db_name, table_name);
        rc = add_xlock(lock_cl, db_name, table_name, op_type, sql_info);
      }
      if (rc) {
        goto error;
      }
    }
  } else {
    // add 'S' lock for databases
    for (ha_table_list *ha_tables = sql_info->tables; ha_tables;
         ha_tables = ha_tables->next) {
      db_name = ha_tables->db_name;
      table_name = HA_EMPTY_STRING;
      op_type = HA_OPERATION_TYPE_DB;
      SDB_LOG_DEBUG("HA: Add 'S' lock for database '%s'", db_name);
      rc = add_slock(lock_cl, db_name, table_name, op_type, sql_info);
      if (HA_ERR_END_OF_FILE == rc) {
        SDB_LOG_DEBUG("HA: Failed to add 'S' lock, add 'X' lock for '%s:%s'",
                      db_name, table_name);
        rc = add_xlock(lock_cl, db_name, table_name, op_type, sql_info);
      }
      if (rc) {
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

// get cached record without lock
ha_cached_record *get_cached_record(HASH &cache,
                                    const char *cached_record_key) {
  DBUG_ENTER("get_cached_record");
  HASH_SEARCH_STATE state;
  uchar *key = (uchar *)cached_record_key;
  ha_cached_record *record = (ha_cached_record *)my_hash_first(
      &cache, key, strlen(cached_record_key), &state);

  while (record && 0 != strcmp(cached_record_key, record->key)) {
    record = (ha_cached_record *)my_hash_next(
        &cache, key, strlen(cached_record_key), &state);
  }

  if (record && 0 != strcmp(cached_record_key, record->key)) {
    record = NULL;
  }
  DBUG_RETURN(record);
}

// update instance object state cached record without lock
int update_cached_record(HASH &cache, PSI_memory_key mem_key,
                         const char *cached_record_key, int sql_id,
                         int cata_version) {
  int rc = 0;
  ha_cached_record *cached_record = get_cached_record(cache, cached_record_key);
  if (cached_record) {
    cached_record->sql_id = sql_id;
    cached_record->cata_version = cata_version;
  } else {
    ha_cached_record *record = NULL;
    char *key = NULL;
    int key_len = strlen(cached_record_key);
    if (!sdb_multi_malloc(mem_key, MYF(MY_WME | MY_ZEROFILL), &record,
                          sizeof(ha_cached_record), &key, key_len + 1, NullS)) {
      rc = SDB_HA_OOM;
      goto error;
    }
    snprintf(key, key_len + 1, "%s", cached_record_key);
    key[key_len] = '\0';
    record->key = key;
    record->sql_id = sql_id;
    record->cata_version = cata_version;
    if (my_hash_insert(&cache, (uchar *)record)) {
      rc = SDB_HA_OOM;
    }
  }
done:
  return rc;
error:
  goto done;
}

// base on 'my_hash_sort' in mariadb
static my_hash_value_type my_hash_value(CHARSET_INFO *cs, const uchar *key,
                                        size_t length) {
  ulong nr1 = 1, nr2 = 4;
  cs->coll->hash_sort(cs, (uchar *)key, length, &nr1, &nr2);
  return (my_hash_value_type)nr1;
}

// find cached record in it's own hash bucket
ha_cached_record *ha_get_cached_record(const char *cached_record_key) {
  my_hash_value_type bucket_num =
      my_hash_value(system_charset_info, (uchar *)cached_record_key,
                    strlen(cached_record_key));
  bucket_num = bucket_num % HA_MAX_CATA_VERSION_CACHES;
  ha_inst_state_cache *inst_state_cache =
      &ha_thread.inst_state_caches[bucket_num];
  ha_cached_record *record = NULL;
  native_rw_rdlock(&inst_state_cache->rw_lock);
  record = get_cached_record(inst_state_cache->cache, cached_record_key);
  native_rw_unlock(&inst_state_cache->rw_lock);
  return record;
}

// update cached record in it's own hash bucket
int ha_update_cached_record(const char *cached_record_key, int sql_id,
                            int cata_version) {
  int rc = 0;
  my_hash_value_type bucket_num =
      my_hash_value(system_charset_info, (uchar *)cached_record_key,
                    strlen(cached_record_key));
  bucket_num = bucket_num % HA_MAX_CATA_VERSION_CACHES;
  ha_inst_state_cache *state_cache = &ha_thread.inst_state_caches[bucket_num];

  native_rw_wrlock(&state_cache->rw_lock);
  rc = update_cached_record(state_cache->cache, HA_KEY_MEM_INST_STATE_CACHE,
                            cached_record_key, sql_id, cata_version);
  native_rw_unlock(&state_cache->rw_lock);
  return rc;
}

// wait instance state to be updated to lastest state by replay thread
static int wait_object_updated_to_lastest(
    const char *db_name, const char *table_name, const char *op_type,
    Sdb_cl &obj_state_cl, ha_sql_stmt_info *sql_info, THD *thd) {
  DBUG_ENTER("wait_object_updated_to_lastest");

  int sql_id = HA_INVALID_SQL_ID, rc = 0, cata_version;
  bson::BSONObj cond, result;
  Sdb_conn *sdb_conn = sql_info->sdb_conn;
  ha_cached_record *cached_record = NULL;
  char cached_record_key[HA_MAX_CACHED_RECORD_KEY_LEN] = {0};
  uint sleep_secs = 0;

  // get latest SQL id from 'HAObjectState'
  cond = BSON(HA_FIELD_DB << db_name << HA_FIELD_TABLE << table_name
                          << HA_FIELD_TYPE << op_type);
  rc = obj_state_cl.query(cond);
  rc = rc ? rc : obj_state_cl.next(result, false);
  if (0 == rc) {
    sql_id = result.getIntField(HA_FIELD_SQL_ID);
    cata_version = result.getIntField(HA_FIELD_CAT_VERSION);
    DBUG_ASSERT(sql_id >= 0);
  } else if (HA_ERR_END_OF_FILE == rc) {
    // can't find object state in 'HAObjectState', this means that those
    // objects exists before instance group function
    SDB_LOG_DEBUG("HA: Can't find object state in 'HAObjectState' for '%s:%s'",
                  db_name, table_name);
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
    cached_record = ha_get_cached_record(cached_record_key);
    // if 'db_name:table_name:op_type' does not exists
    if (!abort_loop && !cached_record) {
      sleep(1);
      sleep_secs++;
    }
  } while (!abort_loop && !cached_record &&
           sleep_secs < ha_wait_replay_timeout && !thd_killed(thd));

  if (sleep_secs >= ha_wait_replay_timeout) {
    rc = SDB_HA_WAIT_TIMEOUT;
    goto error;
  } else if (thd_killed(thd) || abort_loop) {
    rc = SDB_HA_ABORT_BY_USER;
    goto error;
  }

  // if local sql_id less than global sql id on table 'table_name'
  sleep_secs = 0;
  SDB_LOG_DEBUG(
      "HA: Wait for '%s' state, cached SQL ID: %d, global SQL ID: %d, "
      "cached CataVersion: %d, global CataVersion: %d",
      cached_record_key, cached_record->sql_id, sql_id,
      cached_record->cata_version, cata_version);
  while (!abort_loop && cached_record && cached_record->sql_id < sql_id &&
         sleep_secs < ha_wait_replay_timeout && !thd_killed(thd)) {
    sleep(1);
    sleep_secs++;
  }

  if (sleep_secs >= ha_wait_replay_timeout) {
    rc = SDB_HA_WAIT_TIMEOUT;
    goto error;
  } else if (thd_killed(thd) || abort_loop) {
    rc = SDB_HA_ABORT_BY_USER;
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
  Sdb_cl obj_state_cl, inst_obj_state_cl;
  const char *db_name = NULL, *table_name = NULL, *op_type = NULL;
  rc = sdb_conn->get_cl(ha_thread.sdb_group_name, HA_OBJECT_STATE_CL,
                        obj_state_cl);
  if (rc) {
    goto sdb_error;
  }
  rc = sdb_conn->get_cl(ha_thread.sdb_group_name, HA_INSTANCE_OBJECT_STATE_CL,
                        inst_obj_state_cl);
  if (rc) {
    goto sdb_error;
  }

  if (is_db_meta_sql(thd)) {
    db_name = thd->lex->name.str;
    table_name = HA_EMPTY_STRING;
    op_type = HA_OPERATION_TYPE_DB;
    rc = wait_object_updated_to_lastest(db_name, table_name, op_type,
                                        obj_state_cl, sql_info, thd);
    if (0 == rc && SQLCOM_DROP_DB == thd_sql_command(thd)) {
      rc = get_drop_db_objects(thd, sql_info);
    }
    if (rc) {
      goto error;
    }
  } else if (is_routine_meta_sql(thd) && strlen(sql_info->sp_db_name)) {
    // wait for the databases involved to be updated to latest state
    for (ha_table_list *ha_tables = sql_info->tables; ha_tables;
         ha_tables = ha_tables->next) {
      db_name = ha_tables->db_name;
      table_name = HA_EMPTY_STRING;
      op_type = HA_OPERATION_TYPE_DB;
      rc = wait_object_updated_to_lastest(db_name, table_name, op_type,
                                          obj_state_cl, sql_info, thd);
      if (rc) {
        goto error;
      }
    }
  } else if (is_dcl_meta_sql(thd) && sql_info->tables &&
             (0 == thd->lex->type || TYPE_ENUM_PROCEDURE == thd->lex->type ||
              TYPE_ENUM_FUNCTION == thd->lex->type)) {
    // lex->type == 0 means that granted object is table
    for (ha_table_list *ha_tables = sql_info->tables; ha_tables;
         ha_tables = ha_tables->next) {
      db_name = ha_tables->db_name;
      table_name = HA_EMPTY_STRING;
      op_type = HA_OPERATION_TYPE_DB;
      rc = wait_object_updated_to_lastest(db_name, table_name, op_type,
                                          obj_state_cl, sql_info, thd);
      if (rc) {
        goto error;
      }
    }
  } else if (!is_db_meta_sql(thd) && sql_info->tables) {
    ha_table_list *ha_tables = sql_info->tables;
    do {
      db_name = ha_tables->db_name;
      table_name = HA_EMPTY_STRING;
      op_type = HA_OPERATION_TYPE_DB;
      if (db_name) {
        rc = wait_object_updated_to_lastest(db_name, table_name, op_type,
                                            obj_state_cl, sql_info, thd);
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
  Sdb_cl obj_state_cl, inst_obj_state_cl;
  ha_table_list *ha_tables = NULL;

  // get object state handle
  rc = ha_get_object_state_cl(*sql_info->sdb_conn, ha_thread.sdb_group_name,
                              obj_state_cl);
  if (rc) {
    ha_error_string(*sdb_conn, rc, sql_info->err_message);
    goto error;
  }

  rc = ha_get_instance_object_state_cl(
      *sql_info->sdb_conn, ha_thread.sdb_group_name, inst_obj_state_cl);
  if (rc) {
    ha_error_string(*sdb_conn, rc, sql_info->err_message);
    goto error;
  }

  rc = pre_wait_objects_updated_to_lastest(thd, sql_info);
  if (rc) {
    goto error;
  }

  ha_tables = sql_info->tables;
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
                                        obj_state_cl, sql_info, thd);
    if (rc) {
      goto error;
    }
  }
done:
  return rc;
error:
  goto done;
}

static bool build_full_table_name(THD *thd, String &full_name,
                                  const char *db_name, const char *table_name) {
  bool error = false;
  append_identifier(thd, &full_name, db_name, strlen(db_name));
  error |= full_name.append('.');
  append_identifier(thd, &full_name, table_name, strlen(table_name));
  error |= full_name.append(' ');
  return error;
}

// check if the dropping table exists, SQL like 'drop table [if exists]
// t1,t2,t3' if 't2' does not exist, the error or warning will be "Unknown table
// 'db1.t2'" in this situation, SQL log "DROP TABLE `db1`.`t2`" can't be written
// into sequoiadb
static bool dropping_table_exists(THD *thd, const char *db_name,
                                  const char *table_name) {
  static const uint TABLE_BUF_LEN = NAME_LEN * 2 + 20;
  int sql_command = thd_sql_command(thd);

  bool table_exists = true;
  char unknown_table_buf[TABLE_BUF_LEN] = {0};
  String wrong_table(unknown_table_buf, TABLE_BUF_LEN, system_charset_info);
  wrong_table.length(0);

  if (!thd->is_error()) {
    // handle drop table if exists
#ifdef IS_MARIADB
    if (SQLCOM_DROP_TABLE == sql_command) {
      wrong_table.append("Unknown table '");
    } else if (SQLCOM_DROP_VIEW == sql_command) {
      wrong_table.append("Unknown VIEW: '");
    }
#else
    wrong_table.append("Unknown table '");
#endif
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
// 2. update 'HAObjectState' and 'HAInstanceObjectState'
static int write_sql_log_and_states(THD *thd, ha_sql_stmt_info *sql_info,
                                    const ha_event_general &event) {
  int rc = 0, sql_id = -1;
  Sdb_conn *sdb_conn = sql_info->sdb_conn;
  Sdb_cl sql_log_cl, obj_state_cl, inst_obj_state_cl, lock_cl;
  ha_table_list *ha_tables = sql_info->tables;
  bson::BSONObj obj, result, cond, hint;
  int sql_command = thd_sql_command(thd);
  char quoted_name_buf[NAME_LEN * 2 + 3] = {0};
  char session_attrs[HA_MAX_SESSION_ATTRS_LEN] = {0};
  uint client_charset_num = thd->charset()->number;
  String general_query, query;
  char cached_record_key[NAME_LEN * 2 + 20] = {0};
  bool oom = false;  // out of memory while building a string
  bool first_object = true;
  int rename_table_count = 0;

  oom = general_query.append(event.general_query, event.general_query_length);
  oom |= general_query.append('\0');
  if (oom) {
    rc = SDB_HA_OOM;
    goto error;
  }

  rc = sdb_conn->get_cl(ha_thread.sdb_group_name, HA_SQL_LOG_CL, sql_log_cl);
  if (rc) {
    goto sdb_error;
  }

  rc = sdb_conn->get_cl(ha_thread.sdb_group_name, HA_OBJECT_STATE_CL,
                        obj_state_cl);
  if (rc) {
    goto sdb_error;
  }

  rc = sdb_conn->get_cl(ha_thread.sdb_group_name, HA_INSTANCE_OBJECT_STATE_CL,
                        inst_obj_state_cl);
  if (rc) {
    goto sdb_error;
  }

  build_session_attributes(thd, session_attrs);

  // set charset for following sql statement
  if (SQLCOM_DROP_TABLE == sql_command || SQLCOM_DROP_VIEW == sql_command ||
      SQLCOM_RENAME_TABLE == sql_command) {
    query.set_charset(thd->charset());
  }

  for (; ha_tables; ha_tables = ha_tables->next) {
    const char *db_name = ha_tables->db_name;
    const char *table_name = ha_tables->table_name;
    const char *op_type = ha_tables->op_type;
    const char *new_db_name = NULL;
    const char *new_tbl_name = NULL;
    int cata_version = ha_tables->cata_version;

    // previous version maybe not be latest, get cached cata version again
    if (0 == strcmp(op_type, HA_OPERATION_TYPE_TABLE)) {
      int version = ha_get_cata_version(db_name, table_name);
      cata_version = (version > cata_version) ? version : cata_version;
    }
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
      oom = query.append(general_query);
    } else {
      // skip temporary tables for drop/alter/rename table sql
      if (SQLCOM_DROP_TABLE == sql_command ||
          SQLCOM_ALTER_TABLE == sql_command ||
          SQLCOM_RENAME_TABLE == sql_command) {
        if (ha_tables->is_temporary_table) {
          continue;
        }
      }

      // decompose 'SQLCOM_DROP_TABLE/SQLCOM_DROP_VIEW' command into
      // multiple 'DROP TABLE/VIEW' commands
      if (SQLCOM_DROP_TABLE == sql_command || SQLCOM_DROP_VIEW == sql_command) {
        // if the dropping object doesn't exist, don't write SQL log
        if (!dropping_table_exists(thd, db_name, table_name)) {
          continue;
        }

        if (SQLCOM_DROP_TABLE == sql_command) {
          oom = query.append("DROP TABLE IF EXISTS ");
        } else {
          oom |= query.append("DROP VIEW IF EXISTS ");
        }
        oom |= build_full_table_name(thd, query, db_name, table_name);
      } else if (SQLCOM_RENAME_TABLE == sql_command) {
        // build rename table command for original table
        if (0 == rename_table_count % 2) {
          new_db_name = ha_tables->next->db_name;
          new_tbl_name = ha_tables->next->table_name;

          oom = query.append("RENAME TABLE ");
          oom |= build_full_table_name(thd, query, db_name, table_name);
          oom |= query.append(" TO ");
          oom |= build_full_table_name(thd, query, new_db_name, new_tbl_name);
        }
        rename_table_count++;
      } else if (SQLCOM_CREATE_TRIGGER == sql_command ||
                 SQLCOM_CREATE_VIEW == sql_command ||
                 SQLCOM_ALTER_EVENT == sql_command ||
                 SQLCOM_ALTER_TABLE == sql_command || is_dcl_meta_sql(thd)) {
        // 1. creating view depends on multiple tables and functions
        // 2. grant/revoke operation may depends on table/fun/proc
        // 3. 'create/drop user' can hold multiple users
        // 4. 'alter event/table rename' hold two objects

        // write SQL statement just for the first object
        if (first_object) {
          oom = query.append(general_query);
          first_object = false;
        }
      } else if (have_exist_warning(thd)) {
        // if thd have 'not exist' or 'already exist' warning
        continue;
      } else {
        oom = query.append(general_query);
      }
    }

    if (oom) {
      rc = SDB_HA_OOM;
      goto error;
    }
    // write sql info into 'HASQLLog' table
    obj = BSON(HA_FIELD_DB << db_name << HA_FIELD_TABLE << table_name
                           << HA_FIELD_TYPE << op_type << HA_FIELD_SQL
                           << query.c_ptr_safe() << HA_FIELD_OWNER
                           << ha_thread.instance_id << HA_FIELD_SESSION_ATTRS
                           << session_attrs << HA_FIELD_CLIENT_CHARSET_NUM
                           << client_charset_num << HA_FIELD_CAT_VERSION
                           << cata_version);
    rc = sql_log_cl.insert(obj, hint, 0, &result);
    if (rc) {
      goto sdb_error;
    }

    // write 'HAObjectState'
    sql_id = result.getIntField(SDB_FIELD_LAST_GEN_ID);
    DBUG_ASSERT(sql_id > 0);

    cond = BSON(HA_FIELD_DB << db_name << HA_FIELD_TABLE << table_name
                            << HA_FIELD_TYPE << op_type);
    obj = BSON("$set" << BSON(HA_FIELD_SQL_ID << sql_id << HA_FIELD_CAT_VERSION
                                              << cata_version));
    rc = obj_state_cl.upsert(obj, cond);
    if (rc) {
      goto sdb_error;
    }

    // write 'HAInstanceObjectState'
    cond = BSON(HA_FIELD_INSTANCE_ID << ha_thread.instance_id << HA_FIELD_DB
                                     << db_name << HA_FIELD_TABLE << table_name
                                     << HA_FIELD_TYPE << op_type);
    obj = BSON("$set" << BSON(HA_FIELD_SQL_ID << sql_id << HA_FIELD_CAT_VERSION
                                              << cata_version));
    rc = inst_obj_state_cl.upsert(obj, cond);
    if (rc) {
      goto sdb_error;
    }

    // update cached instance object state
    snprintf(cached_record_key, NAME_LEN * 2 + 20, "%s-%s-%s", db_name,
             table_name, op_type);
    rc = ha_update_cached_record(cached_record_key, sql_id, cata_version);
#ifdef IS_MYSQL
    if (SQLCOM_CREATE_TABLE == sql_command) {
      // sql like 'create table/view' may have multi tables in its table_list
      break;
    }
#endif
  }
done:
  return rc;
sdb_error:
  // get sequoiadb error string
  ha_error_string(*sdb_conn, rc, sql_info->err_message);
error:
  goto done;
}

// get SQL objects for DCL
static int get_sql_objects_for_dcl(THD *thd, ha_sql_stmt_info *sql_info) {
  int rc = 0;
  int sql_command = thd_sql_command(thd);
  LEX_USER *lex_user = NULL;
  List_iterator<LEX_USER> users_list(thd->lex->users_list);
  ha_table_list *ha_tbl_node = NULL, *ha_tbl_list_tail = NULL;
  TABLE_LIST *tables = sdb_lex_first_select(thd)->get_table_list();

  // add involved users
  while ((lex_user = users_list++)) {
    ha_tbl_node = (ha_table_list *)thd_calloc(thd, sizeof(ha_table_list));
    if (NULL == ha_tbl_node) {
      rc = SDB_HA_OOM;
      goto error;
    }
    if (tables) {
      ha_tbl_node->db_name = C_STR(tables->db);
    } else {
      ha_tbl_node->db_name = HA_MYSQL_DB;
    }
    ha_tbl_node->table_name = lex_user->user.str;
    ha_tbl_node->op_type = HA_OPERATION_TYPE_DCL;
    ha_tbl_node->is_temporary_table = false;
    ha_tbl_node->next = NULL;
    if (NULL == sql_info->tables) {
      sql_info->tables = ha_tbl_node;
      ha_tbl_list_tail = sql_info->tables;
    } else {
      ha_tbl_list_tail->next = ha_tbl_node;
      ha_tbl_list_tail = ha_tbl_list_tail->next;
    }
  }

  // add granted objects 'proc/func/table'
  if (tables) {
    const char *op_type = NULL;
    if (TYPE_ENUM_PROCEDURE == thd->lex->type) {
      op_type = HA_ROUTINE_TYPE_PROC;
    } else if (TYPE_ENUM_FUNCTION == thd->lex->type) {
      op_type = HA_ROUTINE_TYPE_FUNC;
    } else if (0 == thd->lex->type) {
      op_type = HA_OPERATION_TYPE_TABLE;
    }

    if (op_type) {
      ha_tbl_node = (ha_table_list *)thd_calloc(thd, sizeof(ha_table_list));
      if (NULL == ha_tbl_node) {
        rc = SDB_HA_OOM;
        goto error;
      }
      ha_tbl_node->db_name = C_STR(tables->db);
      ha_tbl_node->table_name = C_STR(tables->table_name);
      ha_tbl_node->op_type = op_type;
      ha_tbl_node->is_temporary_table = false;

      // set cat version for table
      if (0 == thd->lex->type) {
        ha_tbl_node->cata_version =
            ha_get_cata_version(ha_tbl_node->db_name, ha_tbl_node->table_name);
      } else {
        ha_tbl_node->cata_version = 0;
      }

      ha_tbl_node->next = NULL;
      ha_tbl_list_tail->next = ha_tbl_node;
    }
  }
done:
  return rc;
error:
  goto done;
}

// get objects involved in current SQL statement
static int get_sql_objects(THD *thd, ha_sql_stmt_info *sql_info) {
  DBUG_ENTER("get_sql_objects");
  int rc = 0;
  int sql_command = thd_sql_command(thd);
  static const int INVALID_CAT_VERSION = 0;

  DBUG_ASSERT(NULL == sql_info->tables);
  if (SQLCOM_DROP_DB == sql_command) {
    // 'DROP database ' get records from 'HAObjectState' after add 'X' lock
    // on database
  } else if (SQLCOM_CREATE_DB == sql_command ||
             SQLCOM_ALTER_DB == sql_command) {
    sql_info->tables = (ha_table_list *)thd_calloc(thd, sizeof(ha_table_list));
    if (NULL == sql_info->tables) {
      rc = SDB_HA_OOM;
      goto error;
    }
    sql_info->tables->db_name = thd->lex->name.str;
    sql_info->tables->table_name = HA_EMPTY_STRING;
    sql_info->tables->op_type = HA_OPERATION_TYPE_DB;
    sql_info->tables->cata_version = INVALID_CAT_VERSION;
    sql_info->tables->is_temporary_table = false;
    sql_info->tables->next = NULL;
  } else if (is_routine_meta_sql(thd)) {
    // routine include procedure, function, trigger and event
    memset(sql_info->sp_db_name, 0, NAME_LEN + 1);
    memset(sql_info->sp_name, 0, NAME_LEN + 1);
    // set sp database and sp_name
    if (thd->lex->event_parse_data) {
      // fix BUG-775
      // 1. if alter event statement change event body, event body will be
      //    stored in THD::LEX::sphead
      // 2. if alter event statement change event name, new event name will
      //    be stored in THD::LEX::spname
      if (thd->lex->event_parse_data->identifier->m_db.str) {
        sprintf(sql_info->sp_db_name, "%s",
                thd->lex->event_parse_data->identifier->m_db.str);
      }
      sprintf(sql_info->sp_name, "%s",
              thd->lex->event_parse_data->identifier->m_name.str);
    } else if (thd->lex->sphead) {
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

    // if it's 'alter event' statement and event body is modified
    // store alter event body
    sql_info->alter_event_body = NULL;
    if (SQLCOM_ALTER_EVENT == sql_command && thd->lex->sphead) {
      sql_info->alter_event_body =
          (char *)thd_calloc(thd, thd->lex->sphead->m_body.length + 1);
      sprintf(sql_info->alter_event_body, "%s", thd->lex->sphead->m_body.str);
    }

    DBUG_ASSERT(strlen(sql_info->sp_name) != 0);
    DBUG_ASSERT(strlen(sql_info->sp_db_name) != 0);

    sql_info->tables = (ha_table_list *)thd_calloc(thd, sizeof(ha_table_list));
    if (NULL == sql_info->tables) {
      rc = SDB_HA_OOM;
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
        break;
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

    // add renamed object to 'sql_info->tables' for 'alter event rename'
    if (SQLCOM_ALTER_EVENT == sql_command && thd->lex->spname) {
      ha_table_list *ha_tbl_list =
          (ha_table_list *)thd_calloc(thd, sizeof(ha_table_list));
      if (NULL == ha_tbl_list) {
        rc = SDB_HA_OOM;
        goto error;
      }
      ha_tbl_list->db_name = thd->lex->spname->m_db.str;
      ha_tbl_list->table_name = thd->lex->spname->m_name.str;
      ha_tbl_list->is_temporary_table = false;
      ha_tbl_list->op_type = HA_ROUTINE_TYPE_EVENT;
      ha_tbl_list->cata_version = INVALID_CAT_VERSION;
      ha_tbl_list->next = NULL;
      sql_info->tables->next = ha_tbl_list;
    }
    // add table object to 'sql_info->tables' for 'create trigger'
    TABLE_LIST *tables = sdb_lex_first_select(thd)->get_table_list();
    if (SQLCOM_CREATE_TRIGGER == sql_command && tables) {
      ha_table_list *ha_tbl_list =
          (ha_table_list *)thd_calloc(thd, sizeof(ha_table_list));
      if (NULL == ha_tbl_list) {
        rc = SDB_HA_OOM;
        goto error;
      }
      ha_tbl_list->db_name = C_STR(tables->db);
      ha_tbl_list->table_name = C_STR(tables->table_name);
      ha_tbl_list->is_temporary_table = false;
      ha_tbl_list->op_type = HA_OPERATION_TYPE_TABLE;
      ha_tbl_list->cata_version =
          ha_get_cata_version(ha_tbl_list->db_name, ha_tbl_list->table_name);
      ha_tbl_list->next = NULL;
      sql_info->tables->next = ha_tbl_list;
    }
  } else if (is_package_meta_sql(thd)) {
    memset(sql_info->sp_db_name, 0, NAME_LEN + 1);
    memset(sql_info->sp_name, 0, NAME_LEN + 1);
    if (thd->lex->spname) {
      sprintf(sql_info->sp_db_name, "%s", thd->lex->spname->m_db.str);
      sprintf(sql_info->sp_name, "%s", thd->lex->spname->m_name.str);
    } else if (thd->lex->sphead) {
      sprintf(sql_info->sp_db_name, "%s", thd->lex->sphead->m_db.str);
      sprintf(sql_info->sp_name, "%s", thd->lex->sphead->m_name.str);
    }
    sql_info->tables = (ha_table_list *)thd_calloc(thd, sizeof(ha_table_list));
    if (NULL == sql_info->tables) {
      rc = SDB_HA_OOM;
      goto error;
    }
    sql_info->tables->db_name = sql_info->sp_db_name;
    sql_info->tables->table_name = sql_info->sp_name;
    sql_info->tables->is_temporary_table = false;
    sql_info->tables->op_type = HA_ROUTINE_TYPE_PACKAGE;
    sql_info->tables->next = NULL;
  } else if (SQLCOM_ALTER_TABLESPACE == sql_command ||
             SQLCOM_CREATE_SERVER == sql_command ||
             SQLCOM_DROP_SERVER == sql_command ||
             SQLCOM_ALTER_SERVER == sql_command) {
    sql_info->tables = (ha_table_list *)thd_calloc(thd, sizeof(ha_table_list));
    if (NULL == sql_info->tables) {
      rc = SDB_HA_OOM;
      goto error;
    }
    sql_info->tables->db_name = HA_MYSQL_DB;
    if (SQLCOM_ALTER_TABLESPACE == sql_command) {
      sql_info->tables->table_name =
          thd->lex->alter_tablespace_info->tablespace_name;
    } else {
#ifdef IS_MARIADB
      sql_info->tables->table_name = thd->lex->server_options.server_name.str;
#else
      sql_info->tables->table_name = thd->lex->server_options.m_server_name.str;
      // if its 'drop server' statement
      if (SQLCOM_DROP_SERVER == sql_command && !sql_info->tables->table_name) {
        Sql_cmd_drop_server *drop_server_cmd =
            (Sql_cmd_drop_server *)thd->lex->m_sql_cmd;
        struct st_sql_cmd_drop_server *sql_cmd =
            reinterpret_cast<struct st_sql_cmd_drop_server *>(drop_server_cmd);
        sql_info->tables->table_name = sql_cmd->m_server_name.str;
      }
#endif
    }
    sql_info->tables->op_type = HA_OPERATION_TYPE_TABLE;
    sql_info->tables->cata_version = INVALID_CAT_VERSION;
    sql_info->tables->is_temporary_table = false;
    sql_info->tables->next = NULL;
  } else if (is_dcl_meta_sql(thd)) {
    rc = get_sql_objects_for_dcl(thd, sql_info);
    if (rc) {
      goto error;
    }
  } else {
    TABLE_LIST *tables = NULL;
    if (SQLCOM_CREATE_TABLE == sql_command ||
        SQLCOM_CREATE_VIEW == sql_command) {
      tables = thd->lex->query_tables;
    } else {
      tables = sdb_lex_first_select(thd)->get_table_list();
    }
    ha_table_list *ha_tbl_list = NULL, *ha_tbl_list_tail = NULL;
    const char *db_name = NULL, *table_name = NULL;
    bool is_temp_table = false;

    for (TABLE_LIST *tbl = tables; tbl; tbl = tbl->next_global) {
      ha_tbl_list = (ha_table_list *)thd_calloc(thd, sizeof(ha_table_list));
      if (ha_tbl_list == NULL) {
        rc = SDB_HA_OOM;
        goto error;
      }
      ha_tbl_list->db_name = C_STR(tbl->db);
      ha_tbl_list->table_name = C_STR(tbl->table_name);
      if (!ha_tbl_list->db_name || !ha_tbl_list->table_name) {
        continue;
      }

      ha_tbl_list->op_type = HA_OPERATION_TYPE_TABLE;
      ha_tbl_list->cata_version =
          ha_get_cata_version(ha_tbl_list->db_name, ha_tbl_list->table_name);
      ha_tbl_list->is_temporary_table = false;
      ha_tbl_list->next = NULL;
      if (!sql_info->tables) {
        if (SQLCOM_CREATE_VIEW == sql_command) {
          ha_tbl_list->cata_version = INVALID_CAT_VERSION;
        }
        sql_info->tables = ha_tbl_list;
        ha_tbl_list_tail = ha_tbl_list;
      } else {
        ha_tbl_list_tail->next = ha_tbl_list;
        ha_tbl_list_tail = ha_tbl_list;
      }

      // mark temporary table for SQL like 'rename/alter/drop table'
      if (SQLCOM_DROP_TABLE == sql_command ||
          SQLCOM_ALTER_TABLE == sql_command ||
          SQLCOM_RENAME_TABLE == sql_command) {
        db_name = ha_tbl_list->db_name;
        table_name = ha_tbl_list->table_name;

        is_temp_table = is_temporary_table(thd, db_name, table_name);
        // mark temporary table by setting 'ha_table_list::is_temporary_table'
        if (is_temp_table) {
          SDB_LOG_DEBUG("HA: found temporary table %s:%s", db_name, table_name);
          ha_tbl_list->is_temporary_table = true;
        }
      }
    }
    // add table to sql_info->tables for 'alter table rename'
    if (SQLCOM_ALTER_TABLE == sql_command && thd->lex->name.str &&
        C_STR(sdb_lex_first_select(thd)->db)) {
      ha_tbl_list = (ha_table_list *)thd_calloc(thd, sizeof(ha_table_list));
      if (ha_tbl_list == NULL) {
        rc = SDB_HA_OOM;
        goto error;
      }
      ha_tbl_list->db_name = C_STR(sdb_lex_first_select(thd)->db);
      ha_tbl_list->table_name = thd->lex->name.str;
      ha_tbl_list->op_type = HA_OPERATION_TYPE_TABLE;
      ha_tbl_list->cata_version = INVALID_CAT_VERSION;
      ha_tbl_list->is_temporary_table = false;
      ha_tbl_list->next = NULL;
      sql_info->tables->next = ha_tbl_list;
    }

    // add functions for 'create table/view as select ... fun1, fun2'
    if (SQLCOM_CREATE_TABLE == sql_command ||
        SQLCOM_CREATE_VIEW == sql_command) {
      Sroutine_hash_entry **sroutine_to_open = &thd->lex->sroutines_list.first;
      char qname_buff[NAME_LEN * 2 + 2] = {0};
      for (Sroutine_hash_entry *rt = *sroutine_to_open; rt;
           sroutine_to_open = &rt->next, rt = rt->next) {
        memset(qname_buff, 0, NAME_LEN * 2 + 2);
        sp_name name(&rt->mdl_request.key, qname_buff);

        DBUG_ASSERT(MDL_key::FUNCTION == rt->mdl_request.key.mdl_namespace());
        ha_tbl_list =
            (ha_table_list *)thd_calloc(thd, sizeof(ha_table_list) + 1);
        char *db_name = (char *)thd_calloc(thd, name.m_db.length);
        char *table_name = (char *)thd_calloc(thd, name.m_name.length);
        if (!ha_tbl_list || !db_name || !table_name) {
          rc = SDB_HA_OOM;
          goto error;
        } else {
          sprintf(db_name, "%s", name.m_db.str);
          sprintf(table_name, "%s", name.m_name.str);
          ha_tbl_list->db_name = db_name;
          ha_tbl_list->table_name = table_name;
        }
        ha_tbl_list->op_type = HA_ROUTINE_TYPE_FUNC;
        ha_tbl_list->cata_version = INVALID_CAT_VERSION;
        ha_tbl_list->is_temporary_table = false;
        ha_tbl_list->next = NULL;
        ha_tbl_list_tail->next = ha_tbl_list;
        ha_tbl_list_tail = ha_tbl_list_tail->next;
      }
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
    ha_event.general_command = (char *)event->general_command.str;
    ha_event.general_command_length = event->general_command.length;
#else
    ha_event.general_query = event->general_query;
    ha_event.general_query_length = event->general_query_length;
    ha_event.general_command = (char *)event->general_command;
    ha_event.general_command_length = event->general_command_length;
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
                                ha_event_class_t event_class) {
  bool conds = !is_trans_on;
#ifdef IS_MARIADB
  if (MYSQL_AUDIT_GENERAL_CLASS == event_class) {
    conds = conds && (MYSQL_AUDIT_QUERY_BEGIN == event.event_subclass);
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
  conds = conds && (MYSQL_AUDIT_QUERY_END == event.event_subclass ||
                    MYSQL_AUDIT_GENERAL_STATUS == event.event_subclass ||
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
int fix_create_table_stmt(THD *thd, ha_event_class_t event_class,
                          ha_event_general &event, String &query) {
  TABLE_LIST *table_list = sdb_lex_first_select(thd)->get_table_list();
  int sql_command = thd_sql_command(thd);
  int rc = 0;
  query.set_charset(system_charset_info);
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
       MYSQL_AUDIT_GENERAL_STATUS == event.event_subclass ||
       MYSQL_AUDIT_QUERY_END == event.event_subclass)) {
    if (!table_list->table && table_list->next_local) {
      // deal with SQL like "create table t1 like t2" or "create or replace
      // table like".
      TABLE_LIST tables;
      tables.init_one_table(&table_list->db, &table_list->table_name, 0,
                            TL_READ_WITH_SHARED_LOCKS);
      rc = open_and_lock_tables(thd, &tables, FALSE, MYSQL_LOCK_IGNORE_TIMEOUT);
      if (rc) {
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
  rc = SDB_HA_FIX_CREATE_TABLE;
  goto done;
}

static void sp_returns_type(THD *thd, String &result, sp_head *sp) {
  TABLE table;
  TABLE_SHARE share;
  Field *field;
  memset((char *)&table, 0, sizeof(table));
  memset((char *)&share, 0, sizeof(share));
  table.in_use = thd;
  table.s = &share;
  field = sp->create_result_field(0, 0, &table);
  field->sql_type(result);

  if (field->has_charset()) {
    result.append(STRING_WITH_LEN(" CHARSET "));
    result.append(field->charset()->csname);
    if (!(field->charset()->state & MY_CS_PRIMARY)) {
      result.append(STRING_WITH_LEN(" COLLATE "));
      result.append(field->charset()->name);
    }
  }
  delete field;
}

static int show_create_sp(THD *thd, const LEX_CSTRING &returns, String *buf) {
  int sql_command = thd_sql_command(thd);
  const sp_head *sp = thd->lex->sphead;
  LEX_CSTRING db = {NULL, 0};
  const LEX_CSTRING name = {sp->m_name.str, sp->m_name.length};
  const LEX_CSTRING params = {sp->m_params.str, sp->m_params.length};
  const LEX_CSTRING body = {sp->m_body.str, sp->m_body.length};
  const LEX_CSTRING definer_user = thd->lex->definer->user;
  const LEX_CSTRING definer_host = thd->lex->definer->host;
  sql_mode_t sql_mode = thd->variables.sql_mode;
  sql_mode_t saved_sql_mode = thd->variables.sql_mode;

  if (sp->m_explicit_name) {
    db.str = sp->m_db.str;
    db.length = sp->m_db.length;
  }
#ifdef IS_MARIADB
  const st_sp_chistics sp_chistics = sp->chistics();
  const st_sp_chistics *chistics = &sp_chistics;
  size_t agglen = (chistics->agg_type == GROUP_AGGREGATE) ? 10 : 0;
  const DDL_options_st ddl_options = thd->lex->create_info;
  if (SQLCOM_CREATE_PACKAGE == sql_command ||
      SQLCOM_CREATE_PACKAGE_BODY == sql_command) {
    bool rc = sp->m_handler->show_create_sp(
        thd, buf, db, name, params, returns, body, sp_chistics,
        *thd->lex->definer, ddl_options, sql_mode);
    return rc ? SDB_HA_OOM : 0;
  }
#else
  const st_sp_chistics *chistics = sp->m_chistics;
#endif
  LEX_CSTRING tmp;

  /* Make some room to begin with */
  if (buf->alloc(100 + db.length + 1 + name.length + params.length +
                 returns.length + body.length + chistics->comment.length +
                 10 /* length of " DEFINER= "*/ + USER_HOST_BUFF_SIZE)) {
    return SDB_HA_OOM;
  }

  thd->variables.sql_mode = sql_mode;
  buf->append(STRING_WITH_LEN("CREATE "));

#ifdef IS_MARIADB
  if (ddl_options.or_replace())
    buf->append(STRING_WITH_LEN("OR REPLACE "));
#endif
  buf->append(STRING_WITH_LEN("DEFINER="));
  append_identifier(thd, buf, definer_user.str, definer_user.length);
  buf->append('@');
  append_identifier(thd, buf, definer_host.str, definer_host.length);
  buf->append(' ');
#ifdef IS_MARIADB
  if (chistics->agg_type == GROUP_AGGREGATE)
    buf->append(STRING_WITH_LEN("AGGREGATE "));
  tmp = sp->m_handler->type_lex_cstring();
  buf->append(&tmp);
  buf->append(STRING_WITH_LEN(" "));
  if (ddl_options.if_not_exists()) {
    buf->append(STRING_WITH_LEN("IF NOT EXISTS "));
  }
#else
  if (SQLCOM_CREATE_SPFUNCTION == sql_command ||
      SQLCOM_CREATE_FUNCTION == sql_command) {
    buf->append(STRING_WITH_LEN("FUNCTION "));
  } else if (SQLCOM_CREATE_PROCEDURE == sql_command) {
    buf->append(STRING_WITH_LEN("PROCEDURE "));
  }
#endif

  if (db.length > 0) {
    append_identifier(thd, buf, db.str, db.length);
    buf->append('.');
  }
  append_identifier(thd, buf, name.str, name.length);
  buf->append('(');
  buf->append(params.str, params.length);
  buf->append(')');

  if (SQLCOM_CREATE_SPFUNCTION == sql_command ||
      SQLCOM_CREATE_FUNCTION == sql_command) {
#ifdef IS_MARIADB
    if (sql_mode & MODE_ORACLE)
      buf->append(STRING_WITH_LEN(" RETURN "));
    else
#endif
      buf->append(STRING_WITH_LEN(" RETURNS "));
    buf->append(returns.str, returns.length);
  }
  buf->append('\n');

  switch (chistics->daccess) {
    case SP_NO_SQL:
      buf->append(STRING_WITH_LEN("    NO SQL\n"));
      break;
    case SP_READS_SQL_DATA:
      buf->append(STRING_WITH_LEN("    READS SQL DATA\n"));
      break;
    case SP_MODIFIES_SQL_DATA:
      buf->append(STRING_WITH_LEN("    MODIFIES SQL DATA\n"));
      break;
    case SP_DEFAULT_ACCESS:
    case SP_CONTAINS_SQL:
      /* Do nothing */
      break;
  }
  if (chistics->detistic) {
    buf->append(STRING_WITH_LEN("    DETERMINISTIC\n"));
  }
  if (chistics->suid == SP_IS_NOT_SUID) {
    buf->append(STRING_WITH_LEN("    SQL SECURITY INVOKER\n"));
  }
  if (chistics->comment.length) {
    buf->append(STRING_WITH_LEN("    COMMENT "));
    append_unescaped(buf, chistics->comment.str, chistics->comment.length);
    buf->append('\n');
  }
  buf->append(body.str, body.length);  // Not \0 terminated
  thd->variables.sql_mode = saved_sql_mode;
  return 0;
}

static int show_create_event(THD *thd, String *buf) {
  int rc = 0;
  const LEX_CSTRING definer_user = thd->lex->definer->user;
  const LEX_CSTRING definer_host = thd->lex->definer->host;

  // make some room for current statement
  static const int MAX_EXTRA_LEN = 50;
  size_t definition_len =
      thd->lex->stmt_definition_end - thd->lex->stmt_definition_begin;
  if (buf->alloc(MAX_EXTRA_LEN + definition_len + USER_HOST_BUFF_SIZE)) {
    return SDB_HA_OOM;
  }

  // append the "CREATE" part of the query
  buf->length(0);
  buf->append(STRING_WITH_LEN("CREATE "));
#ifdef IS_MARIADB
  if (thd->lex->create_info.or_replace()) {
    buf->append(STRING_WITH_LEN("OR REPLACE "));
  }
#endif

  // append definer
  buf->append(STRING_WITH_LEN("DEFINER="));
  append_identifier(thd, buf, definer_user.str, definer_user.length);
  buf->append('@');
  append_identifier(thd, buf, definer_host.str, definer_host.length);
  buf->append(' ');

  // append the left part of thd->query after "DEFINER" part
  buf->append(thd->lex->stmt_definition_begin, definition_len);
  return 0;
}

static int show_create_trigger(THD *thd, String *buf) {
  int rc = 0;
#ifdef IS_MARIADB
  LEX_CSTRING binlog_definition;
#else
  LEX_STRING binlog_definition;
#endif
  size_t prefix_trimmed = 0;

  // make some room for current statement
  static const int MAX_EXTRA_LEN = 50;
  size_t definition_len =
      thd->lex->stmt_definition_end - thd->lex->stmt_definition_begin;
  if (buf->alloc(MAX_EXTRA_LEN + definition_len + USER_HOST_BUFF_SIZE)) {
    return SDB_HA_OOM;
  }

  // append the "CREATE" part of the query
  buf->append("CREATE ");
#ifdef IS_MARIADB
  if (thd->lex->create_info.or_replace()) {
    buf->append(STRING_WITH_LEN("OR REPLACE "));
  }
#endif

  // append definer
#ifdef IS_MARIADB
  if (thd->lex->sphead->suid() != SP_IS_NOT_SUID)
#else
  if (thd->lex->definer)
#endif
  {
    const LEX_CSTRING *definer_user = &thd->lex->definer->user;
    const LEX_CSTRING *definer_host = &thd->lex->definer->host;
    buf->append(STRING_WITH_LEN("DEFINER="));
    append_identifier(thd, buf, definer_user->str, definer_user->length);
    buf->append('@');
    append_identifier(thd, buf, definer_host->str, definer_host->length);
    buf->append(' ');
  }
  // append the left part of thd->query after "DEFINER" part
  binlog_definition.str = (char *)thd->lex->stmt_definition_begin;
  binlog_definition.length = definition_len;
  trim_whitespace(thd->charset(), &binlog_definition);
  buf->append(binlog_definition.str, binlog_definition.length);
  return 0;
}

// fix BUG-768
// convert SQL like "create procedure/event/trigger/function"
// to "create 'user@hostname' procedure/event/trigger/function"
static int fix_create_routine_stmt(THD *thd, ha_event_general &event,
                                   String &log_query) {
  int sql_command = thd_sql_command(thd);
  int rc = 0;

#ifdef IS_MARIADB
  // set character set, refer mariadb binlog format
  log_query.set_charset(thd->charset());
#endif
  if (SQLCOM_CREATE_SPFUNCTION == sql_command ||
      SQLCOM_CREATE_FUNCTION == sql_command ||
      SQLCOM_CREATE_PROCEDURE == sql_command) {
    LEX_CSTRING returns = {"", 0};
    String retstr(64);
    retstr.set_charset(system_charset_info);
    if (SQLCOM_CREATE_SPFUNCTION == sql_command ||
        SQLCOM_CREATE_FUNCTION == sql_command) {
      sp_returns_type(thd, retstr, thd->lex->sphead);
      returns = retstr.lex_cstring();
    }
    rc = show_create_sp(thd, returns, &log_query);
  } else if (SQLCOM_CREATE_EVENT == sql_command) {
    rc = show_create_event(thd, &log_query);
  } else if (SQLCOM_CREATE_TRIGGER == sql_command) {
    rc = show_create_trigger(thd, &log_query);
  }
  if (0 == rc) {
    event.general_query = log_query.c_ptr();
    event.general_query_length = log_query.length();
  }
  return rc;
}

// rebuild create view statement, append 'ALGORITHM' and 'DEFINER'
static int fix_create_view_stmt(THD *thd, ha_event_general &event,
                                String &log_query) {
  int rc = 0;
  bool oom = false;
#ifdef IS_MARIADB
  // set character set, refer mariadb binlog format
  log_query.set_charset(thd->charset());
#endif
  LEX *lex = thd->lex;
  TABLE_LIST *view = sdb_lex_first_select(thd)->get_table_list();
  static const int CHANGE_VIEW_CMD_NUM = 3;
  static const LEX_STRING command[CHANGE_VIEW_CMD_NUM] = {
      {C_STRING_WITH_LEN("CREATE ")},
      {C_STRING_WITH_LEN("ALTER ")},
      {C_STRING_WITH_LEN("CREATE OR REPLACE ")}};
#ifdef IS_MARIADB
  oom |= log_query.append(&command[thd->lex->create_view->mode]);
#else
  oom |= log_query.append(command[thd->lex->create_view_mode].str,
                          command[thd->lex->create_view_mode].length);
#endif
  view_store_options(thd, view, &log_query);
  oom |= log_query.append(STRING_WITH_LEN("VIEW "));

#ifdef IS_MARIADB
  /* Appending IF NOT EXISTS if present in the query */
  if (lex->create_info.if_not_exists()) {
    oom |= log_query.append(STRING_WITH_LEN("IF NOT EXISTS "));
  }
  /* Test if user supplied a db (ie: we did not use thd->db) */
  if (view->db.str && view->db.str[0] &&
      (thd->db.str == NULL || cmp(&view->db, &thd->db))) {
    append_identifier(thd, &log_query, &view->db);
    oom |= log_query.append('.');
  }
#else
  /* Test if user supplied a db (ie: we did not use thd->db) */
  if (view->db && view->db[0] &&
      (thd->db().str == NULL || strcmp(view->db, thd->db().str))) {
    append_identifier(thd, &log_query, C_STR(view->db), C_STR_LEN(view->db));
    oom |= log_query.append('.');
  }
#endif

  append_identifier(thd, &log_query, C_STR(view->table_name),
                    C_STR_LEN(view->table_name));
  if (lex->view_list.elements) {
#ifdef IS_MARIADB
    List_iterator_fast<LEX_CSTRING> names(lex->view_list);
    LEX_CSTRING *name;
#else
    List_iterator_fast<LEX_STRING> names(lex->view_list);
    LEX_STRING *name;
#endif
    int i;

    for (i = 0; (name = names++); i++) {
      log_query.append(i ? ", " : "(");
      append_identifier(thd, &log_query, name->str, name->length);
    }
    oom |= log_query.append(')');
  }
  oom |= log_query.append(STRING_WITH_LEN(" AS "));
  oom |= log_query.append(view->source.str, view->source.length);
  if (!oom) {
    event.general_query = log_query.c_ptr();
    event.general_query_length = log_query.length();
  }
  rc = oom ? SDB_HA_OOM : 0;
  return rc;
}

static void append_datetime(String *buf, Time_zone *time_zone, my_time_t secs,
                            const char *name, uint len) {
  char dtime_buff[20 * 2 + 32]; /* +32 to make my_snprintf_{8bit|ucs2} happy */
  buf->append(STRING_WITH_LEN(" "));
  buf->append(name, len);
  buf->append(STRING_WITH_LEN(" '"));
  /*
    Pass the buffer and the second param tells fills the buffer and
    returns the number of chars to copy.
  */
  MYSQL_TIME time;
  time_zone->gmt_sec_to_TIME(&time, secs);
  buf->append(dtime_buff, my_datetime_to_str(&time, dtime_buff, 0));
  buf->append(STRING_WITH_LEN("'"));
}

// add definer to 'alter event' statement
static int fix_alter_event_stmt(THD *thd, ha_event_general &event,
                                String &log_query, ha_sql_stmt_info *sql_info) {
#ifndef NO_EMBEDDED_ACCESS_CHECKS
  int rc = 0;
  bool oom = false;
  int sql_command = thd_sql_command(thd);
  DBUG_ASSERT(SQLCOM_ALTER_EVENT == sql_command);
  Event_parse_data *event_parse_data = thd->lex->event_parse_data;
  DBUG_ASSERT(NULL != event_parse_data);
  LEX_CSTRING *definer_user = &thd->lex->definer->user;
  LEX_CSTRING *definer_host = &thd->lex->definer->host;

#ifdef IS_MARIADB
  // set character set, refer mariadb binlog format
  log_query.set_charset(thd->charset());
#endif

  oom = log_query.append(STRING_WITH_LEN("ALTER "));

  // append definer for 'alter event' statement
  oom |= log_query.append(STRING_WITH_LEN("DEFINER="));
  append_identifier(thd, &log_query, definer_user->str, definer_user->length);
  oom |= log_query.append('@');
  append_identifier(thd, &log_query, definer_host->str, definer_host->length);
  oom |= log_query.append(' ');

  oom |= log_query.append(STRING_WITH_LEN("EVENT "));
  append_identifier(thd, &log_query, event_parse_data->name.str,
                    event_parse_data->name.length);

  // append extra options, refer to 'show create event' statement
  char tmp_buf[2 * STRING_BUFFER_USUAL_SIZE];
  String expr_buf(tmp_buf, sizeof(tmp_buf), system_charset_info);
  expr_buf.length(0);
  longlong expression = event_parse_data->expression;
  interval_type interval = event_parse_data->interval;

  if (expression && Events::reconstruct_interval_expression(&expr_buf, interval,
                                                            expression)) {
    rc = SDB_HA_EXCEPTION;
    snprintf(sql_info->err_message, HA_BUF_LEN,
             "Failed to reconstruct interval expression for 'alter event' "
             "statement");
    sql_print_error(
        "HA: Failed to reconstruct interval expression for 'alter event' "
        "statement");
    goto error;
  }

  if (event_parse_data->expression) {
    oom |= log_query.append(STRING_WITH_LEN(" ON SCHEDULE EVERY "));
    oom |= log_query.append(expr_buf);
    oom |= log_query.append(' ');
#ifdef IS_MYSQL
    LEX_STRING *ival = &interval_type_to_name[interval];
#else
    LEX_CSTRING *ival = &interval_type_to_name[interval];
#endif
    oom |= log_query.append(ival->str, ival->length);

    if (!event_parse_data->starts_null) {
      append_datetime(&log_query, thd->variables.time_zone,
                      event_parse_data->starts, STRING_WITH_LEN("STARTS"));
    }

    if (!event_parse_data->ends_null) {
      append_datetime(&log_query, thd->variables.time_zone,
                      event_parse_data->ends, STRING_WITH_LEN("ENDS"));
    }
  } else if (!event_parse_data->execute_at_null) {
    append_datetime(&log_query, thd->variables.time_zone,
                    event_parse_data->execute_at,
                    STRING_WITH_LEN("ON SCHEDULE AT"));
  }

  if (event_parse_data->on_completion == Event_parse_data::ON_COMPLETION_DROP) {
    oom |= log_query.append(STRING_WITH_LEN(" ON COMPLETION NOT PRESERVE "));
  } else {
    oom |= log_query.append(STRING_WITH_LEN(" ON COMPLETION PRESERVE "));
  }

  // append rename part
  if (thd->lex->spname) {
    oom |= log_query.append("RENAME TO ");
    append_identifier(thd, &log_query, thd->lex->spname->m_db.str,
                      thd->lex->spname->m_db.length);
    oom |= log_query.append('.');
    append_identifier(thd, &log_query, thd->lex->spname->m_name.str,
                      thd->lex->spname->m_name.length);
    oom |= log_query.append(" ");
  }

  if (event_parse_data->status == Event_parse_data::ENABLED) {
    oom |= log_query.append(STRING_WITH_LEN("ENABLE"));
  } else if (event_parse_data->status == Event_parse_data::SLAVESIDE_DISABLED) {
    oom |= log_query.append(STRING_WITH_LEN("DISABLE ON SLAVE"));
  } else {
    oom |= log_query.append(STRING_WITH_LEN("DISABLE"));
  }

  if (event_parse_data->comment.length) {
    oom |= log_query.append(STRING_WITH_LEN(" COMMENT "));
    append_unescaped(&log_query, event_parse_data->comment.str,
                     event_parse_data->comment.length);
  }

  // append event body
  if (sql_info->alter_event_body) {
    oom |= log_query.append(STRING_WITH_LEN(" DO "));
    oom |= log_query.append(sql_info->alter_event_body);
    sql_info->alter_event_body = NULL;
  }
  oom |= log_query.append(" ");
  event.general_query = log_query.c_ptr();
  event.general_query_length = log_query.length();
  rc = oom ? SDB_HA_OOM : 0;

done:
  return rc;
error:
  goto done;
#else
  return 0;
#endif
}

// check if current SQL statement can be written into sequoiadb
bool can_write_sql_log(THD *thd, ha_sql_stmt_info *sql_info, int error_code) {
  int sql_command = thd_sql_command(thd);
  bool can_write_log = true;

  if (SQLCOM_ALTER_TABLE == sql_command) {
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
  } else if (can_write_log && ER_CANNOT_USER == error_code &&
             (SQLCOM_CREATE_USER == sql_command ||
              SQLCOM_DROP_USER == sql_command
#ifdef IS_MARIADB
              || SQLCOM_CREATE_ROLE == sql_command ||
              SQLCOM_DROP_ROLE == sql_command
#endif
              )) {
    // if create/drop user/role partial success
    LEX_USER *lex_user = NULL;
    String all_users;
    all_users.length(0);
    List_iterator<LEX_USER> users_list(thd->lex->users_list);
    while ((lex_user = users_list++)) {
#ifdef IS_MYSQL
      append_user(thd, &all_users, lex_user, all_users.length() > 0, false);
#else
      if (all_users.length())
        all_users.append(',');
      append_query_string(system_charset_info, &all_users, lex_user->user.str,
                          lex_user->user.length,
                          thd->variables.sql_mode & MODE_NO_BACKSLASH_ESCAPES);
      /* hostname part is not relevant for roles, it is always empty */
      if (lex_user->user.length == 0 || lex_user->host.length != 0) {
        all_users.append('@');
        append_query_string(
            system_charset_info, &all_users, lex_user->host.str,
            lex_user->host.length,
            thd->variables.sql_mode & MODE_NO_BACKSLASH_ESCAPES);
      }
#endif
    }
#ifdef IS_MYSQL
    const char *err_msg = thd->get_stmt_da()->message_text();
#else
    const char *err_msg = thd->get_stmt_da()->message();
#endif
    if (strstr(err_msg, all_users.c_ptr_safe())) {
      can_write_log = false;
    }
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
      MYSQL_AUDIT_GENERAL_STATUS == event.event_subclass ||
      MYSQL_AUDIT_QUERY_END == event.event_subclass) {
    thd->get_stmt_da()->set_overwrite_status(status);
  }
#endif
}

// get involved objects about current SQL statement
int get_query_objects(THD *thd, ha_sql_stmt_info *sql_info) {
  DBUG_ENTER("get_query_objects");
  int rc = 0;
  int sql_command = thd_sql_command(thd);
  Sroutine_hash_entry **sroutine_to_open = &thd->lex->sroutines_list.first;
  MDL_key::enum_mdl_namespace mdl_type;
  char qname_buff[NAME_LEN * 2 + 2] = {0};
  char *db_name = NULL, *table_name = NULL, *op_type = NULL;

  ha_table_list *ha_tbl_list = NULL;
  switch (sql_command) {
    case SQLCOM_SHOW_FIELDS:
    case SQLCOM_SHOW_KEYS:
    case SQLCOM_REPLACE:
    case SQLCOM_REPLACE_SELECT:
    case SQLCOM_SELECT:
    case SQLCOM_SET_OPTION:
    case SQLCOM_CALL:
    case SQLCOM_UPDATE:
    case SQLCOM_DELETE:
    case SQLCOM_INSERT:
    case SQLCOM_DELETE_MULTI:
    case SQLCOM_UPDATE_MULTI:
    case SQLCOM_INSERT_SELECT:
    case SQLCOM_SHOW_CREATE: {
      TABLE_LIST *dml_tables = sdb_lex_first_select(thd)->get_table_list();
      if (NULL == dml_tables) {
        dml_tables = thd->lex->query_tables;
      }

      // deal with "SQLCOM_SHOW_FIELDS & SQLCOM_SHOW_KEYS"
      if ((SQLCOM_SHOW_FIELDS == sql_command ||
           SQLCOM_SHOW_KEYS == sql_command) &&
          dml_tables->schema_table_reformed) {
        dml_tables = dml_tables->schema_select_lex->get_table_list();
      }

      ha_table_list *ha_tbl_list_tail = NULL;
      ha_tbl_list = NULL;
      for (TABLE_LIST *tbl = dml_tables; tbl; tbl = tbl->next_global) {
        ha_tbl_list = (ha_table_list *)thd_calloc(thd, sizeof(ha_table_list));
        if (NULL == ha_tbl_list) {
          rc = SDB_HA_OOM;
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
        if (!sql_info->dml_tables) {
          sql_info->dml_tables = ha_tbl_list;
          ha_tbl_list_tail = ha_tbl_list;
        } else {
          ha_tbl_list_tail->next = ha_tbl_list;
          ha_tbl_list_tail = ha_tbl_list;
        }
      }
      for (Sroutine_hash_entry *rt = *sroutine_to_open; rt;
           sroutine_to_open = &rt->next, rt = rt->next) {
        mdl_type = rt->mdl_request.key.mdl_namespace();
        memset(qname_buff, 0, NAME_LEN * 2 + 2);
        sp_name name(&rt->mdl_request.key, qname_buff);
        db_name = (char *)name.m_db.str;
        table_name = (char *)name.m_name.str;

        ha_tbl_list =
            (ha_table_list *)thd_calloc(thd, sizeof(ha_table_list) + 1);
        db_name = (char *)thd_calloc(thd, name.m_db.length);
        table_name = (char *)thd_calloc(thd, name.m_name.length);
        if (!ha_tbl_list || !db_name || !table_name) {
          rc = SDB_HA_OOM;
          goto error;
        } else {
          sprintf(db_name, "%s", name.m_db.str);
          sprintf(table_name, "%s", name.m_name.str);
          ha_tbl_list->db_name = db_name;
          ha_tbl_list->table_name = table_name;
        }

        if (MDL_key::FUNCTION == mdl_type) {
          op_type = HA_ROUTINE_TYPE_FUNC;
        } else if (MDL_key::PROCEDURE == mdl_type) {
          op_type = HA_ROUTINE_TYPE_PROC;
        } else if (MDL_key::EVENT == mdl_type) {
          op_type = HA_ROUTINE_TYPE_EVENT;
        } else if (MDL_key::TRIGGER == mdl_type) {
          op_type = HA_ROUTINE_TYPE_TRIG;
        } else {
          DBUG_ASSERT(0);
        }
        ha_tbl_list->op_type = op_type;
        ha_tbl_list->is_temporary_table = false;
        ha_tbl_list->next = NULL;
        if (!sql_info->dml_tables) {
          sql_info->dml_tables = ha_tbl_list;
          ha_tbl_list_tail = ha_tbl_list;
        } else {
          ha_tbl_list_tail->next = ha_tbl_list;
          ha_tbl_list_tail = ha_tbl_list;
        }
      }
    } break;
    case SQLCOM_SHOW_CREATE_DB:
    case SQLCOM_CHANGE_DB:
    case SQLCOM_SHOW_TABLES: {
      const char *db_name = NULL;
      if (SQLCOM_SHOW_CREATE_DB == sql_command) {
        db_name = thd->lex->name.str;
      } else if (SQLCOM_CHANGE_DB == sql_command) {
        db_name = C_STR(sdb_lex_first_select(thd)->db);
      } else {
        db_name = C_STR(sdb_lex_first_select(thd)->db);
        db_name = db_name ? db_name : sdb_thd_db(thd);
      }
      DBUG_ASSERT(db_name != NULL);

      ha_tbl_list = (ha_table_list *)thd_calloc(thd, sizeof(ha_table_list));
      if (NULL == ha_tbl_list) {
        rc = SDB_HA_OOM;
        goto error;
      }

      ha_tbl_list->db_name = db_name;
      ha_tbl_list->table_name = HA_EMPTY_STRING;
      ha_tbl_list->op_type = HA_OPERATION_TYPE_DB;
      ha_tbl_list->is_temporary_table = false;
      ha_tbl_list->next = NULL;
      sql_info->dml_tables = ha_tbl_list;
    } break;
    case SQLCOM_SHOW_GRANTS:
      if (NULL == thd->lex->grant_user->user.str) {
        // deal with 'SHOW GRANTS' without user
        break;
      }
    case SQLCOM_SHOW_CREATE_USER: {
      ha_tbl_list = (ha_table_list *)thd_calloc(thd, sizeof(ha_table_list));
      if (NULL == ha_tbl_list) {
        rc = SDB_HA_OOM;
        goto error;
      }
      ha_tbl_list->db_name = HA_MYSQL_DB;
      ha_tbl_list->table_name = thd->lex->grant_user->user.str;
      ha_tbl_list->op_type = HA_OPERATION_TYPE_DCL;
      ha_tbl_list->is_temporary_table = false;
      ha_tbl_list->next = NULL;
      sql_info->dml_tables = ha_tbl_list;
    } break;
#ifdef IS_MARIADB
    case SQLCOM_SHOW_CREATE_PACKAGE:
    case SQLCOM_SHOW_CREATE_PACKAGE_BODY:
    case SQLCOM_SHOW_PACKAGE_BODY_CODE:
#endif
    case SQLCOM_SHOW_PROC_CODE:
    case SQLCOM_SHOW_FUNC_CODE:
    case SQLCOM_SHOW_CREATE_PROC:
    case SQLCOM_SHOW_CREATE_FUNC:
    case SQLCOM_SHOW_CREATE_EVENT:
    case SQLCOM_SHOW_CREATE_TRIGGER: {
      ha_tbl_list = (ha_table_list *)thd_calloc(thd, sizeof(ha_table_list));
      if (NULL == ha_tbl_list) {
        rc = SDB_HA_OOM;
        goto error;
      }
      ha_tbl_list->db_name = thd->lex->spname->m_db.str;
      ha_tbl_list->table_name = thd->lex->spname->m_name.str;
      ha_tbl_list->is_temporary_table = false;
      ha_tbl_list->next = NULL;
#ifdef IS_MARIADB
      if (SQLCOM_SHOW_CREATE_PACKAGE == sql_command ||
          SQLCOM_SHOW_CREATE_PACKAGE_BODY == sql_command ||
          SQLCOM_SHOW_PACKAGE_BODY_CODE == sql_command) {
        ha_tbl_list->op_type = HA_ROUTINE_TYPE_PACKAGE;
      }
#endif
      if (SQLCOM_SHOW_CREATE_PROC == sql_command ||
          SQLCOM_SHOW_PROC_CODE == sql_command) {
        ha_tbl_list->op_type = HA_ROUTINE_TYPE_PROC;
      } else if (SQLCOM_SHOW_CREATE_FUNC == sql_command ||
                 SQLCOM_SHOW_FUNC_CODE == sql_command) {
        ha_tbl_list->op_type = HA_ROUTINE_TYPE_FUNC;
      } else if (SQLCOM_SHOW_CREATE_EVENT == sql_command) {
        ha_tbl_list->op_type = HA_ROUTINE_TYPE_EVENT;
      } else if (SQLCOM_SHOW_CREATE_TRIGGER == sql_command) {
        ha_tbl_list->op_type = HA_ROUTINE_TYPE_TRIG;
      } else {
        DBUG_ASSERT(0);
      }
      sql_info->dml_tables = ha_tbl_list;
    } break;
  }
done:
  DBUG_RETURN(rc);
error:
  goto done;
}

inline static bool need_retry_stmt(THD *thd) {
  int sql_command = thd_sql_command(thd);
  bool need_retry = false;
  switch (sql_command) {
    case SQLCOM_REPLACE:
    case SQLCOM_REPLACE_SELECT:
    case SQLCOM_SELECT:
    case SQLCOM_UPDATE:
    case SQLCOM_INSERT:
    case SQLCOM_INSERT_SELECT:
    case SQLCOM_DELETE:
    case SQLCOM_DELETE_MULTI:
    case SQLCOM_UPDATE_MULTI:
    case SQLCOM_TRUNCATE:
    case SQLCOM_SET_OPTION:
    case SQLCOM_SHOW_TABLES:
      need_retry = true;
      break;
  }
  return need_retry;
}

static inline bool query_entry(ha_event_class_t event_class,
                               unsigned int sub_event_class) {
#ifdef IS_MYSQL
  return MYSQL_AUDIT_QUERY_CLASS == event_class &&
         MYSQL_AUDIT_QUERY_START == sub_event_class;
#else
  return MYSQL_AUDIT_GENERAL_CLASS == event_class &&
         MYSQL_AUDIT_QUERY_BEGIN == sub_event_class;
#endif
}

static inline bool sub_stmt_entry(ha_event_class_t event_class,
                                  unsigned int sub_event_class) {
#ifdef IS_MYSQL
  return MYSQL_AUDIT_QUERY_CLASS == event_class &&
         MYSQL_AUDIT_QUERY_NESTED_START == sub_event_class;
#else
  return MYSQL_AUDIT_GENERAL_CLASS == event_class &&
         MYSQL_AUDIT_QUERY_BEGIN == sub_event_class;
#endif
}

// wait objects updated to latest states
static int wait_query_objects_updated_to_latest(THD *thd,
                                                ha_sql_stmt_info *sql_info) {
  int rc = 0;
  const char *db_name = NULL, *table_name = NULL, *op_type = NULL;
  Sdb_conn *sdb_conn = sql_info->sdb_conn;
  MDL_key::enum_mdl_namespace mdl_type;
  char qname_buff[NAME_LEN * 2 + 2] = {0};
  char cached_record_key[HA_MAX_CACHED_RECORD_KEY_LEN] = {0};

  SDB_LOG_DEBUG("HA: Deal with DML, table_list: %p", sql_info->dml_tables);

  // check and lock metadata for involved objects
  Sdb_cl obj_state_cl;
  rc =
      ha_get_object_state_cl(*sdb_conn, ha_thread.sdb_group_name, obj_state_cl);
  if (rc) {
    ha_error_string(*sdb_conn, rc, sql_info->err_message);
    goto error;
  }
  // check instance state for each table
  for (ha_table_list *ha_tbl = sql_info->dml_tables; ha_tbl;
       ha_tbl = ha_tbl->next) {
    db_name = ha_tbl->db_name;
    table_name = HA_EMPTY_STRING;
    op_type = HA_OPERATION_TYPE_DB;
    snprintf(cached_record_key, HA_MAX_CACHED_RECORD_KEY_LEN, "%s-%s-%s",
             db_name, table_name, op_type);
    ha_cached_record *cached_record =
        get_cached_record(sql_info->dml_checked_objects, cached_record_key);
    if (!cached_record) {
      rc = update_cached_record(sql_info->dml_checked_objects,
                                PSI_INSTRUMENT_ME, cached_record_key, 0, 0);
      if (rc) {
        goto error;
      }
      // wait database updated to latest state
      rc = wait_object_updated_to_lastest(db_name, table_name, op_type,
                                          obj_state_cl, sql_info, thd);
      if (rc) {
        goto error;
      }
    }

    // wait objects updated to latest state, ignore temporary table
    table_name = ha_tbl->table_name;
    op_type = ha_tbl->op_type;
    if (0 == strcmp(op_type, HA_OPERATION_TYPE_TABLE) &&
        is_temporary_table(thd, db_name, table_name)) {
      continue;
    }

    snprintf(cached_record_key, HA_MAX_CACHED_RECORD_KEY_LEN, "%s-%s-%s",
             db_name, table_name, op_type);
    cached_record =
        get_cached_record(sql_info->dml_checked_objects, cached_record_key);
    if (!cached_record) {
      rc = update_cached_record(sql_info->dml_checked_objects,
                                PSI_INSTRUMENT_ME, cached_record_key, 0, 0);
      if (rc) {
        goto error;
      }
      rc = wait_object_updated_to_lastest(db_name, table_name, op_type,
                                          obj_state_cl, sql_info, thd);
      if (rc) {
        goto error;
      }
    } else {
      SDB_LOG_DEBUG("HA: Current object: %s already checked",
                    cached_record->key);
      continue;
    }
  }
done:
  return rc;
error:
  goto done;
}

static inline bool need_retry_errno(uint mysql_errno) {
  bool need_retry = false;
  switch (mysql_errno) {
    case ER_NO_SUCH_TABLE:
    case ER_BAD_DB_ERROR:
    case ER_WRONG_VALUE_COUNT_ON_ROW:
    case WARN_DATA_TRUNCATED:
    case ER_BAD_FIELD_ERROR:
    case ER_DUP_KEY:
    case ER_DUP_ENTRY:
    case ER_SP_DOES_NOT_EXIST:
    case ER_EVENT_DOES_NOT_EXIST:
    case ER_TRG_DOES_NOT_EXIST:
    case ER_CANNOT_USER:
      need_retry = true;
  }

  if (SDB_DMS_NOTEXIST == get_sdb_code(mysql_errno)) {
    need_retry = true;
  }
  return need_retry;
}

static int wait_latest_state_before_query(THD *thd, ha_sql_stmt_info *sql_info,
                                          unsigned int event_class,
                                          ha_event_general &event);

static void set_retry_flags(THD *thd, ha_sql_stmt_info *sql_info) {
  DBUG_ENTER("set_retry_flag");
  uint mysql_errno = sdb_sql_errno(thd);
  bool version_error = false;

  Sroutine_hash_entry *sroutine_to_open = thd->lex->sroutines_list.first;
  static const int MAX_ERR_LEN = 30;
  char CL_VERSION_ERR[MAX_ERR_LEN] = {0};

  if (ER_GET_ERRNO == mysql_errno) {
    int engine_error_code = SDB_CLIENT_CATA_VER_OLD;
    convert_sdb_code(engine_error_code);
    snprintf(CL_VERSION_ERR, MAX_ERR_LEN, "Got error %d", engine_error_code);
  } else {
    snprintf(CL_VERSION_ERR, MAX_ERR_LEN, "sdb client cata version old");
  }
  const char *cl_version_err = sdb_errno_message(thd);
  // sequoiadb error 'sdb client cata version old' or 'Got error XXX'
  if (0 == strncmp(CL_VERSION_ERR, cl_version_err, strlen(CL_VERSION_ERR))) {
    version_error = true;
    DBUG_ASSERT(!sql_info->is_result_set_started);
    SDB_LOG_DEBUG("HA: Set result set started flag to true for %s",
                  sdb_thd_query(thd));
    sql_info->is_result_set_started = true;
  }

  if (need_retry_stmt(thd) &&
      (need_retry_errno(mysql_errno) || version_error) &&
      NULL == sroutine_to_open) {
    SDB_LOG_DEBUG(
        "HA: Receive set retry flag message with error code: %d, "
        "set retry flag for current statement",
        mysql_errno);
    DBUG_PRINT("info", ("HA: Receive set retry flag message with error "
                        "code: %d, set retry flag for current statement",
                        mysql_errno));
    thd->get_stmt_da()->reset_diagnostics_area();
    sql_info->dml_retry_flag = true;
  }
  DBUG_VOID_RETURN;
}

static void reset_retry_flags(ha_sql_stmt_info *sql_info) {
  if (sql_info->dml_retry_flag) {
    SDB_LOG_DEBUG("HA: Reset retry flag");
    sql_info->dml_retry_flag = false;
  }
  if (sql_info->is_result_set_started) {
    SDB_LOG_DEBUG("HA: Reset result set flag to false");
    sql_info->is_result_set_started = false;
  }
  if (sql_info->last_instr_lex) {
    SDB_LOG_DEBUG("HA: Reset last instruction lex to NULL");
    sql_info->last_instr_lex = NULL;
  }
}

// can't handle 'show tables & show database' yet
static inline bool need_pre_check_stmt(int sql_command) {
  bool need_check_first = false;
  switch (sql_command) {
    case SQLCOM_SHOW_CREATE_USER:
    case SQLCOM_SHOW_CREATE:
    case SQLCOM_SHOW_CREATE_DB:
    case SQLCOM_SHOW_CREATE_PROC:
    case SQLCOM_SHOW_CREATE_FUNC:
    case SQLCOM_SHOW_CREATE_EVENT:
    case SQLCOM_SHOW_CREATE_TRIGGER:
    case SQLCOM_SHOW_GRANTS:
    case SQLCOM_SHOW_FIELDS:
    case SQLCOM_SHOW_KEYS:
    case SQLCOM_SHOW_PROC_CODE:
    case SQLCOM_SHOW_FUNC_CODE:
    case SQLCOM_CALL:
    case SQLCOM_CHANGE_DB:
#ifdef IS_MARIADB
    case SQLCOM_SHOW_CREATE_PACKAGE:
    case SQLCOM_SHOW_CREATE_PACKAGE_BODY:
    case SQLCOM_SHOW_PACKAGE_BODY_CODE:
#endif
      need_check_first = true;
  }
  return need_check_first;
}

// wait query objects updated to latest state before query operation
static int wait_latest_state_before_query(THD *thd, ha_sql_stmt_info *sql_info,
                                          unsigned int event_class,
                                          ha_event_general &event) {
  int rc = 0;
  DBUG_ASSERT(NULL == sql_info->dml_tables);
  Sroutine_hash_entry *sroutine_to_open = thd->lex->sroutines_list.first;
  int sql_command = thd_sql_command(thd);

  // wait objects updated to latest state in following situations:
  // 1. dml_retry_flag is set
  // 2. routines are not NULL
  if (!sql_info->dml_retry_flag && NULL == sroutine_to_open) {
    if (!need_pre_check_stmt(sql_command)) {
      goto done;
    }
    SDB_LOG_DEBUG(
        "HA: Statement contains 'SHOW', wait objects updated to latest state");
  } else if (sql_info->dml_retry_flag) {
    const char *query = thd->in_sub_stmt ? "sub-query" : sdb_thd_query(thd);
    SDB_LOG_DEBUG("HA: DML retry flag is set, retry current statement");
    SDB_LOG_DEBUG("HA: Retry DML query: %s, thread: %p", query, thd);
  } else if (NULL != sroutine_to_open) {
    const char *query = thd->in_sub_stmt ? "sub-query" : sdb_thd_query(thd);
    SDB_LOG_DEBUG(
        "HA: Current query '%s' contains routines, "
        "wait objects updated to latest state first",
        query);
  }

  rc = update_sql_stmt_info(sql_info, event.general_thread_id);
  if (rc) {
    goto error;
  }

  // get objects for current query
  rc = get_query_objects(thd, sql_info);
  if (rc) {
    goto error;
  }

  // wait objects updated to latest state
  rc = wait_query_objects_updated_to_latest(thd, sql_info);
  if (rc) {
    goto error;
  }
done:
  sql_info->dml_tables = NULL;
  return rc;
error:
  goto done;
}

static void handle_error(int error, ha_sql_stmt_info *sql_info) {
  switch (error) {
    case SDB_HA_OOM:
      my_printf_error(SDB_HA_OOM, "HA: Out of memory while persisting SQL log",
                      MYF(0));
      sql_print_error("HA: Out of memory while persisting SQL log");
      break;
    case SDB_HA_ABORT_BY_USER:
      my_printf_error(SDB_HA_ABORT_BY_USER,
                      "HA: Query has been aborted by user", MYF(0));
      break;
    case SDB_HA_WAIT_TIMEOUT:
      my_printf_error(SDB_HA_WAIT_TIMEOUT,
                      "HA: There are SQL statements on releated objects have "
                      "not been executed by replay thread. Please try it later",
                      MYF(0));
      break;
    case SDB_HA_FIX_CREATE_TABLE:
      my_printf_error(SDB_HA_FIX_CREATE_TABLE,
                      "HA: Failed to modify table creation statement. Please "
                      "check mysql error log",
                      MYF(0));
      break;
    case SDB_HA_EXCEPTION:
    default:
      my_printf_error(SDB_HA_EXCEPTION, "HA: %s", MYF(0),
                      sql_info->err_message);
      break;
  }
}

// entry of audit plugin
static int persist_sql_stmt(THD *thd, ha_event_class_t event_class,
                            const void *ev) {
  DBUG_ENTER("persist_sql_stmt");
  DBUG_PRINT("info", ("event: %d, subevent: %d", (int)event_class, *(int *)ev));
  int rc = 0, sql_command = SQLCOM_END;
  bool is_trans_on = false, need_kill_mysqld = false;
  String create_query;
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

  if ((rc = get_sql_stmt_info(&sql_info))) {
    goto error;
  }
  if ((rc = update_sql_stmt_info(sql_info, event.general_thread_id))) {
    goto error;
  }

  // set or reset retry flag for current statement
  if (MYSQL_AUDIT_GENERAL_CLASS == event_class &&
      MYSQL_AUDIT_GENERAL_LOG == event.event_subclass) {
    if (0 == strcmp(event.general_command, HA_SET_RETRY_FLAG) &&
        !sdb_execute_only_in_mysql(thd)) {
      LEX *saved_lex = thd->lex;

      // restore lex of last executed statement in routines
      if (sql_info->last_instr_lex) {
        thd->lex = sql_info->last_instr_lex;
      }

      set_retry_flags(thd, sql_info);
      // for sql like "set m:= select " including tables, in routines,
      // 'mysql_execute_command' will not be called, so query objects
      // need to be checked before next retry.
      if (SQLCOM_SET_OPTION == thd->lex->sql_command &&
          NULL == thd->lex->sroutines_list.first) {
        rc = wait_latest_state_before_query(thd, sql_info, event_class, event);
        if (rc) {
          goto error;
        }
      }

      // restore current lex
      if (sql_info->last_instr_lex) {
        thd->lex = saved_lex;
      }
      goto done;
    } else if (0 == strcmp(event.general_command, HA_RESET_RETRY_FLAG)) {
      reset_retry_flags(sql_info);
      goto done;
    } else if (0 == strcmp(event.general_command, HA_SAVE_INSTR_LEX)) {
      // create procedure p1()    has lex1
      // begin
      //   select * from t1;      has lex2
      //   select * from t2;      has lex3
      // end|
      // for the above query, thd->lex will be set to lex1 after execution
      // of 'select * from t1;'. But if 'select * from t1;' need to be
      // retried, and we can't use lex1 to check if 'select * from t1' need
      // to be retried, last_instr_lex used to saved lex2.
      uint sql_err = sdb_sql_errno(thd);
      if (need_retry_errno(sql_err) && sql_info->last_instr_lex != thd->lex) {
        SDB_LOG_DEBUG("HA: Save last instruction lex");
        sql_info->last_instr_lex = thd->lex;
      }
      goto done;
    } else if (0 == strcmp(event.general_command, HA_PRE_CHECK_OBJECTS)) {
      rc = wait_latest_state_before_query(thd, sql_info, event_class, event);
      if (rc) {
        goto error;
      }
      goto done;
    } else if (0 == strcmp(event.general_command, HA_RESET_CHECKED_OBJECTS)) {
      ulong records = sql_info->dml_checked_objects.records;
      if (records) {
        SDB_LOG_DEBUG("HA: Cached %ld records, clear cached checked objects",
                      records);
        my_hash_reset(&sql_info->dml_checked_objects);
      }
      goto done;
    }
  }

  // destroy sequoiadb connection at end of client session
  if (sql_info->inited && MYSQL_AUDIT_CONNECTION_CLASS == event_class &&
      MYSQL_AUDIT_CONNECTION_DISCONNECT == event.event_subclass) {
    if (sql_info->sdb_conn) {
      delete sql_info->sdb_conn;
      sql_info->sdb_conn = NULL;
      my_hash_reset(&sql_info->dml_checked_objects);
      my_hash_free(&sql_info->dml_checked_objects);
      SDB_LOG_DEBUG("HA: Destroy sequoiadb connection");
    }
    goto done;
  }

  // if 'sequoiadb_execute_only_in_mysql' has been set, goto done
  // 1. execute query from 'HA' thread
  if (sdb_execute_only_in_mysql(thd)) {
    goto done;
  }

  // 1. Wait objects updated to latest state if necessary.
  // 2. In mysql, event is executed by creating a procedure, execution of
  //    event can't be written into sequoiadb
  if (!is_meta_sql(thd, event) || is_event_dispatch_execution(thd)) {
    // wait query objects updated to latest state by HA thread
    if ((need_retry_stmt(thd) || need_pre_check_stmt(sql_command)) &&
        (query_entry(event_class, event.event_subclass) ||
         sub_stmt_entry(event_class, event.event_subclass))) {
      rc = wait_latest_state_before_query(thd, sql_info, event_class, event);
      if (rc) {
        goto error;
      }
    }
    goto done;
  } else if (!ha_thread.recover_finished) {  // current instance is recovering
    struct timespec abstime;
    sdb_set_timespec(abstime, ha_wait_recover_timeout);

    SDB_LOG_INFO("HA: Waiting for completion of recover process");
    // other clients must wait for completion of recover process. It's not
    // convient for automated testing if report error here.
    mysql_mutex_lock(&ha_thread.recover_finished_mutex);
    rc = mysql_cond_timedwait(&ha_thread.recover_finished_cond,
                              &ha_thread.recover_finished_mutex, &abstime);
    mysql_mutex_unlock(&ha_thread.recover_finished_mutex);
    if (rc) {
      rc = SDB_HA_EXCEPTION;
      sql_print_error(
          "HA: Instance is in recovering state. Current query "
          "can't be executed. Please try it later");
      snprintf(sql_info->err_message, HA_BUF_LEN,
               "Instance is in recovering state. Current query can't "
               "be executed. Please try it later");
      goto error;
    }
    SDB_LOG_DEBUG("HA: Metadata recover finished, start execute: %s",
                  sdb_thd_query(thd));
    if (thd_killed(thd) || ha_thread.stopped) {
      rc = SDB_HA_ABORT_BY_USER;
      goto error;
    }
  }

  // in mariadb, THD::Diagnostics_area::m_can_overwrite_status must
  // set to true for using 'my_printf_error' when event_class is
  // MYSQL_AUDIT_GENERAL_STATUS or  MYSQL_AUDIT_GENERAL_RESULT
  set_overwrite_status(thd, event, true);

  // update SQL statement info according to ev
  if ((rc = update_sql_stmt_info(sql_info, event.general_thread_id))) {
    goto error;
  }

  is_trans_on = sql_info->sdb_conn->is_transaction_on();
  if (need_prepare(event, is_trans_on, event_class)) {
    DBUG_ASSERT(sql_info->tables == NULL);
    try {
      SDB_LOG_DEBUG("HA: At the beginning of persisting SQL: %s, thread: %p",
                    sdb_thd_query(thd), thd);
      SDB_LOG_DEBUG("HA: SQL command: %d, event: %d, subevent: %d, thread: %p",
                    sql_command, event_class, *((int *)ev), thd);
      SDB_LOG_DEBUG("HA: Start transaction for persisting SQL log");

      rc = sql_info->sdb_conn->begin_transaction(ISO_READ_STABILITY);
      if (rc) {
        ha_error_string(*(sql_info->sdb_conn), rc, sql_info->err_message);
        SDB_LOG_ERROR("HA: Failed to start transaction, sequoiadb error: %s",
                      sql_info->err_message);
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
      rc = SDB_HA_OOM;
      goto error;
    } catch (std::exception &e) {
      rc = SDB_HA_EXCEPTION;
      snprintf(sql_info->err_message, HA_BUF_LEN, "Unexpected error: %s",
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
        if (SQLCOM_CREATE_TABLE == sql_command) {
          rc = fix_create_table_stmt(thd, event_class, event, create_query);
        }
        // rebuild 'create function/procedure/trigger/event' statement
        if (0 == rc && (SQLCOM_CREATE_SPFUNCTION == sql_command ||
                        SQLCOM_CREATE_FUNCTION == sql_command ||
                        SQLCOM_CREATE_PROCEDURE == sql_command ||
                        SQLCOM_CREATE_EVENT == sql_command ||
                        SQLCOM_CREATE_TRIGGER == sql_command)) {
          rc = fix_create_routine_stmt(thd, event, create_query);
        }
#ifdef IS_MARIADB
        if (SQLCOM_CREATE_PACKAGE == sql_command ||
            SQLCOM_CREATE_PACKAGE_BODY == sql_command) {
          LEX_CSTRING returns = {"", 0};
          rc = show_create_sp(thd, returns, &create_query);
          if (0 == rc) {
            event.general_query = create_query.c_ptr();
            event.general_query_length = create_query.length();
          }
        }
#endif
        // rebuild 'create view' statement
        if (0 == rc && SQLCOM_CREATE_VIEW == sql_command) {
          rc = fix_create_view_stmt(thd, event, create_query);
        }
        // rebuild 'alter event' statement
        if (0 == rc && SQLCOM_ALTER_EVENT == sql_command) {
          rc = fix_alter_event_stmt(thd, event, create_query, sql_info);
        }

        // update temporary table flags for 'rename table' command
        if (SQLCOM_RENAME_TABLE == sql_command) {
          ha_table_list *ha_tables = sql_info->tables;
          for (TABLE_LIST *tables = thd->lex->query_tables; tables;
               tables = tables->next_global) {
            ha_tables->is_temporary_table = is_temporary_table(tables);
            ha_tables = ha_tables->next;
          }
        }

        // write sql_logs
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
        SDB_LOG_DEBUG("HA: Current statement can't be persisted to SQL log");
        sql_info->tables = NULL;
        rc = sql_info->sdb_conn->rollback_transaction();
        if (rc) {
          ha_error_string(*sql_info->sdb_conn, rc, sql_info->err_message);
          goto error;
        }
      }
    } catch (std::bad_alloc &e) {
      rc = SDB_HA_OOM;
      need_kill_mysqld = true;
      goto error;
    } catch (std::exception &e) {
      rc = SDB_HA_EXCEPTION;
      need_kill_mysqld = true;
      snprintf(sql_info->err_message, HA_BUF_LEN, "Unexpected error: %s",
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
    SDB_LOG_ERROR(
        "HA: Failed to persist SQL statement, start stop current server");
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

// get cata version from SQL instance
int ha_get_cata_version(const char *db_name, const char *table_name) {
  int rc = 0;
  int version = 0;

  if (ha_inst_group_name && 0 != strlen(ha_inst_group_name)) {
    ha_cached_record *cached_record = NULL;
    char cached_record_key[HA_MAX_CACHED_RECORD_KEY_LEN] = {0};
    snprintf(cached_record_key, HA_MAX_CACHED_RECORD_KEY_LEN, "%s-%s-%s",
             db_name, table_name, HA_OPERATION_TYPE_TABLE);
    cached_record = ha_get_cached_record(cached_record_key);
    if (cached_record) {
      version = cached_record->cata_version;
    } else {
      // can't find instance state in cache, maybe its original table before
      // upgrading to instance group HA, set version to 0
    }
  }
  return version;
}

// for special DDL statement 'create table as select '
int ha_get_latest_cata_version(const char *db_name, const char *table_name,
                               int &version) {
  ha_sql_stmt_info *sql_info = NULL;
  int rc = get_sql_stmt_info(&sql_info);
  DBUG_ASSERT(0 == rc && sql_info != NULL);

  if (sql_info->tables && sql_info->tables->cata_version > version &&
      0 == strcmp(db_name, sql_info->tables->db_name) &&
      0 == strcmp(table_name, sql_info->tables->table_name) &&
      0 == strcmp(HA_OPERATION_TYPE_TABLE, sql_info->tables->op_type)) {
    version = sql_info->tables->cata_version;
  }
  return rc;
}

// save cata version for a single table, this is only for DDL operaion
// in 'write_sql_log_and_states', cata version will be written into
// 'HASQLLog', 'HAObjectState' and 'HAInstanceObjectState'
void ha_set_cata_version(const char *db_name, const char *table_name,
                         int version) {
  if (ha_inst_group_name && 0 != strlen(ha_inst_group_name)) {
    if (sdb_is_tmp_table(NULL, table_name)) {
      return;
    }
    ha_sql_stmt_info *sql_info = NULL;
    int rc = get_sql_stmt_info(&sql_info);
    DBUG_ASSERT(0 == rc && sql_info != NULL);

    for (ha_table_list *tables = sql_info->tables; tables;
         tables = tables->next) {
      if (0 == strcmp(tables->op_type, HA_OPERATION_TYPE_TABLE) &&
          0 == strcmp(tables->db_name, db_name) &&
          0 == strcmp(tables->table_name, table_name)) {
        tables->cata_version = version;
        break;
      }
    }
    SDB_LOG_DEBUG("HA: Set cata version %d for table: %s-%s", version, db_name,
                  table_name);
  }
}

static int write_empty_sql_log_for_object(
    const char *db_name, const char *table_name, const char *op_type,
    int driver_cata_version, Sdb_cl &sql_log_cl, Sdb_cl &obj_state_cl,
    Sdb_cl &inst_obj_state_cl) {
  int rc = 0;
  DBUG_ENTER("write_object_empty_log");
  bool write_empty_log = false;
  bson::BSONObj obj, cond, hint, result;
  THD *thd = current_thd;
  char cached_record_key[NAME_LEN * 2 + 20] = {0};
  ha_sql_stmt_info *sql_info = NULL;

  cond = BSON(HA_FIELD_DB << db_name << HA_FIELD_TABLE << table_name
                          << HA_FIELD_TYPE << op_type);
  rc = obj_state_cl.query(cond);
  if (rc && HA_ERR_END_OF_FILE != rc) {
    goto error;
  }
  rc = obj_state_cl.next(obj, false);
  if (rc && HA_ERR_END_OF_FILE != rc) {
    goto error;
  }

  // rc is 0 or HA_ERR_END_OF_FILE
  if (HA_ERR_END_OF_FILE == rc) {
    // can't find object state for current table
    write_empty_log = true;
  } else if (0 == strcmp(op_type, HA_OPERATION_TYPE_DB)) {
    // if database SQL Log state exists
    goto done;
  } else {
    // if driver cata version is greater than cata version in 'HAObjectState'
    // write an empty SQL log, and set 'CataVersion' to driver cata version
    int object_cata_version = obj.getIntField(HA_FIELD_CAT_VERSION);
    write_empty_log = (driver_cata_version > object_cata_version);
    if (write_empty_log) {
      SDB_LOG_DEBUG(
          "HA: Write an empty log for '%s.%s', new cata version is %d", db_name,
          table_name, driver_cata_version);
    }
  }

  rc = get_sql_stmt_info(&sql_info);
  if (rc) {
    goto error;
  }

  if (write_empty_log) {
    int sql_id = 0;
    int charset_num = my_charset_utf8mb4_bin.number;

    // wait object state updated to lastest by replay thread
    rc = wait_object_updated_to_lastest(db_name, table_name, op_type,
                                        obj_state_cl, sql_info, thd);
    if (rc) {
      goto error;
    }

    obj = BSON(HA_FIELD_DB << db_name << HA_FIELD_TABLE << table_name
                           << HA_FIELD_TYPE << op_type << HA_FIELD_SQL
                           << HA_EMPTY_STRING << HA_FIELD_OWNER
                           << ha_thread.instance_id << HA_FIELD_SESSION_ATTRS
                           << "" << HA_FIELD_CLIENT_CHARSET_NUM << charset_num
                           << HA_FIELD_CAT_VERSION << driver_cata_version);
    rc = sql_log_cl.insert(obj, hint, 0, &result);
    if (rc) {
      goto error;
    }
    sql_id = result.getIntField(SDB_FIELD_LAST_GEN_ID);
    DBUG_ASSERT(sql_id > 0);
    cond = BSON(HA_FIELD_DB << db_name << HA_FIELD_TABLE << table_name
                            << HA_FIELD_TYPE << op_type);
    obj = BSON("$set" << BSON(HA_FIELD_SQL_ID << sql_id << HA_FIELD_CAT_VERSION
                                              << driver_cata_version));
    rc = obj_state_cl.upsert(obj, cond);
    if (rc) {
      goto error;
    }

    // write 'HAInstanceObjectState'
    cond = BSON(HA_FIELD_INSTANCE_ID << ha_thread.instance_id << HA_FIELD_DB
                                     << db_name << HA_FIELD_TABLE << table_name
                                     << HA_FIELD_TYPE << op_type);
    obj = BSON("$set" << BSON(HA_FIELD_SQL_ID << sql_id << HA_FIELD_CAT_VERSION
                                              << driver_cata_version));
    rc = inst_obj_state_cl.upsert(obj, cond);
    if (rc) {
      goto error;
    }

    // update cached instance state
    snprintf(cached_record_key, NAME_LEN * 2 + 20, "%s-%s-%s", db_name,
             table_name, op_type);
    rc =
        ha_update_cached_record(cached_record_key, sql_id, driver_cata_version);
  }
done:
  DBUG_RETURN(rc);
error:
  goto done;
}

// write an empty SQL log for tables before sequoiadb-sql-3.4.2
// and background split operation
int ha_write_empty_sql_log(const char *db_name, const char *table_name,
                           int driver_cata_version) {
  int rc = 0;
  DBUG_ENTER("write_empty_sql_log");

  ha_sql_stmt_info *sql_info = NULL;
  Sdb_conn *sdb_conn = NULL;
  Sdb_cl sql_log_cl, obj_state_cl, inst_obj_state_cl, lock_cl;
  rc = get_sql_stmt_info(&sql_info);
  if (rc) {
    goto error;
  }

  try {
    sdb_conn = sql_info->sdb_conn;
    if (sdb_conn->is_transaction_on()) {
      goto done;
    }

    rc = ha_get_lock_cl(*sdb_conn, ha_thread.sdb_group_name, lock_cl);
    if (rc) {
      ha_error_string(*sdb_conn, rc, sql_info->err_message);
      goto error;
    }

    rc = sql_info->sdb_conn->begin_transaction(ISO_READ_STABILITY);
    if (rc) {
      goto error;
    }

    // add lock for database
    rc = add_slock(lock_cl, db_name, HA_EMPTY_STRING, HA_OPERATION_TYPE_DB,
                   sql_info);
    if (HA_ERR_END_OF_FILE == rc) {
      SDB_LOG_DEBUG("HA: Failed to add 'S' lock, add 'X' lock for '%s:%s'",
                    db_name, HA_EMPTY_STRING);
      rc = add_xlock(lock_cl, db_name, HA_EMPTY_STRING, HA_OPERATION_TYPE_DB,
                     sql_info);
    }
    if (rc) {
      goto error;
    }

    // add lock for table
    rc = add_xlock(lock_cl, db_name, table_name, HA_OPERATION_TYPE_TABLE,
                   sql_info);
    if (rc) {
      goto error;
    }

    rc = ha_get_object_state_cl(*sql_info->sdb_conn, ha_thread.sdb_group_name,
                                obj_state_cl);
    if (rc) {
      ha_error_string(*sdb_conn, rc, sql_info->err_message);
      goto error;
    }

    rc = sdb_conn->get_cl(ha_thread.sdb_group_name, HA_SQL_LOG_CL, sql_log_cl);
    if (rc) {
      ha_error_string(*sdb_conn, rc, sql_info->err_message);
      goto error;
    }

    rc = ha_get_instance_object_state_cl(*sdb_conn, ha_thread.sdb_group_name,
                                         inst_obj_state_cl);
    if (rc) {
      ha_error_string(*sdb_conn, rc, sql_info->err_message);
      goto error;
    }

    rc = write_empty_sql_log_for_object(db_name, HA_EMPTY_STRING,
                                        HA_OPERATION_TYPE_DB, 0, sql_log_cl,
                                        obj_state_cl, inst_obj_state_cl);
    if (rc) {
      goto error;
    }
    rc = write_empty_sql_log_for_object(
        db_name, table_name, HA_OPERATION_TYPE_TABLE, driver_cata_version,
        sql_log_cl, obj_state_cl, inst_obj_state_cl);
    if (rc) {
      goto error;
    }

    rc = sql_info->sdb_conn->commit_transaction();
    if (rc) {
      goto error;
    }
  } catch (std::bad_alloc &e) {
    rc = SDB_ERR_OOM;
    SDB_LOG_ERROR("Failed to write empty SQL log for table:%s.%s, exception:%s",
                  db_name, table_name, e.what());
    convert_sdb_code(rc);
    goto error;
  } catch (std::exception &e) {
    SDB_LOG_ERROR("Failed to write empty SQL log for table:%s.%s, exception:%s",
                  db_name, table_name, e.what());
    rc = SDB_ERR_BUILD_BSON;
    convert_sdb_code(rc);
    goto error;
  }
done:
  DBUG_RETURN(rc);
error:
  ha_error_string(*sdb_conn, rc, sql_info->err_message);
  sql_info->sdb_conn->rollback_transaction();
  goto done;
}

static int init_inst_state_cache() {
  for (int i = 0; i < HA_MAX_CATA_VERSION_CACHES; i++) {
    native_rw_init(&ha_thread.inst_state_caches[i].rw_lock);
    if (sdb_hash_init(
            &ha_thread.inst_state_caches[i].cache, system_charset_info, 32, 0,
            0, (my_hash_get_key)cached_record_get_key, free_cached_record_elem,
            0, HA_KEY_MEM_INST_STATE_CACHE)) {
      SDB_LOG_ERROR(
          "HA: Out of memory while initializing instance state cache");
      return SDB_HA_OOM;
    }
  }
  return 0;
}

static void destroy_inst_state_cache() {
  for (int i = 0; i < HA_MAX_CATA_VERSION_CACHES; i++) {
    native_rw_destroy(&ha_thread.inst_state_caches[i].rw_lock);
    my_hash_reset(&ha_thread.inst_state_caches[i].cache);
    my_hash_free(&ha_thread.inst_state_caches[i].cache);
  }
}

// HA plugin initialization entry
static int server_ha_init(void *p) {
  DBUG_ENTER("server_ha_init");

  // build instance group collection space name
  int len = strlen(HA_INST_GROUP_PREFIX) + strlen(ha_inst_group_name);
  if (len > HA_MAX_INST_GROUP_NAME_LEN) {
    sql_print_error("HA: Instance group name '%s' is too long",
                    ha_inst_group_name);
    DBUG_RETURN(SDB_HA_INIT_ERR);
  }
  snprintf(ha_thread.sdb_group_name, HA_MAX_INST_GROUP_NAME_LEN, "%s%s",
           HA_INST_GROUP_PREFIX, ha_inst_group_name);

  // copy instance group key into ha_thread.group_key
  // then set instance group key to '*'
  if (strlen(ha_inst_group_key) > HA_MAX_KEY_LEN) {
    sql_print_error("HA: Instance group key is too long");
    DBUG_RETURN(SDB_HA_INIT_ERR);
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
    if (init_inst_state_cache()) {
      DBUG_RETURN(SDB_HA_OOM);
    }
    my_thread_attr_init(&ha_thread.thread_attr);
    if (mysql_thread_create(HA_KEY_HA_THREAD, &ha_thread.thread,
                            &ha_thread.thread_attr, ha_recover_and_replay,
                            (void *)(&ha_thread))) {
      SDB_LOG_ERROR("HA: Out of memory while creating 'HA' thread");
      DBUG_RETURN(SDB_HA_OOM);
    }
    ha_thread.is_open = true;
  }
  DBUG_RETURN(0);
}

// HA plugin destruction entry
static int server_ha_deinit(void *p __attribute__((unused))) {
  DBUG_ENTER("server_ha_deinit");

#ifdef IS_MARIADB
  if (NULL == ha_inst_group_name) {
    DBUG_RETURN(0);
  }
#endif

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
    destroy_inst_state_cache();
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
