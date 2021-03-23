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

#ifndef MYSQL_SERVER
#define MYSQL_SERVER
#endif

#include <my_global.h>
#include <mysql/plugin.h>
#include <sql_class.h>
#include <my_base.h>
#include "ha_sdb_thd.h"
#include "ha_sdb_log.h"
#include "ha_sdb_errcode.h"
#include "server_ha.h"
#include <strfunc.h>

// Complete the struct declaration
struct st_mysql_sys_var {
  MYSQL_PLUGIN_VAR_HEADER;
};

void sdb_update_sys_var_str(const struct st_mysql_sys_var *var,
                            const void *var_ptr, const void *save) {
#if defined IS_MYSQL
  *(char **)var_ptr = *(char **)save;
#elif defined IS_MARIADB
  char *value = *(char **)save;
  if (var->flags & PLUGIN_VAR_MEMALLOC) {
    char *old = *(char **)tgt;
    if (value) {
      *(char **)var_ptr = my_strdup(value, MYF(0));
    } else {
      *(char **)var_ptr = 0;
    }
    my_free(old);
  } else {
    *(char **)var_ptr = value;
  }
#endif
}

static void sdb_set_conn_addr(THD *thd, struct st_mysql_sys_var *var, void *tgt,
                              const void *save) {
  bool addr_changed = false;

  if (0 != strncmp(*(char **)tgt, *(char **)save, strlen(*(char **)save))) {
    addr_changed = true;
  }

  sdb_update_sys_var_str(var, tgt, save);

  if (!addr_changed) {
    goto done;
  }

  /* Invalid the version cache and refresh.*/
  /* TODO: not consider the case of coord addrs was configured with
     two or more different version of sdb.*/
  sdb_invalidate_version_cache();

done:
  /* Ignore the error code. */
  return;
}

static int sdb_use_trans_check(THD *thd, struct st_mysql_sys_var *var,
                               void *save, struct st_mysql_value *value) {
  int rc = SDB_OK;
  Sdb_conn *conn = NULL;
  Sdb_session_attrs *session_attrs = NULL;
  char buff[STRING_BUFFER_USUAL_SIZE] = {0};
  const char *str = NULL;
  int result = 0, length = 0;
  long long tmp = 0;
  Thd_sdb *thd_sdb = thd_get_thd_sdb(thd);

  /* Check the validate of parameter input value. */
  if (value->value_type(value) == MYSQL_VALUE_TYPE_STRING) {
    length = sizeof(buff);
    if (!(str = value->val_str(value, buff, &length)) ||
        (result = find_type(&bool_typelib, str, length, 1) - 1) < 0) {
      goto error;
    }
  } else {
    if (value->val_int(value, &tmp) < 0) {
      goto error;
    }
    if (tmp > 1) {
      goto error;
    }
    result = (int)tmp;
  }

  /*
    Check if sequoiadb_use_transaction changed or not, it is not
    allowed to change during transaction.
  */
  if ((thd_sdb && thd_sdb->valid_conn() && thd_sdb->conn_is_authenticated())) {
    conn = thd_sdb->get_conn();
    if (conn->is_transaction_on()) {
      session_attrs = conn->get_session_attrs();
      if (session_attrs->get_last_trans_auto_commit() != (bool)result) {
        SDB_PRINT_ERROR(ER_WRONG_VALUE_FOR_VAR,
                        "Cannot change sequoiadb_use_transaction during "
                        "transaction.");
        goto error;
      }
    }
  }
  *(my_bool *)save = result ? TRUE : FALSE;

done:
  return rc;
error:
  rc = 1;
  goto done;
}

bool is_trans_timeout_supported(Sdb_conn *conn) {
  bool supported = false;
  int major = 0;
  int minor = 0;
  int fix = 0;
  int rc = 0;

  if (NULL == conn) {
    goto error;
  }
  DBUG_ASSERT(conn->thread_id() == sdb_thd_id(current_thd));

  rc = sdb_get_version(*conn, major, minor, fix);
  if (rc != 0) {
    goto error;
  }

  if (major < 3 ||                  // x < 3
      (3 == major && minor < 2)) {  // 3.x < 3.2
    supported = false;
  } else {
    supported = true;
  }

done:
  return supported;
error:
  supported = false;
  goto done;
}

/*The session attribute of TransTimeout can be updated during transaction.*/
void sdb_set_trans_timeout(MYSQL_THD thd, struct st_mysql_sys_var *var,
                           void *var_ptr, const void *save) {
  // ignore the ret value.
  int rc = SDB_OK;
  Sdb_conn *conn = NULL;
  Sdb_session_attrs *session_attrs = NULL;
  Thd_sdb *thd_sdb = thd_get_thd_sdb(thd);

  /* Set the target value here, then we can use sdb_lock_wait_timeout(thd) to
     check if it was setted by session but not global.*/
  *(int *)var_ptr = *(int *)save;

  /* No need to change sequoiadb_lock_wait_timeout if the sdb conn is not
     establised. */
  if (!(thd_sdb && thd_sdb->valid_conn() && thd_sdb->conn_is_authenticated())) {
    goto done;
  }

  conn = thd_sdb->get_conn();
  /* No need to change sequoiadb_lock_wait_timeout out of transaction, it will
     be changed during beginning transaction. */
  if (!conn->is_transaction_on()) {
    goto done;
  }

  if (is_trans_timeout_supported(conn)) {
    session_attrs = conn->get_session_attrs();
    /* Set the lock wait timeout when the lock_wait_timeout has changed*/
    /* Use sdb_lock_wait_timeout(thd) insted of *(int*)save to updated only in
       set session sequoiadb_lock_wait_timeout.*/
    session_attrs->set_trans_timeout(sdb_lock_wait_timeout(thd));
    rc = conn->set_my_session_attr();
    if (rc != SDB_OK) {
      SDB_LOG_WARNING("Failed to set the lock wait timeout on sequoiadb, rc=%d",
                      rc);
      goto error;
    }
  }
done:
  // ignore the ret value.
  return;

error:
  goto done;
}

static int sdb_use_rbs_check(THD *thd, struct st_mysql_sys_var *var, void *save,
                             struct st_mysql_value *value) {
  int rc = SDB_OK;
  Sdb_conn *conn = NULL;
  Sdb_session_attrs *session_attrs = NULL;
  char buff[STRING_BUFFER_USUAL_SIZE] = {0};
  const char *str = NULL;
  int result = 0, length = 0;
  long long tmp = 0;
  Thd_sdb *thd_sdb = thd_get_thd_sdb(thd);

  /* Check the validate of parameter input value. */
  if (value->value_type(value) == MYSQL_VALUE_TYPE_STRING) {
    length = sizeof(buff);
    if (!(str = value->val_str(value, buff, &length)) ||
        (result = find_type(&bool_typelib, str, length, 1) - 1) < 0) {
      goto error;
    }
  } else {
    if (value->val_int(value, &tmp) < 0) {
      goto error;
    }
    if (tmp > 1) {
      goto error;
    }
    result = (int)tmp;
  }

  /*
    Check if sequoiadb_use_rollback_segments changed or not, it is not
    allowed to change during transaction.
  */
  if ((thd_sdb && thd_sdb->valid_conn() && thd_sdb->conn_is_authenticated())) {
    conn = thd_sdb->get_conn();
    if (conn->is_transaction_on()) {
      session_attrs = conn->get_session_attrs();
      if (session_attrs->get_last_trans_use_rollback_segments() !=
          (bool)result) {
        SDB_PRINT_ERROR(ER_WRONG_VALUE_FOR_VAR,
                        "Cannot change sequoiadb_use_rollback_segments during "
                        "transaction.");
        goto error;
      }
    }
  }
  *(my_bool *)save = result ? TRUE : FALSE;

done:
  return rc;
error:
  rc = 1;
  goto done;
}

// Check the validity of preferred instance
bool sdb_prefer_inst_is_valid(const char *s) {
  int rs = true;
  const char *right = s;
  const char *left = s;
  if (0 == strlen(s)) {
    rs = false;
    goto done;
  }
  while (*left != 0) {
    size_t len = 0;
    const char *p_str = NULL;
    bool has_valid_char = false;
    char str[STRING_BUFFER_USUAL_SIZE] = "";
    right = strchr(right, ',');
    if (right) {
      len = right - left;
    } else {
      len = strlen(left);
    }
    memcpy(str, left, len);
    str[len] = '\0';
    p_str = str;
    has_valid_char = false;
    while (*p_str) {
      if ((*p_str) == ' ' || (*p_str) == '\t') {
        ++p_str;
        continue;
      }
      if ((*p_str) < '0' || ((*p_str) > '9' && (*p_str) < 'A') ||
          (*p_str) > 'z') {
        rs = false;
        goto done;
      }
      has_valid_char = true;
      ++p_str;
    }
    if (!has_valid_char) {
      rs = false;
      goto done;
    }
    if (NULL == right) {
      break;
    }
    if ('\0' == *(++right)) {
      rs = false;
      goto done;
    }
    left = right;
  }

done:
  return rs;
}

static int sdb_prefer_inst_check(THD *thd, struct st_mysql_sys_var *var,
                                 void *save, struct st_mysql_value *value) {
  int rc = 0;
  const char *str = NULL;
  int length = 0;
  char buff[STRING_BUFFER_USUAL_SIZE] = {0};

  length = sizeof(buff);
  str = value->val_str(value, buff, &length);
  if (length >= STRING_BUFFER_USUAL_SIZE) {
    rc = 1;
    goto error;
  }
  rc = sdb_prefer_inst_is_valid(str) ? 0 : 1;

done:
  *static_cast<const char **>(save) = (0 == rc) ? str : NULL;
  return rc;
error:
  goto done;
}

static void sdb_set_prefer_inst(THD *thd, struct st_mysql_sys_var *var,
                                void *var_ptr, const void *save) {
  int rc = SDB_OK;
  bool is_changed = false;
  size_t var_ptr_len = strlen(*(char **)var_ptr);
  size_t save_len = strlen(*(char **)save);
  int len = var_ptr_len > save_len ? var_ptr_len : save_len;
  Sdb_conn *conn = NULL;
  Sdb_session_attrs *session_attrs = NULL;
  Thd_sdb *thd_sdb = thd_get_thd_sdb(thd);

  if (0 != strncmp(*(char **)var_ptr, *(char **)save, len)) {
    is_changed = true;
  }

  sdb_update_sys_var_str(var, var_ptr, save);

  if (!is_changed) {
    goto done;
  }

  /* No need to change sequoiadb_preferred_instance if the sdb conn is not
           establised. */
  if (!(thd_sdb && thd_sdb->valid_conn() && thd_sdb->conn_is_authenticated())) {
    goto done;
  }

  conn = thd_sdb->get_conn();
  session_attrs = conn->get_session_attrs();
  session_attrs->set_preferred_instance(*(char **)var_ptr);
  rc = conn->set_my_session_attr();
  if (rc != SDB_OK) {
    SDB_LOG_WARNING("Failed to set preferred instance on sequoiadb, rc=%d", rc);
    goto error;
  }

done:
  return;
error:
  goto done;
}

static int sdb_prefer_inst_mode_check(THD *thd, struct st_mysql_sys_var *var,
                                      void *save,
                                      struct st_mysql_value *value) {
  int rc = 0;
  const char *str = NULL;
  int length = 0;
  char buff[STRING_BUFFER_USUAL_SIZE] = {0};

  length = sizeof(buff);
  str = value->val_str(value, buff, &length);
  if (0 != strcasecmp(str, SDB_PREFERRED_INSTANCE_MODE_RANDOM) &&
      0 != strcasecmp(str, SDB_PREFERRED_INSTANCE_MODE_ORDERED)) {
    rc = 1;
  }
  *static_cast<const char **>(save) = (0 == rc) ? str : NULL;
  return rc;
}

static void sdb_set_prefer_inst_mode(THD *thd, struct st_mysql_sys_var *var,
                                     void *var_ptr, const void *save) {
  int rc = SDB_OK;
  bool is_changed = false;
  Sdb_conn *conn = NULL;
  Sdb_session_attrs *session_attrs = NULL;
  Thd_sdb *thd_sdb = thd_get_thd_sdb(thd);
  size_t var_ptr_len = strlen(*(char **)var_ptr);
  size_t save_len = strlen(*(char **)save);
  int len = var_ptr_len > save_len ? var_ptr_len : save_len;

  if (0 != strncmp(*(char **)var_ptr, *(char **)save, len)) {
    is_changed = true;
  }

  sdb_update_sys_var_str(var, var_ptr, save);

  if (!is_changed) {
    goto done;
  }

  /* No need to change sequoiadb_preferred_instance_mode if the sdb conn is not
           establised. */
  if (!(thd_sdb && thd_sdb->valid_conn() && thd_sdb->conn_is_authenticated())) {
    goto done;
  }

  conn = thd_sdb->get_conn();
  session_attrs = conn->get_session_attrs();
  session_attrs->set_preferred_instance_mode(*(char **)var_ptr);
  rc = conn->set_my_session_attr();
  if (rc != SDB_OK) {
    SDB_LOG_WARNING("Failed to set preferred instance mode on sequoiadb, rc=%d",
                    rc);
    goto error;
  }

done:
  return;
error:
  goto done;
}

bool is_prefer_strict_supported(Sdb_conn *conn) {
  bool supported = false;
  int major = 0;
  int minor = 0;
  int fix = 0;
  int rc = 0;

  if (NULL == conn) {
    goto error;
  }
  DBUG_ASSERT(conn->thread_id() == sdb_thd_id(current_thd));

  rc = sdb_get_version(*conn, major, minor, fix);
  if (rc != 0) {
    goto error;
  }

  if (major < 3 ||                  // x < 3
      (3 == major && minor < 2)) {  // 3.x < 3.2
    supported = false;
  } else {
    supported = true;
  }

done:
  return supported;
error:
  supported = false;
  goto done;
}

static void sdb_set_prefer_strict(THD *thd, struct st_mysql_sys_var *var,
                                  void *var_ptr, const void *save) {
  int rc = SDB_OK;
  bool is_changed = false;
  Sdb_conn *conn = NULL;
  Sdb_session_attrs *session_attrs = NULL;
  Thd_sdb *thd_sdb = thd_get_thd_sdb(thd);

  if (*(bool *)var_ptr != *(bool *)save) {
    is_changed = true;
  }

  *(bool *)var_ptr = *(bool *)save;

  if (!is_changed) {
    goto done;
  }

  /* No need to change sequoiadb_preferred_strict if the sdb conn is not
           establised. */
  if (!(thd_sdb && thd_sdb->valid_conn() && thd_sdb->conn_is_authenticated())) {
    goto done;
  }

  conn = thd_sdb->get_conn();
  if (is_prefer_strict_supported(conn)) {
    session_attrs = conn->get_session_attrs();
    session_attrs->set_preferred_strict(*(bool *)var_ptr);
    rc = conn->set_my_session_attr();
    if (rc != SDB_OK) {
      SDB_LOG_WARNING("Failed to set preferred strict on sequoiadb, rc=%d", rc);
      goto error;
    }
  }

done:
  return;
error:
  goto done;
}

bool is_prefer_period_supported(Sdb_conn *conn) {
  bool supported = false;
  int major = 0;
  int minor = 0;
  int fix = 0;
  int rc = 0;

  if (NULL == conn) {
    goto error;
  }
  DBUG_ASSERT(conn->thread_id() == sdb_thd_id(current_thd));

  rc = sdb_get_version(*conn, major, minor, fix);
  if (rc != 0) {
    goto error;
  }

  if (major < 3 ||                              // x < 3
      (3 == major && minor < 2) ||              // 3.x < 3.2
      (3 == major && 2 == minor && fix < 5) ||  // 3.2.x < 3.2.5
      (3 == major && 4 == minor && fix < 1)) {  // 3.4.x < 3.4.1
    supported = false;
  } else {
    supported = true;
  }

done:
  return supported;
error:
  supported = false;
  goto done;
}

static void sdb_set_prefer_period(THD *thd, struct st_mysql_sys_var *var,
                                  void *var_ptr, const void *save) {
  int rc = SDB_OK;
  bool is_changed = false;
  Sdb_conn *conn = NULL;
  Sdb_session_attrs *session_attrs = NULL;
  Thd_sdb *thd_sdb = thd_get_thd_sdb(thd);

  if (*(int *)var_ptr != *(int *)save) {
    is_changed = true;
  }

  *(int *)var_ptr = *(int *)save;

  if (!is_changed) {
    goto done;
  }

  /* No need to change sequoiadb_preferred_period if the sdb conn is not
           establised. */
  if (!(thd_sdb && thd_sdb->valid_conn() && thd_sdb->conn_is_authenticated())) {
    goto done;
  }

  conn = thd_sdb->get_conn();
  if (is_prefer_period_supported(conn)) {
    session_attrs = conn->get_session_attrs();
    session_attrs->set_preferred_period(*(int *)var_ptr);
    rc = conn->set_my_session_attr();
    if (rc != SDB_OK) {
      SDB_LOG_WARNING("Failed to set preferred period on sequoiadb, rc=%d", rc);
      goto error;
    }
  }

done:
  return;
error:
  goto done;
}

void sdb_init_vars_check_and_update_funcs() {
  sdb_set_connection_addr = &sdb_set_conn_addr;
  sdb_use_transaction_check = &sdb_use_trans_check;
  sdb_set_lock_wait_timeout = &sdb_set_trans_timeout;
  sdb_use_rollback_segments_check = &sdb_use_rbs_check;
  sdb_preferred_instance_check = &sdb_prefer_inst_check;
  sdb_set_preferred_instance = &sdb_set_prefer_inst;
  sdb_preferred_instance_mode_check = &sdb_prefer_inst_mode_check;
  sdb_set_preferred_instance_mode = &sdb_set_prefer_inst_mode;
  sdb_set_preferred_strict = &sdb_set_prefer_strict;
  sdb_set_preferred_period = &sdb_set_prefer_period;
}
uchar *thd_sdb_share_get_key(THD_SDB_SHARE *thd_sdb_share, size_t *length,
                             my_bool not_used MY_ATTRIBUTE((unused))) {
  *length = sizeof(thd_sdb_share->share_ptr.get());
  return (uchar *)thd_sdb_share->share_ptr.get();
}

extern void free_thd_open_shares_elem(void *share_ptr);

Thd_sdb::Thd_sdb(THD *thd)
    : m_thd(thd),
      m_slave_thread(thd->slave_thread),
      m_conn(thd_get_thread_id(thd)) {
  m_thread_id = thd_get_thread_id(thd);
  lock_count = 0;
  auto_commit = false;
  start_stmt_count = 0;
  save_point_count = 0;
  found = 0;
  updated = 0;
  deleted = 0;
  duplicated = 0;
  cl_copyer = NULL;

  // check collection version for HA module
  if (ha_is_open()) {
    m_conn.set_check_collection_version(true);
  }
#ifdef IS_MYSQL
  part_alter_ctx = NULL;
#endif

  (void)sdb_hash_init(&open_table_shares, table_alias_charset, 5, 0, 0,
                      (my_hash_get_key)thd_sdb_share_get_key,
                      free_thd_open_shares_elem, 0, PSI_INSTRUMENT_ME);
}

Thd_sdb::~Thd_sdb() {
  my_hash_free(&open_table_shares);
}

Thd_sdb *Thd_sdb::seize(THD *thd) {
  Thd_sdb *thd_sdb = new (std::nothrow) Thd_sdb(thd);
  if (NULL == thd_sdb) {
    return NULL;
  }

  return thd_sdb;
}

void Thd_sdb::release(Thd_sdb *thd_sdb) {
  delete thd_sdb;
}

int Thd_sdb::recycle_conn() {
  int rc = SDB_ERR_OK;
  rc = m_conn.connect();
  if (SDB_ERR_OK != rc) {
    SDB_LOG_ERROR("%s", m_conn.get_err_msg());
    m_conn.clear_err_msg();
    goto error;
  }
done:
  return rc;
error:
  goto done;
}

// Make sure THD has a Thd_sdb struct allocated and associated
int check_sdb_in_thd(THD *thd, Sdb_conn **conn, bool validate_conn) {
  int rc = 0;
  Thd_sdb *thd_sdb = thd_get_thd_sdb(thd);
  if (NULL == thd_sdb) {
    thd_sdb = Thd_sdb::seize(thd);
    if (NULL == thd_sdb) {
      rc = HA_ERR_OUT_OF_MEM;
      goto error;
    }
    thd_set_thd_sdb(thd, thd_sdb);
  }

  if (validate_conn && !thd_sdb->get_conn()->is_transaction_on() &&
      !(thd_sdb->valid_conn() && thd_sdb->conn_is_authenticated())) {
    rc = thd_sdb->recycle_conn();
    if (0 != rc) {
      goto error;
    }
  }

  DBUG_ASSERT(thd_sdb->is_slave_thread() == thd->slave_thread);
  *conn = thd_sdb->get_conn();

done:
  return rc;
error:
  if (thd_sdb) {
    *conn = thd_sdb->get_conn();
  } else {
    *conn = NULL;
  }
  goto done;
}
