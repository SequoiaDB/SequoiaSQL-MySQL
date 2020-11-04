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
#include <sql_class.h>
#include <my_base.h>
#include "ha_sdb_thd.h"
#include "ha_sdb_log.h"
#include "ha_sdb_errcode.h"
#include <strfunc.h>

/*The session attribute of TransTimeout can be updated during transaction.*/
void sdb_set_trans_timeout(MYSQL_THD thd, struct st_mysql_sys_var *var,
                           void *var_ptr, const void *save) {
  // ignore the ret value.
  int rc = SDB_OK;
  Sdb_conn *conn = NULL;
  Sdb_session_attrs *session_attrs = NULL;
  static bool support_trans_timeout_attr = true;
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

  if (support_trans_timeout_attr) {
    session_attrs = conn->get_session_attrs();
    /* Set the lock wait timeout when the lock_wait_timeout has changed*/
    /* Use sdb_lock_wait_timeout(thd) insted of *(int*)save to updated only in
       set session sequoiadb_lock_wait_timeout.*/
    session_attrs->set_trans_timeout(sdb_lock_wait_timeout(thd));
    rc = conn->set_my_session_attr();
    // No such options before sdb v3.2.4 Ignore it.
    if (SDB_INVALIDARG == get_sdb_code(rc)) {
      support_trans_timeout_attr = false;
      rc = SDB_ERR_OK;
    }

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
  char buff[STRING_BUFFER_USUAL_SIZE];
  const char *str;
  int result, length;
  long long tmp;
  Thd_sdb *thd_sdb = thd_get_thd_sdb(thd);

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

void sdb_init_lock_wait_timeout() {
  sdb_set_lock_wait_timeout = &sdb_set_trans_timeout;
  sdb_use_rollback_segments_check = &sdb_use_rbs_check;
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

  if (validate_conn &&
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
