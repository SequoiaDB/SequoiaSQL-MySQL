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
#ifndef MYSQL_SERVER
#define MYSQL_SERVER
#endif

#include <my_global.h>
#include <sql_class.h>
#include <my_base.h>
#include "ha_sdb_thd.h"
#include "ha_sdb_log.h"
#include "ha_sdb_errcode.h"

uchar* thd_sdb_share_get_key(THD_SDB_SHARE* thd_sdb_share, size_t* length,
                             my_bool not_used MY_ATTRIBUTE((unused))) {
  *length = sizeof(thd_sdb_share->share_ptr.get());
  return (uchar*)thd_sdb_share->share_ptr.get();
}

extern void free_thd_open_shares_elem(void* share_ptr);

Thd_sdb::Thd_sdb(THD* thd)
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

Thd_sdb* Thd_sdb::seize(THD* thd) {
  Thd_sdb* thd_sdb = new (std::nothrow) Thd_sdb(thd);
  if (NULL == thd_sdb) {
    return NULL;
  }

  return thd_sdb;
}

void Thd_sdb::release(Thd_sdb* thd_sdb) {
  delete thd_sdb;
}

int Thd_sdb::recycle_conn() {
  int rc = SDB_ERR_OK;
  rc = m_conn.connect();
  return rc;
}

// Make sure THD has a Thd_sdb struct allocated and associated
int check_sdb_in_thd(THD* thd, Sdb_conn** conn, bool validate_conn) {
  int rc = 0;
  Thd_sdb* thd_sdb = thd_get_thd_sdb(thd);
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
