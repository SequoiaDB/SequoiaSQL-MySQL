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
#include "sdb_thd.h"
#include "sdb_log.h"
#include "sdb_errcode.h"

Thd_sdb::Thd_sdb(THD* thd)
    : m_thd(thd),
      m_slave_thread(thd->slave_thread),
      m_conn(thd_get_thread_id(thd)) {
  m_thread_id = thd_get_thread_id(thd);
  lock_count = 0;
  start_stmt_count = 0;
  save_point_count = 0;
}

Thd_sdb::~Thd_sdb() {}

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

bool Thd_sdb::recycle_conn() {
  int rc = SDB_ERR_OK;
  rc = m_conn.connect();
  if (SDB_ERR_OK != rc) {
    return false;
  }

  return true;
}

// Make sure THD has a Thd_sdb struct allocated and associated
Sdb_conn* check_sdb_in_thd(THD* thd, bool validate_conn) {
  Thd_sdb* thd_sdb = thd_get_thd_sdb(thd);
  if (NULL == thd_sdb) {
    thd_sdb = Thd_sdb::seize(thd);
    if (NULL == thd_sdb) {
      return NULL;
    }
    thd_set_thd_sdb(thd, thd_sdb);
  }

  if (validate_conn && !thd_sdb->valid_conn()) {
    if (!thd_sdb->recycle_conn()) {
      return NULL;
    }
  }

  DBUG_ASSERT(thd_sdb->is_slave_thread() == thd->slave_thread);

  return thd_sdb->get_conn();
}