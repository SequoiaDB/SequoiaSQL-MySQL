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
    SDB_LOG_ERROR("Failed to connect to sequoiadb");
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