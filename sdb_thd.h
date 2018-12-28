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

#ifndef SDB_THD__H
#define SDB_THD__H

#include <mysql/plugin.h>
#include <client.hpp>
#include "sdb_conn.h"

extern handlerton* sdb_hton;

class Thd_sdb {
 private:
  Thd_sdb(THD* thd);
  ~Thd_sdb();

 public:
  static Thd_sdb* seize(THD* thd);
  static void release(Thd_sdb* thd_sdb);

  bool recycle_conn();
  inline my_thread_id thread_id() const { return m_thread_id; }
  inline bool is_slave_thread() const { return m_slave_thread; }
  inline Sdb_conn* get_conn() { return &m_conn; }
  inline bool valid_conn() { return m_conn.is_valid(); }

  uint lock_count;
  uint start_stmt_count;
  uint save_point_count;

 private:
  THD* m_thd;
  my_thread_id m_thread_id;
  const bool m_slave_thread;  // cached value of m_thd->slave_thread
  Sdb_conn m_conn;
};

// Set Thd_sdb pointer for THD
static inline void thd_set_thd_sdb(THD* thd, Thd_sdb* thd_sdb) {
  thd_set_ha_data(thd, sdb_hton, thd_sdb);
}

// Get Thd_sdb pointer from THD
static inline Thd_sdb* thd_get_thd_sdb(THD* thd) {
  return (Thd_sdb*)thd_get_ha_data(thd, sdb_hton);
}

// Make sure THD has a Thd_sdb struct assigned
Sdb_conn* check_sdb_in_thd(THD* thd, bool validate_conn = false);

#endif /* SDB_THD__H */
