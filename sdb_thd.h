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
