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
#ifndef SDB_CONF__H
#define SDB_CONF__H

#include <mysql/plugin.h>
#include <my_global.h>

#define SDB_COORD_NUM_MAX 128

class Sdb_conn_addrs {
 public:
  Sdb_conn_addrs();
  ~Sdb_conn_addrs();

  int parse_conn_addrs(const char *conn_addrs);

  const char **get_conn_addrs() const;

  int get_conn_num() const;

 private:
  Sdb_conn_addrs(const Sdb_conn_addrs &rh) {}

  Sdb_conn_addrs &operator=(const Sdb_conn_addrs &rh) { return *this; }

  void clear_conn_addrs();

 private:
  char *addrs[SDB_COORD_NUM_MAX];
  int conn_num;
};

extern char *sdb_conn_str;
extern my_bool sdb_use_partition;
extern my_bool sdb_use_bulk_insert;
extern int sdb_bulk_insert_size;
extern my_bool sdb_debug_log;
extern st_mysql_sys_var *sdb_sys_vars[];

#endif
