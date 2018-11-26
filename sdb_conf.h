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

#ifndef SDB_CONF__H
#define SDB_CONF__H

#include "sdb_util.h"
#include <mysql/plugin.h>
#include <my_global.h>
#include <sql_string.h>

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

int sdb_encrypt_password();
int sdb_get_password(String &res);

extern char *sdb_conn_str;
extern my_bool sdb_use_partition;
extern my_bool sdb_use_bulk_insert;
extern int sdb_bulk_insert_size;
extern my_bool sdb_use_autocommit;
extern my_bool sdb_debug_log;
extern char *sdb_user;
extern st_mysql_sys_var *sdb_sys_vars[];

#endif
