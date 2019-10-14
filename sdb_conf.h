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

#include <my_global.h>
#include "sdb_util.h"
#include <mysql/plugin.h>
#include <sql_string.h>

#define SDB_OPTIMIZER_OPTION_SELECT_COUNT (1ULL << 0)
#define SDB_OPTIMIZER_OPTION_DELETE (1ULL << 1)
#define SDB_OPTIMIZER_OPTION_UPDATE (1ULL << 2)

#define SDB_OPTIMIZER_OPTIONS_DEFAULT                                \
  (SDB_OPTIMIZER_OPTION_SELECT_COUNT | SDB_OPTIMIZER_OPTION_DELETE | \
   SDB_OPTIMIZER_OPTION_UPDATE)

#if MYSQL_VERSION_ID >= 50725
#define SDB_INVISIBLE | PLUGIN_VAR_INVISIBLE
#else
#define SDB_INVISIBLE
#endif

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
uint sdb_selector_pushdown_threshold(THD *thd);
bool sdb_execute_only_in_mysql(THD *thd);
longlong sdb_alter_table_overhead_threshold(THD *thd);
ulonglong sdb_get_optimizer_options(THD *thd);
#define SDB_EXECUTE_ONLY_IN_MYSQL_RETURN(thd, ret, default) \
  {                                                         \
    do {                                                    \
      if (sdb_execute_only_in_mysql(thd)) {                 \
        ret = default;                                      \
        return ret;                                         \
      }                                                     \
    } while (0);                                            \
  }

#define SDB_EXECUTE_ONLY_IN_MYSQL_DBUG_RETURN(thd, ret, default) \
  {                                                              \
    do {                                                         \
      if (sdb_execute_only_in_mysql(thd)) {                      \
        ret = default;                                           \
        DBUG_RETURN(ret);                                        \
      }                                                          \
    } while (0);                                                 \
  }

#define SDB_EXECUTE_ONLY_IN_MYSQL_VOID_RETURN(thd) \
  {                                                \
    do {                                           \
      if (sdb_execute_only_in_mysql(thd)) {        \
        return;                                    \
      }                                            \
    } while (0);                                   \
  }

extern char *sdb_conn_str;
extern char *sdb_user;
extern my_bool sdb_use_partition;
extern my_bool sdb_use_bulk_insert;
extern int sdb_bulk_insert_size;
extern int sdb_replica_size;
extern my_bool sdb_use_autocommit;
extern my_bool sdb_debug_log;
extern st_mysql_sys_var *sdb_sys_vars[];
extern ulong sdb_error_level;

extern String sdb_encoded_password;

#endif
