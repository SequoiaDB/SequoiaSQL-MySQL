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

#ifndef SDB_CONN__H
#define SDB_CONN__H

#include <my_global.h>
#include <my_thread_local.h>
#include <client.hpp>
#include "sdb_def.h"

class Sdb_cl;
class Sdb_statistics;

class Sdb_conn {
 public:
  Sdb_conn(my_thread_id _tid);

  ~Sdb_conn();

  int connect();

  sdbclient::sdb &get_sdb();

  my_thread_id thread_id();

  int begin_transaction();

  int commit_transaction();

  int rollback_transaction();

  bool is_transaction_on();

  int get_cl(char *cs_name, char *cl_name, Sdb_cl &cl);

  int create_cl(char *cs_name, char *cl_name,
                const bson::BSONObj &options = SDB_EMPTY_BSON,
                bool *created_cs = NULL, bool *created_cl = NULL);

  int rename_cl(char *cs_name, char *old_cl_name, char *new_cl_name);

  int drop_cl(char *cs_name, char *cl_name);

  int drop_cs(char *cs_name);

  int get_cl_statistics(char *cs_name, char *cl_name, Sdb_statistics &stats);

  inline bool is_valid() { return m_connection.isValid(); }

 private:
  sdbclient::sdb m_connection;
  bool m_transaction_on;
  my_thread_id m_thread_id;
};

#endif
