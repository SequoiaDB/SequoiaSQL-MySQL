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

  int snapshot(bson::BSONObj &obj, int snap_type,
               const bson::BSONObj &condition = SDB_EMPTY_BSON,
               const bson::BSONObj &selected = SDB_EMPTY_BSON,
               const bson::BSONObj &orderBy = SDB_EMPTY_BSON,
               const bson::BSONObj &hint = SDB_EMPTY_BSON, INT64 numToSkip = 0);

  int get_last_result_obj(bson::BSONObj &result, bool get_owned = false);
  int set_session_attr(const bson::BSONObj &option);

  inline bool is_valid() { return m_connection.isValid(); }
  inline void set_pushed_autocommit() { pushed_autocommit = true; }
  inline bool get_pushed_autocommit() { return pushed_autocommit; }

 private:
  sdbclient::sdb m_connection;
  bool m_transaction_on;
  my_thread_id m_thread_id;
  bool pushed_autocommit;
};

#endif
