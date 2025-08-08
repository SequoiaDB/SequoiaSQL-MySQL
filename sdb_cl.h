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
#ifndef SDB_CL__H
#define SDB_CL__H

#include <mysql/psi/mysql_thread.h>
#include <vector>
#include "client.hpp"
#include "sdb_def.h"
#include "sdb_conn.h"

class Sdb_cl {
 public:
  Sdb_cl();

  ~Sdb_cl();

  int init(Sdb_conn *connection, char *cs_name, char *cl_name);

  bool is_transaction_on();

  const char *get_cs_name();

  const char *get_cl_name();

  int query(const bson::BSONObj &condition = sdbclient::_sdbStaticObject,
            const bson::BSONObj &selected = sdbclient::_sdbStaticObject,
            const bson::BSONObj &orderBy = sdbclient::_sdbStaticObject,
            const bson::BSONObj &hint = sdbclient::_sdbStaticObject,
            INT64 numToSkip = 0, INT64 numToReturn = -1,
            INT32 flags = QUERY_WITH_RETURNDATA);

  int query_one(bson::BSONObj &obj,
                const bson::BSONObj &condition = sdbclient::_sdbStaticObject,
                const bson::BSONObj &selected = sdbclient::_sdbStaticObject,
                const bson::BSONObj &orderBy = sdbclient::_sdbStaticObject,
                const bson::BSONObj &hint = sdbclient::_sdbStaticObject,
                INT64 numToSkip = 0, INT32 flags = QUERY_WITH_RETURNDATA);

  int current(bson::BSONObj &obj);

  int next(bson::BSONObj &obj);

  int insert(bson::BSONObj &obj);

  int bulk_insert(INT32 flag, std::vector<bson::BSONObj> &objs);

  int update(const bson::BSONObj &rule,
             const bson::BSONObj &condition = sdbclient::_sdbStaticObject,
             const bson::BSONObj &hint = sdbclient::_sdbStaticObject,
             INT32 flag = 0);

  int upsert(const bson::BSONObj &rule,
             const bson::BSONObj &condition = sdbclient::_sdbStaticObject,
             const bson::BSONObj &hint = sdbclient::_sdbStaticObject,
             const bson::BSONObj &setOnInsert = sdbclient::_sdbStaticObject,
             INT32 flag = 0);

  int del(const bson::BSONObj &condition = sdbclient::_sdbStaticObject,
          const bson::BSONObj &hint = sdbclient::_sdbStaticObject);

  int create_index(const bson::BSONObj &indexDef, const CHAR *pName,
                   BOOLEAN isUnique, BOOLEAN isEnforced);

  int drop_index(const char *pName);

  int truncate();

  void close();  // close m_cursor

  my_thread_id thread_id();

  int drop();

  int get_count(long long &count);

 private:
  Sdb_conn *m_conn;
  my_thread_id m_thread_id;
  sdbclient::sdbCollection m_cl;
  sdbclient::sdbCursor m_cursor;
};
#endif
