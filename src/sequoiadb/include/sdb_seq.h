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
#ifndef SDB_SEQ__H
#define SDB_SEQ__H

#ifdef IS_MARIADB

#include <mysql/psi/mysql_thread.h>
#include <client.hpp>
#include <boost/bind.hpp>
#include <boost/function.hpp>
#include "ha_sdb_def.h"
#include "sdb_conn.h"

class Sdb_seq {
 public:
  Sdb_seq();

  ~Sdb_seq();

  int init(Sdb_conn *connection, const char *seq_name);

  int fetch(int fetch_num, longlong &next_value, int &return_num,
            int &increment);

  int set_current_value(const longlong value);

  int set_attributes(const bson::BSONObj &options);

  int restart(const longlong value);

  my_thread_id thread_id();

 private:
  int retry(boost::function<int()> func);

 private:
  Sdb_conn *m_conn;
  my_thread_id m_thread_id;
  sdbclient::sdbSequence m_seq;
};

#endif  // IS_MARIADB
#endif
