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
#ifdef IS_MARIADB

#ifndef MYSQL_SERVER
#define MYSQL_SERVER
#endif

#include <my_global.h>
#include <my_base.h>
#include "sdb_seq.h"
#include "sdb_conn.h"
#include "ha_sdb_errcode.h"
#include "ha_sdb_log.h"

using namespace sdbclient;

Sdb_seq::Sdb_seq() : m_conn(NULL), m_thread_id(0) {}

Sdb_seq::~Sdb_seq() {}

int Sdb_seq::retry(boost::function<int()> func) {
  int rc = SDB_ERR_OK;
  int retry_times = 2;
retry:
  rc = func();
  if (rc != SDB_ERR_OK) {
    goto error;
  }
done:
  return rc;
error:
  if (IS_SDB_NET_ERR(rc)) {
    bool is_transaction = m_conn->is_transaction_on();
    if (!is_transaction && retry_times-- > 0 && 0 == m_conn->connect()) {
      goto retry;
    }
  }
  convert_sdb_code(rc);
  goto done;
}

int seq_fetch(sdbclient::sdbSequence *seq, int fetch_num, longlong *next_value,
              int *return_num, int *increment) {
  return seq->fetch(fetch_num, *next_value, *return_num, *increment);
}

int Sdb_seq::fetch(int fetch_num, longlong &next_value, int &return_num,
                   int &increment) {
  return retry(boost::bind(seq_fetch, &m_seq, fetch_num, &next_value,
                           &return_num, &increment));
}

int seq_set_current_value(sdbclient::sdbSequence *seq, const longlong value) {
  return seq->setCurrentValue(value);
}

int Sdb_seq::set_current_value(const longlong value) {
  return retry(boost::bind(seq_set_current_value, &m_seq, value));
}

int seq_set_attributes(sdbclient::sdbSequence *seq,
                       const bson::BSONObj &options) {
  int rc = SDB_ERR_OK;
  try {
    rc = seq->setAttributes(options);
  }
  SDB_EXCEPTION_CATCHER(rc, "Failed to set sequence attributes, exception:%s",
                        e.what());
done:
  return rc;
error:
  goto done;
}

int Sdb_seq::set_attributes(const bson::BSONObj &options) {
  return retry(boost::bind(seq_set_attributes, &m_seq, options));
}

int seq_restart(sdbclient::sdbSequence *seq, const longlong value) {
  return seq->restart(value);
}

int Sdb_seq::restart(const longlong value) {
  return retry(boost::bind(seq_restart, &m_seq, value));
}

my_thread_id Sdb_seq::thread_id() {
  return m_thread_id;
}

#endif  // IS_MARIADB
