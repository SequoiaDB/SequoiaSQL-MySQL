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

#ifdef IS_MARIADB

#ifndef MYSQL_SERVER
#define MYSQL_SERVER
#endif

#include <my_global.h>
#include <my_base.h>
#include "sdb_seq.h"
#include "sdb_conn.h"
#include "ha_sdb_errcode.h"

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

int seq_init(sdbclient::sdbSequence *seq, Sdb_conn *connection,
             const char *seq_name) {
  int rc = SDB_ERR_OK;

  rc = connection->get_sdb().getSequence(seq_name, *seq);
  if (rc != SDB_ERR_OK) {
    goto error;
  }
done:
  return rc;
error:
  goto done;
}

int Sdb_seq::init(Sdb_conn *connection, const char *seq_name) {
  int rc = SDB_ERR_OK;

  if (NULL == connection || NULL == seq_name) {
    rc = SDB_ERR_INVALID_ARG;
    goto error;
  }

  m_conn = connection;
  m_thread_id = connection->thread_id();

  rc = retry(boost::bind(seq_init, &m_seq, connection, seq_name));
done:
  return rc;
error:
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

int seq_set_current_value(sdbclient::sdbSequence *seq, int value) {
  return seq->setCurrentValue(value);
}

int Sdb_seq::set_current_value(const int value) {
  return retry(boost::bind(seq_set_current_value, &m_seq, value));
}

int seq_set_attributes(sdbclient::sdbSequence *seq,
                       const bson::BSONObj &options) {
  return seq->setAttributes(options);
}

int Sdb_seq::set_attributes(const bson::BSONObj &options) {
  return retry(boost::bind(seq_set_attributes, &m_seq, options));
}

my_thread_id Sdb_seq::thread_id() {
  return m_thread_id;
}

#endif  // IS_MARIADB
