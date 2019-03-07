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

#include <my_base.h>
#include "sdb_cl.h"
#include "sdb_conn.h"
#include "sdb_errcode.h"

using namespace sdbclient;

Sdb_cl::Sdb_cl() : m_conn(NULL), m_thread_id(0) {}

Sdb_cl::~Sdb_cl() {
  close();
}

int Sdb_cl::init(Sdb_conn *connection, char *cs_name, char *cl_name) {
  int rc = SDB_ERR_OK;
  int retry_times = 2;
  sdbCollectionSpace cs;

  if (NULL == connection || NULL == cs_name || NULL == cl_name) {
    rc = SDB_ERR_INVALID_ARG;
    goto error;
  }

  m_conn = connection;
  m_thread_id = connection->thread_id();

retry:
  rc = m_conn->get_sdb().getCollectionSpace(cs_name, cs);
  if (rc != SDB_ERR_OK) {
    goto error;
  }

  rc = cs.getCollection(cl_name, m_cl);
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

bool Sdb_cl::is_transaction_on() {
  return m_conn->is_transaction_on();
}

const char *Sdb_cl::get_cs_name() {
  return m_cl.getCSName();
}

const char *Sdb_cl::get_cl_name() {
  return m_cl.getCollectionName();
}

int Sdb_cl::query(const bson::BSONObj &condition, const bson::BSONObj &selected,
                  const bson::BSONObj &orderBy, const bson::BSONObj &hint,
                  INT64 numToSkip, INT64 numToReturn, INT32 flags) {
  int rc = SDB_ERR_OK;
  int retry_times = 2;
retry:
  rc = m_cl.query(m_cursor, condition, selected, orderBy, hint, numToSkip,
                  numToReturn, flags);
  if (SDB_ERR_OK != rc) {
    goto error;
  }

done:
  return rc;
error:
  if (IS_SDB_NET_ERR(rc)) {
    bool is_transaction = m_conn->is_transaction_on();
    if (0 == m_conn->connect() && !is_transaction && retry_times-- > 0) {
      goto retry;
    }
  }
  convert_sdb_code(rc);
  goto done;
}

int Sdb_cl::query_one(bson::BSONObj &obj, const bson::BSONObj &condition,
                      const bson::BSONObj &selected,
                      const bson::BSONObj &orderBy, const bson::BSONObj &hint,
                      INT64 numToSkip, INT32 flags) {
  int rc = SDB_ERR_OK;
  sdbclient::sdbCursor cursor_tmp;
  int retry_times = 2;
retry:
  rc = m_cl.query(cursor_tmp, condition, selected, orderBy, hint, numToSkip, 1,
                  flags);
  if (rc != SDB_ERR_OK) {
    goto error;
  }

  rc = cursor_tmp.next(obj);
  if (rc != SDB_ERR_OK) {
    goto error;
  }

done:
  return rc;
error:
  if (IS_SDB_NET_ERR(rc)) {
    bool is_transaction = m_conn->is_transaction_on();
    if (0 == m_conn->connect() && !is_transaction && retry_times-- > 0) {
      goto retry;
    }
  }
  convert_sdb_code(rc);
  goto done;
}

int Sdb_cl::current(bson::BSONObj &obj) {
  int rc = SDB_ERR_OK;
  rc = m_cursor.current(obj);
  if (rc != SDB_ERR_OK) {
    if (SDB_DMS_EOC == rc) {
      rc = HA_ERR_END_OF_FILE;
    }
    goto error;
  }

done:
  return rc;
error:
  convert_sdb_code(rc);
  goto done;
}

int Sdb_cl::next(bson::BSONObj &obj) {
  int rc = SDB_ERR_OK;
  rc = m_cursor.next(obj);
  if (rc != SDB_ERR_OK) {
    if (SDB_DMS_EOC == rc) {
      rc = HA_ERR_END_OF_FILE;
    }
    goto error;
  }

done:
  return rc;
error:
  convert_sdb_code(rc);
  goto done;
}

int Sdb_cl::insert(bson::BSONObj &obj) {
  int rc = SDB_ERR_OK;
  int retry_times = 2;
retry:
  rc = m_cl.insert(obj);
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

int Sdb_cl::bulk_insert(INT32 flag, std::vector<bson::BSONObj> &objs) {
  int rc = SDB_ERR_OK;

  rc = m_cl.bulkInsert(flag, objs);
  if (rc != SDB_ERR_OK) {
    goto error;
  }

done:
  return rc;
error:
  convert_sdb_code(rc);
  goto done;
}

int Sdb_cl::upsert(const bson::BSONObj &rule, const bson::BSONObj &condition,
                   const bson::BSONObj &hint, const bson::BSONObj &setOnInsert,
                   INT32 flag) {
  int rc = SDB_ERR_OK;
  int retry_times = 2;
retry:
  rc = m_cl.upsert(rule, condition, hint, setOnInsert, flag);
  if (rc != SDB_ERR_OK) {
    goto error;
  }
done:
  return rc;
error:
  if (IS_SDB_NET_ERR(rc)) {
    bool is_transaction = m_conn->is_transaction_on();
    if (0 == m_conn->connect() && !is_transaction && retry_times-- > 0) {
      goto retry;
    }
  }
  convert_sdb_code(rc);
  goto done;
}

int Sdb_cl::update(const bson::BSONObj &rule, const bson::BSONObj &condition,
                   const bson::BSONObj &hint, INT32 flag) {
  int rc = SDB_ERR_OK;
  int retry_times = 2;
retry:
  rc = m_cl.update(rule, condition, hint, flag);
  if (rc != SDB_ERR_OK) {
    goto error;
  }
done:
  return rc;
error:
  if (IS_SDB_NET_ERR(rc)) {
    bool is_transaction = m_conn->is_transaction_on();
    if (0 == m_conn->connect() && !is_transaction && retry_times-- > 0) {
      goto retry;
    }
  }
  convert_sdb_code(rc);
  goto done;
}

int Sdb_cl::del(const bson::BSONObj &condition, const bson::BSONObj &hint) {
  int rc = SDB_ERR_OK;
  int retry_times = 2;
retry:
  rc = m_cl.del(condition, hint);
  if (rc != SDB_ERR_OK) {
    goto error;
  }
done:
  return rc;
error:
  if (IS_SDB_NET_ERR(rc)) {
    bool is_transaction = m_conn->is_transaction_on();
    if (0 == m_conn->connect() && !is_transaction && retry_times-- > 0) {
      goto retry;
    }
  }
  convert_sdb_code(rc);
  goto done;
}

int Sdb_cl::create_index(const bson::BSONObj &indexDef, const CHAR *pName,
                         BOOLEAN isUnique, BOOLEAN isEnforced) {
  int rc = SDB_ERR_OK;
  int retry_times = 2;
retry:
  rc = m_cl.createIndex(indexDef, pName, isUnique, isEnforced);
  if (SDB_IXM_REDEF == rc) {
    rc = SDB_ERR_OK;
  }
  if (rc != SDB_ERR_OK) {
    goto error;
  }
done:
  return rc;
error:
  if (IS_SDB_NET_ERR(rc)) {
    bool is_transaction = m_conn->is_transaction_on();
    if (0 == m_conn->connect() && !is_transaction && retry_times-- > 0) {
      goto retry;
    }
  }
  convert_sdb_code(rc);
  goto done;
}

int Sdb_cl::drop_index(const char *pName) {
  int rc = SDB_ERR_OK;
  int retry_times = 2;
retry:
  rc = m_cl.dropIndex(pName);
  if (SDB_IXM_NOTEXIST == rc) {
    rc = SDB_ERR_OK;
  }
  if (rc != SDB_ERR_OK) {
    goto error;
  }
done:
  return rc;
error:
  if (IS_SDB_NET_ERR(rc)) {
    bool is_transaction = m_conn->is_transaction_on();
    if (0 == m_conn->connect() && !is_transaction && retry_times-- > 0) {
      goto retry;
    }
  }
  convert_sdb_code(rc);
  goto done;
}

int Sdb_cl::truncate() {
  int rc = SDB_ERR_OK;
  int retry_times = 2;
retry:
  rc = m_cl.truncate();
  if (rc != SDB_ERR_OK) {
    goto error;
  }
done:
  return rc;
error:
  if (IS_SDB_NET_ERR(rc)) {
    bool is_transaction = m_conn->is_transaction_on();
    if (0 == m_conn->connect() && !is_transaction && retry_times-- > 0) {
      goto retry;
    }
  }
  convert_sdb_code(rc);
  goto done;
}

void Sdb_cl::close() {
  m_cursor.close();
}

my_thread_id Sdb_cl::thread_id() {
  return m_thread_id;
}

int Sdb_cl::drop() {
  int rc = SDB_ERR_OK;
  int retry_times = 2;
retry:
  rc = m_cl.drop();
  if (rc != SDB_ERR_OK) {
    if (SDB_DMS_NOTEXIST == rc) {
      rc = SDB_ERR_OK;
      goto done;
    }
    goto error;
  }
done:
  return rc;
error:
  if (IS_SDB_NET_ERR(rc)) {
    bool is_transaction = m_conn->is_transaction_on();
    if (0 == m_conn->connect() && !is_transaction && retry_times-- > 0) {
      goto retry;
    }
  }
  convert_sdb_code(rc);
  goto done;
}

int Sdb_cl::get_count(long long &count, 
                      const bson::BSONObj &condition, 
                      const bson::BSONObj &hint) {
  int rc = SDB_ERR_OK;
  int retry_times = 2;
retry:
  rc = m_cl.getCount(count, condition, hint);
  if (rc != SDB_ERR_OK) {
    goto error;
  }
done:
  return rc;
error:
  if (IS_SDB_NET_ERR(rc)) {
    bool is_transaction = m_conn->is_transaction_on();
    if (0 == m_conn->connect() && !is_transaction && retry_times-- > 0) {
      goto retry;
    }
  }
  convert_sdb_code(rc);
  goto done;
}
