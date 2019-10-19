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

#ifndef MYSQL_SERVER
#define MYSQL_SERVER
#endif

#include <my_global.h>
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
                  const bson::BSONObj &order_by, const bson::BSONObj &hint,
                  longlong num_to_skip, longlong num_to_return, int flags) {
  int rc = SDB_ERR_OK;
  int retry_times = 2;
retry:
  rc = m_cl.query(m_cursor, condition, selected, order_by, hint, num_to_skip,
                  num_to_return, flags);
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
                      const bson::BSONObj &order_by, const bson::BSONObj &hint,
                      longlong num_to_skip, int flags) {
  int rc = SDB_ERR_OK;
  sdbclient::sdbCursor cursor_tmp;
  int retry_times = 2;
retry:
  rc = m_cl.query(cursor_tmp, condition, selected, order_by, hint, num_to_skip,
                  1, flags);
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

int Sdb_cl::current(bson::BSONObj &obj, my_bool get_owned) {
  int rc = SDB_ERR_OK;
  rc = m_cursor.current(obj, get_owned);
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

int Sdb_cl::next(bson::BSONObj &obj, my_bool get_owned) {
  int rc = SDB_ERR_OK;
  rc = m_cursor.next(obj, get_owned);
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

int Sdb_cl::bulk_insert(int flag, std::vector<bson::BSONObj> &objs) {
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
                   const bson::BSONObj &hint,
                   const bson::BSONObj &set_on_insert, int flag) {
  int rc = SDB_ERR_OK;
  int retry_times = 2;
retry:
  rc = m_cl.upsert(rule, condition, hint, set_on_insert, flag);
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
                   const bson::BSONObj &hint, int flag) {
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

int Sdb_cl::create_index(const bson::BSONObj &index_def, const CHAR *name,
                         my_bool is_unique, my_bool is_enforced) {
  int rc = SDB_ERR_OK;
  int retry_times = 2;
retry:
  rc = m_cl.createIndex(index_def, name, is_unique, is_enforced);
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

/*
   Test if index is created by v3.2.2 or earlier.
*/
bool Sdb_cl::is_old_version_index(const bson::BSONObj &index_def,
                                  const CHAR *name,
                                  const bson::BSONObj &options) {
  bool rs = false;
  bson::BSONObj info;
  try {
    do {
      int rc = SDB_ERR_OK;
      rc = m_cl.getIndex(name, info);
      if (rc != SDB_ERR_OK) {
        break;
      }

      bson::BSONObj def_obj = info.getField(SDB_FIELD_IDX_DEF).Obj();
      bson::BSONObj key_obj = def_obj.getField(SDB_FIELD_KEY).Obj();
      if (!key_obj.equal(index_def)) {
        break;
      }

      bool opt_unique = options.getField(SDB_FIELD_UNIQUE).booleanSafe();
      bool def_unique = def_obj.getField(SDB_FIELD_UNIQUE2).booleanSafe();
      if (opt_unique != def_unique) {
        break;
      }

      bool opt_not_null = options.getField(SDB_FIELD_NOT_NULL).booleanSafe();
      bool def_not_null = def_obj.getField(SDB_FIELD_NOT_NULL).booleanSafe();
      if (!(opt_not_null && !def_not_null)) {
        break;
      }

      rs = true;

    } while (0);
  } catch (bson::assertion e) {
    DBUG_ASSERT(false);
  }

  return rs;
}

int Sdb_cl::create_index(const bson::BSONObj &index_def, const CHAR *name,
                         const bson::BSONObj &options) {
  int rc = SDB_ERR_OK;
  int retry_times = 2;
retry:
  rc = m_cl.createIndex(index_def, name, options);
  if (SDB_IXM_REDEF == rc ||
      (SDB_IXM_EXIST == rc && is_old_version_index(index_def, name, options))) {
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

int Sdb_cl::drop_index(const char *name) {
  int rc = SDB_ERR_OK;
  int retry_times = 2;
retry:
  rc = m_cl.dropIndex(name);
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

int Sdb_cl::set_attributes(const bson::BSONObj &options) {
  int rc = SDB_ERR_OK;
  int retry_times = 2;
retry:
  rc = m_cl.setAttributes(options);
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

int Sdb_cl::drop_auto_increment(const char *field_name) {
  int rc = SDB_ERR_OK;
  int retry_times = 2;
retry:
  rc = m_cl.dropAutoIncrement(field_name);
  if (SDB_AUTOINCREMENT_FIELD_NOT_EXIST == rc) {
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

int Sdb_cl::create_auto_increment(const bson::BSONObj &options) {
  int rc = SDB_ERR_OK;
  int retry_times = 2;
retry:
  rc = m_cl.createAutoIncrement(options);
  if (SDB_AUTOINCREMENT_FIELD_CONFLICT == rc) {
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

int Sdb_cl::get_count(long long &count, const bson::BSONObj &condition,
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
