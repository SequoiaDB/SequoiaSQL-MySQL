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

#include "sdb_conn.h"
#include <sql_class.h>
#include <client.hpp>
#include <sstream>
#include "sdb_cl.h"
#include "sdb_conf.h"
#include "sdb_util.h"
#include "sdb_errcode.h"
#include "sdb_conf.h"
#include "sdb_log.h"
#include "ha_sdb.h"

Sdb_conn::Sdb_conn(my_thread_id _tid)
    : m_transaction_on(false), m_thread_id(_tid) {}

Sdb_conn::~Sdb_conn() {}

sdbclient::sdb &Sdb_conn::get_sdb() {
  return m_connection;
}

my_thread_id Sdb_conn::thread_id() {
  return m_thread_id;
}

int Sdb_conn::connect() {
  int rc = SDB_ERR_OK;
  String password;

  if (!m_connection.isValid()) {
    m_transaction_on = false;
    Sdb_conn_addrs conn_addrs;
    rc = conn_addrs.parse_conn_addrs(sdb_conn_str);
    if (SDB_ERR_OK != rc) {
      SDB_LOG_ERROR("Failed to parse connection addresses, rc=%d", rc);
      goto error;
    }

    rc = sdb_get_password(password);
    if (SDB_ERR_OK != rc) {
      SDB_LOG_ERROR("Failed to decrypt password, rc=%d", rc);
      goto error;
    }
    rc = m_connection.connect(conn_addrs.get_conn_addrs(),
                              conn_addrs.get_conn_num(), sdb_user,
                              password.ptr());
    if (SDB_ERR_OK != rc) {
      goto error;
    }
  }

done:
  return rc;
error:
  convert_sdb_code(rc);
  goto done;
}

int Sdb_conn::begin_transaction() {
  int rc = SDB_ERR_OK;
  int retry_times = 2;
  while (!m_transaction_on) {
    rc = m_connection.transactionBegin();
    if (SDB_ERR_OK == rc) {
      m_transaction_on = true;
      break;
    } else if (IS_SDB_NET_ERR(rc) && --retry_times > 0) {
      connect();
    } else {
      goto error;
    }
  }

done:
  return rc;
error:
  convert_sdb_code(rc);
  goto done;
}

int Sdb_conn::commit_transaction() {
  int rc = SDB_ERR_OK;
  if (m_transaction_on) {
    m_transaction_on = false;
    rc = m_connection.transactionCommit();
    if (rc != SDB_ERR_OK) {
      goto error;
    }
  }

done:
  return rc;
error:
  if (IS_SDB_NET_ERR(rc)) {
    connect();
  }
  convert_sdb_code(rc);
  goto done;
}

int Sdb_conn::rollback_transaction() {
  if (m_transaction_on) {
    int rc = SDB_ERR_OK;
    m_transaction_on = false;
    rc = m_connection.transactionRollback();
    if (IS_SDB_NET_ERR(rc)) {
      connect();
    }
  }
  return 0;
}

bool Sdb_conn::is_transaction_on() {
  return m_transaction_on;
}

int Sdb_conn::get_cl(char *cs_name, char *cl_name, Sdb_cl &cl) {
  int rc = SDB_ERR_OK;
  cl.close();

  rc = cl.init(this, cs_name, cl_name);
  if (rc != SDB_ERR_OK) {
    goto error;
  }

done:
  return rc;
error:
  if (IS_SDB_NET_ERR(rc)) {
    connect();
  }
  convert_sdb_code(rc);
  goto done;
}

int Sdb_conn::create_cl(char *cs_name, char *cl_name,
                        const bson::BSONObj &options, bool *created_cs,
                        bool *created_cl) {
  int rc = SDB_ERR_OK;
  int retry_times = 2;
  sdbclient::sdbCollectionSpace cs;
  sdbclient::sdbCollection cl;
  bool new_cs = false;
  bool new_cl = false;

retry:
  rc = m_connection.createCollectionSpace(cs_name, 4096, cs);
  if (SDB_DMS_CS_EXIST == rc) {
    rc = m_connection.getCollectionSpace(cs_name, cs);
  } else if (SDB_OK == rc) {
    new_cs = true;
  }
  if (rc != SDB_ERR_OK) {
    goto error;
  }

  rc = cs.createCollection(cl_name, options, cl);
  if (SDB_DMS_EXIST == rc) {
    rc = cs.getCollection(cl_name, cl);
  } else if (SDB_OK == rc) {
    new_cl = true;
  }
  if (rc != SDB_ERR_OK) {
    goto error;
  }

done:
  if (created_cs) {
    *created_cs = new_cs;
  }
  if (created_cl) {
    *created_cl = new_cl;
  }
  return rc;
error:
  if (IS_SDB_NET_ERR(rc)) {
    if (!m_transaction_on && retry_times-- > 0 && 0 == connect()) {
      goto retry;
    }
  }
  convert_sdb_code(rc);
  if (new_cs) {
    drop_cs(cs_name);
    new_cs = false;
    new_cl = false;
  } else if (new_cl) {
    drop_cl(cs_name, cl_name);
    new_cl = false;
  }
  goto done;
}

int Sdb_conn::rename_cl(char *cs_name, char *old_cl_name, char *new_cl_name) {
  int rc = SDB_ERR_OK;
  int retry_times = 2;
  sdbclient::sdbCollectionSpace cs;
  sdbclient::sdbCollection cl;

retry:
  rc = m_connection.getCollectionSpace(cs_name, cs);
  if (rc != SDB_ERR_OK) {
    goto error;
  }

  rc = cs.renameCollection(old_cl_name, new_cl_name);
  if (rc != SDB_ERR_OK) {
    goto error;
  }

done:
  return rc;
error:
  if (IS_SDB_NET_ERR(rc)) {
    if (!m_transaction_on && retry_times-- > 0 && 0 == connect()) {
      goto retry;
    }
  }
  convert_sdb_code(rc);
  goto done;
}

int Sdb_conn::drop_cl(char *cs_name, char *cl_name) {
  int rc = SDB_ERR_OK;
  int retry_times = 2;
  sdbclient::sdbCollectionSpace cs;

retry:
  rc = m_connection.getCollectionSpace(cs_name, cs);
  if (rc != SDB_ERR_OK) {
    if (SDB_DMS_CS_NOTEXIST == rc) {
      // There is no specified collection space, igonre the error.
      rc = 0;
      goto done;
    }
    goto error;
  }

  rc = cs.dropCollection(cl_name);
  if (rc != SDB_ERR_OK) {
    if (SDB_DMS_NOTEXIST == rc) {
      // There is no specified collection, igonre the error.
      rc = 0;
      goto done;
    }
    goto error;
  }

done:
  return rc;
error:
  if (IS_SDB_NET_ERR(rc)) {
    if (!m_transaction_on && retry_times-- > 0 && 0 == connect()) {
      goto retry;
    }
  }
  convert_sdb_code(rc);
  goto done;
}

int Sdb_conn::drop_cs(char *cs_name) {
  int rc = SDB_ERR_OK;
  rc = m_connection.dropCollectionSpace(cs_name);
  if (rc != SDB_ERR_OK) {
    goto error;
  }
done:
  return rc;
error:
  if (IS_SDB_NET_ERR(rc)) {
    connect();
  }
  convert_sdb_code(rc);
  goto done;
}

int Sdb_conn::get_cl_statistics(char *cs_name, char *cl_name,
                                Sdb_statistics &stats) {
  int rc = SDB_ERR_OK;
  sdbclient::sdbCursor cursor;
  bson::BSONObj condition;
  bson::BSONObj obj;
  int retry_times = 2;
  std::stringstream ss;

  DBUG_ASSERT(NULL != cs_name);
  DBUG_ASSERT(strlength(cs_name) != 0);

  ss << "select CL.PageSize,"
     << "sum(CL.TotalDataPages) as TotalDataPages,"
     << "sum(CL.TotalIndexPages) as TotalIndexPages,"
     << "sum(CL.TotalDataFreeSpace) as TotalDataFreeSpace,"
     << "sum(CL.TotalRecords) as TotalRecords "
     << "from "
     << "("
     << "select Name from $SNAPSHOT_CATA "
     << "where MainCLName="
     << "'" << cs_name << "." << cl_name << "' "
     << "or (IsMainCL is null and Name="
     << "'" << cs_name << "." << cl_name << "')"
     << ") as CATA "
     << "inner join "
     << "("
     << "select T.Name,"
     << "T.Details.$[0].PageSize as PageSize,"
     << "T.Details.$[0].TotalDataPages as TotalDataPages,"
     << "T.Details.$[0].TotalIndexPages as TotalIndexPages,"
     << "T.Details.$[0].TotalDataFreeSpace as TotalDataFreeSpace,"
     << "T.Details.$[0].TotalRecords as TotalRecords "
     << "from $SNAPSHOT_CL as T "
     << "where T.NodeSelect='primary' split by T.Details"
     << ") as CL "
     << "on CATA.Name=CL.Name";

  std::string sql = ss.str();

retry:
  rc = m_connection.exec(sql.c_str(), cursor);
  if (rc != SDB_ERR_OK) {
    goto error;
  }

  rc = cursor.next(obj);
  if (rc != SDB_ERR_OK) {
    goto error;
  }

  stats.page_size = obj.getIntField("PageSize");
  stats.total_data_pages = obj.getIntField("TotalDataPages");
  stats.total_index_pages = obj.getIntField("TotalIndexPages");
  stats.total_data_free_space = obj.getField("TotalDataFreeSpace").numberLong();
  stats.total_records = obj.getField("TotalRecords").numberLong();

done:
  return rc;
error:
  if (IS_SDB_NET_ERR(rc)) {
    if (!m_transaction_on && retry_times-- > 0 && 0 == connect()) {
      goto retry;
    }
  }
  convert_sdb_code(rc);
  goto done;
}
