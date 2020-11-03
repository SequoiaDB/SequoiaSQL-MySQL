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
#include "ha_sdb_conf.h"
#include "ha_sdb_util.h"
#include "ha_sdb_errcode.h"
#include "ha_sdb.h"
#include "ha_sdb_def.h"

static int sdb_proc_id() {
#ifdef _WIN32
  return GetCurrentProcessId();
#else
  return getpid();
#endif
}

/* caller should catch the exception. */
void Sdb_session_attrs::attrs_to_obj(bson::BSONObj *attr_obj) {
  bson::BSONObjBuilder builder(160);

  if (test_attrs_mask(SDB_SESSION_ATTR_SOURCE_MASK)) {
    builder.append(SDB_SESSION_ATTR_SOURCE, source_str);
  }
  if (test_attrs_mask(SDB_SESSION_ATTR_TRANS_ISOLATION_MASK)) {
    builder.append(SDB_SESSION_ATTR_TRANS_ISOLATION,
                   (long long int)trans_isolation);
  }
  if (test_attrs_mask(SDB_SESSION_ATTR_TRANS_AUTO_COMMIT_MASK)) {
    builder.append(SDB_SESSION_ATTR_TRANS_AUTO_COMMIT, trans_auto_commit);
  }
  if (test_attrs_mask(SDB_SESSION_ATTR_TRANS_AUTO_ROLLBACK_MASK)) {
    builder.append(SDB_SESSION_ATTR_TRANS_AUTO_ROLLBACK, trans_auto_rollback);
  }
  if (test_attrs_mask(SDB_SESSION_ATTR_TRANS_TIMEOUT_MASK)) {
    builder.append(SDB_SESSION_ATTR_TRANS_TIMEOUT, trans_timeout);
  }
  if (test_attrs_mask(SDB_SESSION_ATTR_TRANS_USE_RBS_MASK)) {
    builder.append(SDB_SESSION_ATTR_TRANS_USE_RBS, trans_use_rollback_segments);
  }
  *attr_obj = builder.obj();
}

void Sdb_session_attrs::save_last_attrs() {
  if (test_attrs_mask(SDB_SESSION_ATTR_TRANS_ISOLATION_MASK)) {
    last_trans_isolation = trans_isolation;
  }
  if (test_attrs_mask(SDB_SESSION_ATTR_TRANS_TIMEOUT_MASK)) {
    last_trans_timeout = trans_timeout;
  }
  if (test_attrs_mask(SDB_SESSION_ATTR_TRANS_AUTO_COMMIT_MASK)) {
    last_trans_auto_commit = trans_auto_commit;
  }
  if (test_attrs_mask(SDB_SESSION_ATTR_TRANS_USE_RBS_MASK)) {
    last_trans_use_rollback_segments = trans_use_rollback_segments;
  }
}

Sdb_conn::Sdb_conn(my_thread_id _tid, bool server_ha_conn)
    : m_transaction_on(false),
      m_thread_id(_tid),
      pushed_autocommit(false),
      m_is_authenticated(false),
      m_is_server_ha_conn(server_ha_conn) {
  // Only init the first bit to save cpu.
  errmsg[0] = '\0';
  rollback_on_timeout = false;
}

Sdb_conn::~Sdb_conn() {}

sdbclient::sdb &Sdb_conn::get_sdb() {
  return m_connection;
}

my_thread_id Sdb_conn::thread_id() {
  return m_thread_id;
}

int Sdb_conn::retry(boost::function<int()> func) {
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
    if (!m_transaction_on && retry_times-- > 0 && 0 == connect()) {
      goto retry;
    }
  }
  convert_sdb_code(rc);
  goto done;
}

int Sdb_conn::connect() {
  int rc = SDB_ERR_OK;
  String password;
  Sdb_session_attrs *session_attrs = NULL;

  if (!(is_valid() && is_authenticated())) {
    m_transaction_on = false;
    ha_sdb_conn_addrs conn_addrs;
    rc = conn_addrs.parse_conn_addrs(sdb_conn_str);
    if (SDB_ERR_OK != rc) {
      snprintf(errmsg, sizeof(errmsg),
               "Failed to parse connection addresses, rc=%d", rc);
      goto error;
    }

    rc = sdb_get_password(password);
    if (SDB_ERR_OK != rc) {
      snprintf(errmsg, sizeof(errmsg), "Failed to decrypt password, rc=%d", rc);
      goto error;
    }
    if (password.length()) {
      rc = m_connection.connect(conn_addrs.get_conn_addrs(),
                                conn_addrs.get_conn_num(), sdb_user,
                                password.ptr());
    } else {
      rc = m_connection.connect(conn_addrs.get_conn_addrs(),
                                conn_addrs.get_conn_num(), sdb_user,
                                sdb_password_token, sdb_password_cipherfile);
    }
    if (SDB_ERR_OK != rc) {
      if (SDB_NET_CANNOT_CONNECT != rc) {
        switch (rc) {
          case SDB_FNE:
            snprintf(errmsg, sizeof(errmsg), "Cipherfile not exist, rc=%d", rc);
            break;
          case SDB_AUTH_USER_NOT_EXIST:
            snprintf(errmsg, sizeof(errmsg),
                     "User specified is not exist, you can add the user by "
                     "sdbpasswd tool, rc=%d",
                     rc);
            break;
          case SDB_PERM:
            snprintf(
                errmsg, sizeof(errmsg),
                "Permission error, you can check if you have permission to "
                "access cipherfile, rc=%d",
                rc);
            break;
          default:
            snprintf(errmsg, sizeof(errmsg),
                     "Failed to connect to sequoiadb, rc=%d", rc);
            break;
        }
        rc = SDB_AUTH_AUTHORITY_FORBIDDEN;
      }
      goto error;
    }

    static bool support_source_and_trans_attr = true;

    if (support_source_and_trans_attr) {
      const char *hostname = NULL;
      int hostname_len = (int)strlen(glob_hostname);

      if (0 >= hostname_len) {
        static char empty[] = "";
        hostname = empty;
      } else {
        hostname = glob_hostname;
      }
      session_attrs = get_session_attrs();
      session_attrs->set_source(hostname, sdb_proc_id(),
                                (ulonglong)thread_id());
      session_attrs->set_trans_auto_rollback(false);
      /* Server HA conn:
         1. use transaction.
         2. default lock wait timeout, use rbs.
      */
      if (m_is_server_ha_conn) {
        session_attrs->set_trans_auto_commit(true);
      } else {
        session_attrs->set_trans_auto_commit(
            sdb_use_transaction(current_thd) ? true : false);
        session_attrs->set_trans_timeout(sdb_lock_wait_timeout(current_thd));
        session_attrs->set_trans_use_rollback_segments(
            sdb_use_rollback_segments(current_thd));
      }
      rc = set_my_session_attr();
      // No such options before sdb v3.2.4. Ignore it.
      if (SDB_INVALIDARG == get_sdb_code(rc)) {
        support_source_and_trans_attr = false;
        rc = SDB_ERR_OK;
      }

      if (SDB_ERR_OK != rc) {
        snprintf(errmsg, sizeof(errmsg), "Failed to set session attr, rc=%d",
                 rc);
        goto error;
      }
    }
    m_is_authenticated = true;
  }

done:
  return rc;
error:
  convert_sdb_code(rc);
  m_connection.disconnect();
  goto done;
}

int Sdb_conn::set_my_session_attr() {
  bson::BSONObj attrs_obj;
  int rc = SDB_ERR_OK;
  Sdb_session_attrs *session_attrs = NULL;

  try {
    session_attrs = get_session_attrs();
    if (session_attrs->get_attr_count()) {
      session_attrs->attrs_to_obj(&attrs_obj);
      rc = set_session_attr(attrs_obj);
      if (SDB_OK == rc) {
        session_attrs->save_last_attrs();
      } else {
        snprintf(errmsg, sizeof(errmsg),
                 "Failed to set session attr, attr obj:%s, rc=%d",
                 attrs_obj.toString(false, false, false).c_str(), rc);
        goto error;
      }
    }
  } catch (std::bad_alloc &e) {
    snprintf(errmsg, sizeof(errmsg),
             "Failed to build session option obj, exception=%s", e.what());
    rc = SDB_ERR_OOM;
    goto error;

  } catch (std::exception &e) {
    snprintf(errmsg, sizeof(errmsg),
             "Failed to build session option obj, exception=%s", e.what());
    rc = SDB_ERR_BUILD_BSON;
    goto error;
  }

done:
  session_attrs->clear_args();
  return rc;
error:
  goto done;
}

int Sdb_conn::begin_transaction(uint tx_isolation) {
  DBUG_ENTER("Sdb_conn::begin_transaction");
  int rc = SDB_ERR_OK;
  int retry_times = 2;
  ulong tx_iso = SDB_TRANS_ISO_RU;
  bson::BSONObj option;
  bson::BSONObjBuilder builder(32);
  Sdb_session_attrs *session_attrs = NULL;
  static bool support_trans_and_timeout_attr = true;
  bool use_transaction = false;

  set_rollback_on_timeout(false);
  /*
    Use transactions in the case of:
    1. Server HA conns always use transaction.
    2. Normal conns with sequoiadb_use_transaction on.
  */
  use_transaction = m_is_server_ha_conn || sdb_use_transaction(current_thd);
  if (use_transaction && ISO_SERIALIZABLE == tx_isolation) {
    rc = SDB_ERR_NOT_ALLOWED;
    snprintf(errmsg, sizeof(errmsg),
             "SequoiaDB engine not support transaction "
             "serializable isolation. Please set transaction_isolation "
             "to other level and restart transaction");
    goto error;
  }

  if (support_trans_and_timeout_attr) {
    session_attrs = get_session_attrs();
    /*
      TODO:
      Temporary solution to set session isolation and other session attrs in
      two phase because of SEQUOIASQLMAINSTREAM-792.
      set all the session attrs in one phase after SEQUOIASQLMAINSTREAM-792
      later.
    */
    if (use_transaction) {
      /*Set the trans isolation when the isolation has changed*/
      tx_iso = convert_to_sdb_isolation(tx_isolation);
      session_attrs->set_trans_isolation(tx_iso);
      rc = set_my_session_attr();
      // No such options before sdb v3.2.4 Ignore it.
      if (SDB_INVALIDARG == get_sdb_code(rc)) {
        support_trans_and_timeout_attr = false;
        rc = SDB_ERR_OK;
      }

      if (rc != SDB_OK) {
        snprintf(errmsg, sizeof(errmsg),
                 "Failed to set session attributes, rc=%d", rc);
        goto error;
      }
    }

    /* conns attrs:
       1. Normal conns: TransTimeout/TransAutocommit/TransUseRBS,
                        take effect here.
       2. Server HA conns: TransAutocommit(true)
                           TransIsolation(rc)
                           all other attrs use default value.
    */
    if (!m_is_server_ha_conn) {
      /*Set the lock wait timeout when the sequoiadb_lock_wait_timeout has
       * changed*/
      session_attrs->set_trans_timeout(sdb_lock_wait_timeout(current_thd));
      if (sdb_use_transaction(current_thd)) {
        /*Set the trans_auto_commit when the sequoiadb_use_transaction has
         * changed*/
        session_attrs->set_trans_auto_commit(true);
        session_attrs->set_trans_use_rollback_segments(
            sdb_use_rollback_segments(current_thd));
      } else {
        session_attrs->set_trans_auto_commit(false);
      }
      rc = set_my_session_attr();
      // No such options before sdb v3.2.4. Ignore it.
      if (SDB_INVALIDARG == get_sdb_code(rc)) {
        support_trans_and_timeout_attr = false;
        rc = SDB_ERR_OK;
      }

      if (rc != SDB_OK) {
        snprintf(errmsg, sizeof(errmsg),
                 "Failed to set session attributes, rc=%d", rc);
        goto error;
      }
    }

    if (!use_transaction) {
      goto done;
    }
  }

  while (!m_transaction_on) {
    if (pushed_autocommit) {
      m_transaction_on = true;
    } else {
      rc = m_connection.transactionBegin();
      if (SDB_ERR_OK == rc) {
        m_transaction_on = true;
      } else if (IS_SDB_NET_ERR(rc) && --retry_times > 0) {
        connect();
      } else {
        goto error;
      }
    }
    DBUG_PRINT("Sdb_conn::info",
               ("Begin transaction, flag: %d", pushed_autocommit));
  }

done:
  DBUG_RETURN(rc);
error:
  convert_sdb_code(rc);
  goto done;
}

int Sdb_conn::commit_transaction(const bson::BSONObj &hint) {
  DBUG_ENTER("Sdb_conn::commit_transaction");
  int rc = SDB_ERR_OK;
  if (m_transaction_on) {
    m_transaction_on = false;
    if (!pushed_autocommit) {
      rc = m_connection.transactionCommit(hint);
      if (rc != SDB_ERR_OK) {
        goto error;
      }
    }
    DBUG_PRINT("Sdb_conn::info",
               ("Commit transaction, flag: %d", pushed_autocommit));
    pushed_autocommit = false;
  }

done:
  DBUG_RETURN(rc);
error:
  if (IS_SDB_NET_ERR(rc)) {
    connect();
  }
  convert_sdb_code(rc);
  goto done;
}

int Sdb_conn::rollback_transaction() {
  DBUG_ENTER("Sdb_conn::rollback_transaction");
  if (m_transaction_on) {
    int rc = SDB_ERR_OK;
    m_transaction_on = false;
    if (!pushed_autocommit) {
      rc = m_connection.transactionRollback();
      if (IS_SDB_NET_ERR(rc)) {
        connect();
      }
    }
    DBUG_PRINT("Sdb_conn::info",
               ("Rollback transaction, flag: %d", pushed_autocommit));
    pushed_autocommit = false;
  }
  DBUG_RETURN(0);
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
  rc = m_connection.getCollectionSpace(cs_name, cs);
  if (SDB_DMS_CS_NOTEXIST == rc) {
    rc = m_connection.createCollectionSpace(cs_name, SDB_PAGESIZE_64K, cs);
    if (SDB_OK == rc) {
      new_cs = true;
    }
  }

  if (SDB_ERR_OK != rc && SDB_DMS_CS_EXIST != rc) {
    goto error;
  }

  rc = cs.createCollection(cl_name, options, cl);
  if (SDB_DMS_EXIST == rc) {
    rc = cs.getCollection(cl_name, cl);
    /* CS cached on sdbclient. so SDB_DMS_CS_NOTEXIST maybe retuned here. */
  } else if (SDB_DMS_CS_NOTEXIST == rc) {
    rc = m_connection.createCollectionSpace(cs_name, SDB_PAGESIZE_64K, cs);
    if (SDB_OK == rc) {
      new_cs = true;
    } else if (SDB_DMS_CS_EXIST != rc) {
      goto error;
    }
    goto retry;
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

int conn_rename_cl(sdbclient::sdb *connection, char *cs_name, char *old_cl_name,
                   char *new_cl_name) {
  int rc = SDB_ERR_OK;
  sdbclient::sdbCollectionSpace cs;

  rc = connection->getCollectionSpace(cs_name, cs);
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
  goto done;
}

int Sdb_conn::rename_cl(char *cs_name, char *old_cl_name, char *new_cl_name) {
  return retry(boost::bind(conn_rename_cl, &m_connection, cs_name, old_cl_name,
                           new_cl_name));
}

int conn_drop_cl(sdbclient::sdb *connection, char *cs_name, char *cl_name) {
  int rc = SDB_ERR_OK;
  sdbclient::sdbCollectionSpace cs;

  rc = connection->getCollectionSpace(cs_name, cs);
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
  goto done;
}

int Sdb_conn::drop_cl(char *cs_name, char *cl_name) {
  return retry(boost::bind(conn_drop_cl, &m_connection, cs_name, cl_name));
}

int conn_drop_cs(sdbclient::sdb *connection, char *cs_name) {
  int rc = connection->dropCollectionSpace(cs_name);
  if (SDB_DMS_CS_NOTEXIST == rc) {
    rc = SDB_ERR_OK;
  }
  return rc;
}

int Sdb_conn::drop_cs(char *cs_name) {
  return retry(boost::bind(conn_drop_cs, &m_connection, cs_name));
}

int conn_exec(sdbclient::sdb *connection, const char *sql,
              sdbclient::sdbCursor *cursor) {
  return connection->exec(sql, *cursor);
}

int Sdb_conn::get_cl_statistics(char *cs_name, char *cl_name,
                                Sdb_statistics &stats) {
  static bool support_get_detail = true;
  int rc = SDB_ERR_OK;
  if (support_get_detail) {
    rc = get_cl_stats_by_get_detail(cs_name, cl_name, stats);
    if (SDB_INVALIDARG == get_sdb_code(rc)) {
      support_get_detail = false;
    }
  }
  if (!support_get_detail) {
    rc = get_cl_stats_by_snapshot(cs_name, cl_name, stats);
  }
  return rc;
}

int Sdb_conn::get_cl_stats_by_get_detail(char *cs_name, char *cl_name,
                                         Sdb_statistics &stats) {
  static const int PAGE_SIZE_MIN = 4096;
  static const int PAGE_SIZE_MAX = 65536;

  int rc = SDB_ERR_OK;
  sdbclient::sdbCursor cursor;
  bson::BSONObj obj;
  Sdb_cl cl;

  DBUG_ASSERT(NULL != cs_name);
  DBUG_ASSERT(strlength(cs_name) != 0);

  rc = get_cl(cs_name, cl_name, cl);
  if (rc != SDB_ERR_OK) {
    goto error;
  }

  rc = cl.get_detail(cursor);
  if (rc != SDB_ERR_OK) {
    goto error;
  }

  stats.page_size = PAGE_SIZE_MAX;
  stats.total_data_pages = 0;
  stats.total_index_pages = 0;
  stats.total_data_free_space = 0;
  stats.total_records = 0;

  try {
    while (!(rc = cursor.next(obj, false))) {
      // Reject SequoiaDB in standalone mode. It's not supported yet.
      if (obj.getField(SDB_FIELD_UNIQUEID).numberLong() <= 0) {
        rc = SDB_RTN_COORD_ONLY;
        break;
      }

      bson::BSONObjIterator it(obj.getField(SDB_FIELD_DETAILS).Obj());
      if (!it.more()) {
        continue;
      }
      bson::BSONObj detail = it.next().Obj();
      bson::BSONObjIterator iter(detail);

      int page_size = 0;
      int total_data_pages = 0;
      int total_index_pages = 0;
      longlong total_data_free_space = 0;
      longlong total_records = 0;

      while (iter.more()) {
        bson::BSONElement ele = iter.next();
        if (!strcmp(ele.fieldName(), SDB_FIELD_PAGE_SIZE)) {
          page_size = ele.numberInt();
        } else if (!strcmp(ele.fieldName(), SDB_FIELD_TOTAL_DATA_PAGES)) {
          total_data_pages = ele.numberInt();
        } else if (!strcmp(ele.fieldName(), SDB_FIELD_TOTAL_INDEX_PAGES)) {
          total_index_pages = ele.numberInt();
        } else if (!strcmp(ele.fieldName(), SDB_FIELD_TOTAL_DATA_FREE_SPACE)) {
          total_data_free_space = ele.numberLong();
        } else if (!strcmp(ele.fieldName(), SDB_FIELD_TOTAL_RECORDS)) {
          total_records = ele.numberLong();
        }
      }

      // When exception occurs, page size may be 0. Fix it to default.
      if (0 == page_size) {
        page_size = PAGE_SIZE_MAX;
      }
      // For main cl, each data node may have different page size,
      // so calculate pages base on the min page size.
      if (page_size < stats.page_size) {
        stats.page_size = page_size;
      }
      stats.total_data_pages +=
          (total_data_pages * (page_size / PAGE_SIZE_MIN));
      stats.total_index_pages +=
          (total_index_pages * (page_size / PAGE_SIZE_MIN));

      stats.total_data_free_space += total_data_free_space;
      stats.total_records += total_records;
    }
  } catch (std::bad_alloc &e) {
    rc = SDB_ERR_OOM;
    snprintf(errmsg, sizeof(errmsg),
             "Failed to get "
             "statistics of collection, exception:%s.",
             e.what());
    goto error;
  } catch (std::exception &e) {
    snprintf(errmsg, sizeof(errmsg),
             "Failed to get "
             "statistics of collection, exception:%s.",
             e.what());
    rc = SDB_ERR_BUILD_BSON;
    goto error;
  }

  if (SDB_DMS_EOC == rc) {
    rc = SDB_ERR_OK;
  }
  if (rc != SDB_ERR_OK) {
    goto error;
  }

  stats.total_data_pages /= (stats.page_size / PAGE_SIZE_MIN);
  stats.total_index_pages /= (stats.page_size / PAGE_SIZE_MIN);

done:
  cursor.close();
  return rc;
error:
  convert_sdb_code(rc);
  goto done;
}

int Sdb_conn::get_cl_stats_by_snapshot(char *cs_name, char *cl_name,
                                       Sdb_statistics &stats) {
  static const char NORMAL_CL_STATS_SQL[] =
      "select T.Details.$[0].PageSize as PageSize, "
      "T.Details.$[0].TotalDataPages as TotalDataPages,"
      "T.Details.$[0].TotalIndexPages as TotalIndexPages, "
      "T.Details.$[0].TotalDataFreeSpace as TotalDataFreeSpace, "
      "T.Details.$[0].TotalRecords as TotalRecords "
      "from $SNAPSHOT_CL as T "
      "where T.NodeSelect='primary' and T.Name='%s.%s' split by T.Details";

  static const char MAIN_CL_STATS_SQL[] =
      "select CL.PageSize,"
      "sum(CL.TotalDataPages) as TotalDataPages,"
      "sum(CL.TotalIndexPages) as TotalIndexPages,"
      "sum(CL.TotalDataFreeSpace) as TotalDataFreeSpace,"
      "sum(CL.TotalRecords) as TotalRecords "
      "from "
      "("
      "select Name from $SNAPSHOT_CATA "
      "where MainCLName='%s.%s' "
      ") as CATA "
      "inner join "
      "("
      "select T.Name,"
      "T.Details.$[0].PageSize as PageSize,"
      "T.Details.$[0].TotalDataPages as TotalDataPages,"
      "T.Details.$[0].TotalIndexPages as TotalIndexPages,"
      "T.Details.$[0].TotalDataFreeSpace as TotalDataFreeSpace,"
      "T.Details.$[0].TotalRecords as TotalRecords "
      "from $SNAPSHOT_CL as T "
      "where T.NodeSelect='primary' split by T.Details"
      ") as CL "
      "on CATA.Name=CL.Name";

  int rc = SDB_ERR_OK;
  sdbclient::sdbCursor cursor;
  bson::BSONObj obj;
  char normal_cl_stats_sql[sizeof(NORMAL_CL_STATS_SQL) +
                           SDB_CL_FULL_NAME_MAX_SIZE] = {0};
  char main_cl_stats_sql[sizeof(MAIN_CL_STATS_SQL) +
                         SDB_CL_FULL_NAME_MAX_SIZE] = {0};

  DBUG_ASSERT(NULL != cs_name);
  DBUG_ASSERT(strlength(cs_name) != 0);

  // Try getting statistics as normal cl. If not, try main cl again.
  sprintf(normal_cl_stats_sql, NORMAL_CL_STATS_SQL, cs_name, cl_name);
  rc = retry(
      boost::bind(conn_exec, &m_connection, normal_cl_stats_sql, &cursor));
  if (rc != SDB_ERR_OK) {
    goto error;
  }
  rc = cursor.next(obj, false);
  if (SDB_DMS_EOC == rc) {
    sprintf(main_cl_stats_sql, MAIN_CL_STATS_SQL, cs_name, cl_name);
    rc = retry(
        boost::bind(conn_exec, &m_connection, main_cl_stats_sql, &cursor));
    if (rc != SDB_ERR_OK) {
      goto error;
    }
    rc = cursor.next(obj, false);
  }
  if (rc != SDB_ERR_OK) {
    goto error;
  }

  stats.page_size = obj.getField(SDB_FIELD_PAGE_SIZE).numberInt();
  stats.total_data_pages = obj.getField(SDB_FIELD_TOTAL_DATA_PAGES).numberInt();
  stats.total_index_pages =
      obj.getField(SDB_FIELD_TOTAL_INDEX_PAGES).numberInt();
  stats.total_data_free_space =
      obj.getField(SDB_FIELD_TOTAL_DATA_FREE_SPACE).numberLong();
  stats.total_records = obj.getField(SDB_FIELD_TOTAL_RECORDS).numberLong();

done:
  cursor.close();
  return rc;
error:
  convert_sdb_code(rc);
  goto done;
}

int conn_snapshot(sdbclient::sdb *connection, bson::BSONObj *obj, int snap_type,
                  const bson::BSONObj *condition, const bson::BSONObj *selected,
                  const bson::BSONObj *order_by, const bson::BSONObj *hint,
                  longlong num_to_skip, char *errmsg) {
  int rc = SDB_ERR_OK;
  sdbclient::sdbCursor cursor;

  rc = connection->getSnapshot(cursor, snap_type, *condition, *selected,
                               *order_by, *hint, num_to_skip, 1);
  if (rc != SDB_ERR_OK) {
    goto error;
  }
  try {
    rc = cursor.next(*obj);
    if (rc != SDB_ERR_OK) {
      goto error;
    }
  } catch (std::bad_alloc &e) {
    rc = SDB_ERR_OOM;
    snprintf(errmsg, SDB_ERR_BUFF_SIZE, "Failed to get snapshot, exception:%s",
             e.what());
    goto error;
  } catch (std::exception &e) {
    snprintf(errmsg, SDB_ERR_BUFF_SIZE, "Failed to get snapshot, exception:%s",
             e.what());
    rc = SDB_ERR_BUILD_BSON;
    goto error;
  }
done:
  return rc;
error:
  goto done;
}

int Sdb_conn::snapshot(bson::BSONObj &obj, int snap_type,
                       const bson::BSONObj &condition,
                       const bson::BSONObj &selected,
                       const bson::BSONObj &order_by, const bson::BSONObj &hint,
                       longlong num_to_skip) {
  return retry(boost::bind(conn_snapshot, &m_connection, &obj, snap_type,
                           &condition, &selected, &order_by, &hint, num_to_skip,
                           get_err_msg()));
}

int conn_get_last_result_obj(sdbclient::sdb *connection, bson::BSONObj *result,
                             bool get_owned) {
  return connection->getLastResultObj(*result, get_owned);
}

int Sdb_conn::get_last_result_obj(bson::BSONObj &result, bool get_owned) {
  return retry(
      boost::bind(conn_get_last_result_obj, &m_connection, &result, get_owned));
}

int conn_get_session_attr(sdbclient::sdb *connection, bson::BSONObj *option) {
  return connection->getSessionAttr(*option);
}

int Sdb_conn::get_session_attr(bson::BSONObj &option) {
  return retry(boost::bind(conn_get_session_attr, &m_connection, &option));
}

int conn_set_session_attr(sdbclient::sdb *connection,
                          const bson::BSONObj *option) {
  return connection->setSessionAttr(*option);
}

int Sdb_conn::set_session_attr(const bson::BSONObj &option) {
  return retry(boost::bind(conn_set_session_attr, &m_connection, &option));
}

int conn_interrupt(sdbclient::sdb *connection) {
  return connection->interruptOperation();
}

int Sdb_conn::interrupt_operation() {
  return retry(boost::bind(conn_interrupt, &m_connection));
}

int conn_analyze(sdbclient::sdb *connection, const bson::BSONObj *options) {
  return connection->analyze(*options);
}

int Sdb_conn::analyze(const bson::BSONObj &options) {
  return retry(boost::bind(conn_analyze, &m_connection, &options));
}
