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
#include "sdb_seq.h"
#include "ha_sdb_conf.h"
#include "ha_sdb_util.h"
#include "ha_sdb_errcode.h"
#include "ha_sdb.h"
#include "ha_sdb_def.h"
#include "ha_sdb_log.h"

extern char *sdb_password;

static int sdb_proc_id() {
#ifdef _WIN32
  return GetCurrentProcessId();
#else
  return getpid();
#endif
}

/* caller should catch the exception. */
void Sdb_session_attrs::attrs_to_obj(bson::BSONObj *attr_obj) {
  bson::BSONObjBuilder builder(240);

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
  if (test_attrs_mask(SDB_SESSION_ATTR_CHECK_CL_VERSION_MASK)) {
    builder.append(SDB_SESSION_ATTR_CHECK_CL_VERSION, check_collection_version);
  }
  if (test_attrs_mask(SDB_SESSION_ATTR_PREFERRED_INSTANCE_MASK)) {
    int value = 0;
    char *str = prefer_inst;
    char *pos = NULL;
    const char *separator = " ,\t";
    bson::BSONArrayBuilder sub_builder(
        builder.subarrayStart(SDB_SESSION_ATTR_PREFERRED_INSTANCE));
    while (str) {
      while ((pos = strsep(&str, separator))) {
        if (0 == *pos) {
          continue;
        }
        value = atoi(pos);
        if (value) {
          sub_builder.append(value);
        } else {
          sub_builder.append(pos);
        }
      }
    }
    sub_builder.doneFast();
  }
  if (test_attrs_mask(SDB_SESSION_ATTR_PREFERRED_INSTANCE_MODE_MASK)) {
    builder.append(SDB_SESSION_ATTR_PREFERRED_INSTABCE_MODE,
                   str_to_lowwer(prefer_inst_mode));
  }
  if (test_attrs_mask(SDB_SESSION_ATTR_PREFERRED_STRICT_MASK)) {
    builder.append(SDB_SESSION_ATTR_PREFERRED_STRICT, prefer_strict);
  }
  if (test_attrs_mask(SDB_SESSION_ATTR_PREFERRED_PERIOD_MASK)) {
    builder.append(SDB_SESSION_ATTR_PREFERRED_PERIOD, prefer_period);
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
  if (test_attrs_mask(SDB_SESSION_ATTR_CHECK_CL_VERSION_MASK)) {
    last_check_collection_version = check_collection_version;
  }
  if (test_attrs_mask(SDB_SESSION_ATTR_PREFERRED_INSTANCE_MASK)) {
    strncpy(last_prefer_inst, prefer_inst, STRING_BUFFER_USUAL_SIZE);
    last_prefer_inst[STRING_BUFFER_USUAL_SIZE - 1] = '\0';
  }
  if (test_attrs_mask(SDB_SESSION_ATTR_PREFERRED_INSTANCE_MODE_MASK)) {
    strncpy(last_prefer_inst_mode, prefer_inst_mode,
            SDB_PREFERRED_INSTANCE_MODE_MAX_SIZE);
    last_prefer_inst_mode[SDB_PREFERRED_INSTANCE_MODE_MAX_SIZE - 1] = '\0';
  }
  if (test_attrs_mask(SDB_SESSION_ATTR_PREFERRED_STRICT_MASK)) {
    last_prefer_strict = prefer_strict;
  }
  if (test_attrs_mask(SDB_SESSION_ATTR_PREFERRED_PERIOD_MASK)) {
    last_prefer_period = prefer_period;
  }
}

Sdb_conn::Sdb_conn(my_thread_id _tid, bool server_ha_conn)
    : m_transaction_on(false),
      m_thread_id(_tid),
      pushed_autocommit(false),
      m_is_authenticated(false),
      m_is_server_ha_conn(server_ha_conn),
      m_check_collection_version(false) {
  // Only init the first bit to save cpu.
  errmsg[0] = '\0';
  rollback_on_timeout = false;
}

Sdb_conn::~Sdb_conn() {
  m_cursor.close();
}

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

    try {
      if (sdb_password && strlen(sdb_password)) {
        rc = sdb_get_password(password);
        if (SDB_ERR_OK != rc) {
          snprintf(errmsg, sizeof(errmsg), "Failed to decrypt password, rc=%d",
                   rc);
          goto error;
        }
        rc = m_connection.connect(conn_addrs.get_conn_addrs(),
                                  conn_addrs.get_conn_num(), sdb_user,
                                  password.ptr());
      } else {
        rc = m_connection.connect(conn_addrs.get_conn_addrs(),
                                  conn_addrs.get_conn_num(), sdb_user,
                                  sdb_password_token, sdb_password_cipherfile);
      }
    }
    SDB_EXCEPTION_CATCHER(rc, "Failed to connect sdb, exception:%s", e.what());

    if (SDB_ERR_OK != rc) {
      if (SDB_NET_CANNOT_CONNECT != rc) {
        switch (rc) {
          case SDB_FNE:
            snprintf(errmsg, sizeof(errmsg), "Cipherfile not exist, rc=%d", rc);
            rc = SDB_AUTH_AUTHORITY_FORBIDDEN;
            break;
          case SDB_AUTH_USER_NOT_EXIST:
            snprintf(errmsg, sizeof(errmsg),
                     "User specified is not exist, you can add the user by "
                     "sdbpasswd tool, rc=%d",
                     rc);
            rc = SDB_AUTH_AUTHORITY_FORBIDDEN;
            break;
          case SDB_PERM:
            snprintf(
                errmsg, sizeof(errmsg),
                "Permission error, you can check if you have permission to "
                "access cipherfile, rc=%d",
                rc);
            rc = SDB_AUTH_AUTHORITY_FORBIDDEN;
            break;
          default:
            snprintf(errmsg, sizeof(errmsg),
                     "Failed to connect to sequoiadb, rc=%d", rc);
            break;
        }
      }
      m_connection.disconnect();
      goto error;
    }

    const char *hostname = NULL;
    int hostname_len = (int)strlen(glob_hostname);

    if (0 >= hostname_len) {
      static char empty[] = "";
      hostname = empty;
    } else {
      hostname = glob_hostname;
    }

    int major = 0, minor = 0, fix = 0;
    rc = sdb_get_version(*this, major, minor, fix);
    if (rc != 0) {
      snprintf(errmsg, sizeof(errmsg), "Failed to begin transaction, rc:%d",
               rc);
      goto error;
    }

    session_attrs = get_session_attrs();
    session_attrs->set_preferred_instance(sdb_preferred_instance(current_thd));
    session_attrs->set_preferred_instance_mode(
        sdb_preferred_instance_mode(current_thd));

    /*The pre version of SequoiaDB(3.2.5) and SequoiaDB(3.4.1) has no
     * Preferred_period attrs.*/
    if (!(major < 3 || (3 == major && minor < 2) ||
          (3 == major && 2 == minor && fix < 5) ||
          (3 == major && 4 == minor && fix < 1))) {
      session_attrs->set_preferred_period(sdb_preferred_period(current_thd));
    }
    /*The pre version of SequoiaDB(3.2) has no Source/Trans/Preferred_strict
     * attrs.*/
    if (!(major < 3 || (3 == major && minor < 2))) {
      /* Sdb restart but not restart mysql client, session_attrs need to reset
        and reset the session attrs. */
      session_attrs->reset();
      session_attrs->set_source(hostname, sdb_proc_id(),
                                (ulonglong)thread_id());
      session_attrs->set_trans_auto_rollback(false);
      session_attrs->set_preferred_strict(sdb_preferred_strict(current_thd));

      /* Server HA conn:
         1. use transaction.
         2. default lock wait timeout, use rbs.
      */
      bool use_transaction =
          m_is_server_ha_conn || sdb_use_transaction(current_thd);
      if (use_transaction) {
        if (m_is_server_ha_conn) {
          session_attrs->set_trans_auto_commit(true, true);
        } else {
          session_attrs->set_trans_auto_commit(true, true);
          session_attrs->set_trans_timeout(sdb_lock_wait_timeout(current_thd));
          session_attrs->set_trans_use_rollback_segments(
              sdb_use_rollback_segments(current_thd), true);
        }
      } else {
        session_attrs->set_trans_auto_commit(false, true);
      }
      if (m_check_collection_version) {
        session_attrs->set_check_collection_version(true);
      }

      rc = set_my_session_attr();
      if (SDB_ERR_OK != rc) {
        m_connection.disconnect();
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
        SDB_LOG_DEBUG("Set session attributes: %s",
                      attrs_obj.toString(false, false).c_str());
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
  int major = 0, minor = 0, fix = 0;
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

  rc = sdb_get_version(*this, major, minor, fix);
  if (rc != 0) {
    snprintf(errmsg, sizeof(errmsg), "Failed to begin transaction, rc:%d", rc);
    goto error;
  }
  /*The pre version of SequoiaDB(3.2) has no Source/Trans attrs.*/
  if (!(major < 3 ||                   // x < 3
        (3 == major && minor < 2))) {  // 3.x < 3.2
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
      tx_iso = convert_to_sdb_isolation(tx_isolation, major);
      session_attrs->set_trans_isolation(tx_iso);

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
        /*Set the trans_auto_commit when the sequoiadb_use_transaction has
         * changed*/
        session_attrs->set_trans_auto_commit(true);
        session_attrs->set_trans_use_rollback_segments(
            sdb_use_rollback_segments(current_thd));
      }
    } else {
      session_attrs->set_trans_auto_commit(false);
    }

    rc = set_my_session_attr();
    if (rc != SDB_OK) {
      snprintf(errmsg, sizeof(errmsg),
               "Failed to begin transaction during setting session attrs");
      goto error;
    }
  }

  if (!use_transaction) {
    goto done;
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

int Sdb_conn::get_cl(const char *cs_name, const char *cl_name, Sdb_cl &cl,
                     const bool check_exist) {
  int rc = SDB_ERR_OK;
  cl.close();

  rc = cl.init(this, cs_name, cl_name, check_exist);
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

int Sdb_conn::create_cl(const char *cs_name, const char *cl_name,
                        const bson::BSONObj &options, bool *created_cs,
                        bool *created_cl) {
  int rc = SDB_ERR_OK;
  int retry_times = 2;
  sdbclient::sdbCollectionSpace cs;
  sdbclient::sdbCollection cl;
  bool new_cs = false;
  bool new_cl = false;

retry:
  try {
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
  }
  SDB_EXCEPTION_CATCHER(
      rc, "Failed to create collection cl name:%s.%s, exception:%s", cs_name,
      cl_name, e.what());
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
    sdb_drop_empty_cs(*this, cs_name);
    new_cs = false;
    new_cl = false;
  } else if (new_cl) {
    drop_cl(cs_name, cl_name);
    new_cl = false;
  }
  goto done;
}

const char *Sdb_conn::get_err_msg() {
  if ('\0' == errmsg[0]) {
    try {
      bson::BSONObj err_obj;
      int rc = m_connection.getLastErrorObj(err_obj);
      if (0 == rc) {
        const char *error_msg = err_obj.getStringField(SDB_FIELD_DETAIL);
        if (error_msg && '\0' != error_msg[0]) {
          snprintf(errmsg, SDB_ERR_BUFF_SIZE, "%s", error_msg);
        } else {
          error_msg = err_obj.getStringField(SDB_FIELD_DESCRIPTION);
          if (error_msg && '\0' != error_msg[0]) {
            snprintf(errmsg, SDB_ERR_BUFF_SIZE, "%s", error_msg);
          } else {
            int error_code = err_obj.getIntField(SDB_FIELD_ERRNO);
            snprintf(errmsg, SDB_ERR_BUFF_SIZE, "No description for error %d",
                     error_code);
          }
        }
      } else if (SDB_DMS_EOC == rc) {
        snprintf(errmsg, SDB_ERR_BUFF_SIZE, "There is no error object");
      } else if (0 != rc) {
        snprintf(errmsg, SDB_ERR_BUFF_SIZE,
                 "Failed to get last error, error code: %d", rc);
      }
    } catch (std::bad_alloc &e) {
      snprintf(errmsg, SDB_ERR_BUFF_SIZE,
               "OOM while fetching error message, exception: %s", e.what());
    } catch (std::exception &e) {
      snprintf(errmsg, SDB_ERR_BUFF_SIZE,
               "Failed to get error object, exception: %s", e.what());
    }
  }
  return errmsg;
}

int conn_rename_cl(sdbclient::sdb *connection, const char *cs_name,
                   const char *old_cl_name, const char *new_cl_name) {
  int rc = SDB_ERR_OK;
  sdbclient::sdbCollectionSpace cs;

  try {
    rc = connection->getCollectionSpace(cs_name, cs);
    if (rc != SDB_ERR_OK) {
      goto error;
    }

    rc = cs.renameCollection(old_cl_name, new_cl_name);
    if (rc != SDB_ERR_OK) {
      goto error;
    }
  }
  SDB_EXCEPTION_CATCHER(
      rc, "Failed to rename collection old name:%s new name:%s, exception:%s",
      old_cl_name, new_cl_name, e.what());
done:
  return rc;
error:
  goto done;
}

int Sdb_conn::rename_cl(const char *cs_name, const char *old_cl_name,
                        const char *new_cl_name) {
  return retry(boost::bind(conn_rename_cl, &m_connection, cs_name, old_cl_name,
                           new_cl_name));
}

int conn_drop_cl(sdbclient::sdb *connection, const char *cs_name,
                 const char *cl_name) {
  int rc = SDB_ERR_OK;
  sdbclient::sdbCollectionSpace cs;

  try {
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
  }
  SDB_EXCEPTION_CATCHER(rc, "Failed to drop collection, exception:%s",
                        e.what());
done:
  return rc;
error:
  goto done;
}

int Sdb_conn::drop_cl(const char *cs_name, const char *cl_name) {
  return retry(boost::bind(conn_drop_cl, &m_connection, cs_name, cl_name));
}

#ifdef IS_MARIADB
int Sdb_conn::get_seq(const char *cs_name, const char *table_name,
                      char *sequence_name, Sdb_seq &seq) {
  int rc = SDB_ERR_OK;

  rc = sdb_rebuild_sequence_name(this, cs_name, table_name, sequence_name);
  if (rc) {
    goto error;
  }

  rc = seq.init(this, sequence_name);
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

int Sdb_conn::create_seq(const char *cs_name, const char *table_name,
                         char *sequence_name, const bson::BSONObj &options,
                         bool *created_cs, bool *created_seq) {
  int rc = SDB_ERR_OK;
  int retry_times = 2;
  sdbclient::sdbCollectionSpace cs;
  sdbclient::sdbSequence seq;
  sdbclient::sdbCollection cl;
  bool new_cs = false;
  bool new_seq = false;

retry:
  try {
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

    rc = sdb_rebuild_sequence_name(this, cs_name, table_name, sequence_name);
    if (rc) {
      goto error;
    }

    rc = m_connection.createSequence(sequence_name, options, seq);
    if (SDB_SEQUENCE_EXIST == rc) {
      rc = m_connection.getSequence(sequence_name, seq);
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
      new_seq = true;
    }

    if (rc != SDB_ERR_OK) {
      goto error;
    }
  }
  SDB_EXCEPTION_CATCHER(rc, "Failed to create sequence, exception:%s",
                        e.what());

done:
  if (created_cs) {
    *created_cs = new_cs;
  }
  if (created_seq) {
    *created_seq = new_seq;
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
    sdb_drop_empty_cs(*this, cs_name);
    new_cs = false;
    new_seq = false;
  } else if (new_seq) {
    m_connection.dropSequence(sequence_name);
    new_seq = false;
  }
  goto done;
}

int conn_rename_seq(Sdb_conn *conn, sdbclient::sdb *connection,
                    const char *cs_name, const char *old_table_name,
                    const char *new_table_name) {
  int rc = SDB_ERR_OK;
  sdbclient::sdbCollectionSpace cs;
  char old_sequence_name[SDB_CL_NAME_MAX_SIZE + 1] = {0};
  char new_sequence_name[SDB_CL_NAME_MAX_SIZE + 1] = {0};

  try {
    rc = connection->getCollectionSpace(cs_name, cs);
    if (rc) {
      goto error;
    }
  }
  SDB_EXCEPTION_CATCHER(rc, "Failed to get collection space, exception:%s",
                        e.what());

  rc = sdb_rebuild_sequence_name(conn, cs_name, old_table_name,
                                 old_sequence_name);
  if (rc) {
    goto error;
  }
  rc = sdb_rebuild_sequence_name(conn, cs_name, new_table_name,
                                 new_sequence_name);
  if (rc) {
    goto error;
  }

  rc = connection->renameSequence(old_sequence_name, new_sequence_name);
  if (rc != SDB_ERR_OK) {
    goto error;
  }

done:
  return rc;
error:
  goto done;
}

int Sdb_conn::rename_seq(const char *cs_name, const char *old_seq_name,
                         const char *new_seq_name) {
  return retry(boost::bind(conn_rename_seq, this, &m_connection, cs_name,
                           old_seq_name, new_seq_name));
}

int conn_drop_seq(Sdb_conn *conn, sdbclient::sdb *connection,
                  const char *cs_name, const char *table_name) {
  int rc = SDB_ERR_OK;
  char sequence_name[SDB_CL_NAME_MAX_SIZE + 1] = "";

  rc = sdb_rebuild_sequence_name(conn, cs_name, table_name, sequence_name);
  if (rc) {
    if (SDB_DMS_CS_NOTEXIST == rc) {
      // There is no specified collection space, igonre the error.
      rc = SDB_ERR_OK;
      goto done;
    }
    goto error;
  }

  rc = connection->dropSequence(sequence_name);
  if (rc != SDB_ERR_OK) {
    if (SDB_SEQUENCE_NOT_EXIST == rc) {
      // There is no specified sequence, igonre the error.
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

int Sdb_conn::drop_seq(const char *cs_name, const char *table_name) {
  return retry(
      boost::bind(conn_drop_seq, this, &m_connection, cs_name, table_name));
}
#endif

int conn_drop_cs(sdbclient::sdb *connection, const char *cs_name) {
  int rc = connection->dropCollectionSpace(cs_name);
  if (SDB_DMS_CS_NOTEXIST == rc) {
    rc = SDB_ERR_OK;
  }
  return rc;
}

int Sdb_conn::drop_cs(const char *cs_name) {
  return retry(boost::bind(conn_drop_cs, &m_connection, cs_name));
}

int conn_drop_empty_cs(sdbclient::sdb *connection, const char *cs_name,
                       const bson::BSONObj &option) {
  int rc = connection->dropCollectionSpace(cs_name, option);
  if (SDB_DMS_CS_NOTEXIST == rc) {
    rc = SDB_ERR_OK;
  }
  return rc;
}

int Sdb_conn::drop_empty_cs(const char *cs_name, const bson::BSONObj &option) {
  return retry(boost::bind(conn_drop_empty_cs, &m_connection, cs_name, option));
}

// SequoiaDB doesn't support cl statistics before v3.4.2/5.0.2
bool Sdb_conn::is_cl_statistics_supported() {
  bool supported = false;
  int major = 0;
  int minor = 0;
  int fix = 0;
  int rc = 0;

  rc = sdb_get_version(*this, major, minor, fix);
  if (rc != 0) {
    goto error;
  }

  if (major < 3 ||                              // x < 3
      (3 == major && minor < 2) ||              // 3.x < 3.2
      (3 == major && 2 == minor && fix < 5) ||  // 3.2.x < 3.2.5
      (3 == major && 4 == minor && fix < 1)) {  // 3.4.x < 3.4.1
    supported = false;
  } else {
    supported = true;
  }

done:
  return supported;
error:
  supported = false;
  goto done;
}

int conn_exec(sdbclient::sdb *connection, const char *sql,
              sdbclient::sdbCursor *cursor) {
  return connection->exec(sql, *cursor);
}

int Sdb_conn::execute(const char *sql) {
  int rc = retry(boost::bind(conn_exec, &m_connection, sql, &m_cursor));
  return rc;
}

int Sdb_conn::get_cl_statistics(const char *cs_name, const char *cl_name,
                                Sdb_statistics &stats) {
  int rc = SDB_ERR_OK;
  if (is_cl_statistics_supported()) {
    rc = get_cl_stats_by_get_detail(cs_name, cl_name, stats);
  } else {
    rc = get_cl_stats_by_snapshot(cs_name, cl_name, stats);
  }
  return rc;
}

int Sdb_conn::get_cl_stats_by_get_detail(const char *cs_name,
                                         const char *cl_name,
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

int Sdb_conn::get_cl_stats_by_snapshot(const char *cs_name, const char *cl_name,
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
  bson::BSONObj obj;
  char normal_cl_stats_sql[sizeof(NORMAL_CL_STATS_SQL) +
                           SDB_CL_FULL_NAME_MAX_SIZE] = {0};
  char main_cl_stats_sql[sizeof(MAIN_CL_STATS_SQL) +
                         SDB_CL_FULL_NAME_MAX_SIZE] = {0};

  DBUG_ASSERT(NULL != cs_name);
  DBUG_ASSERT(strlength(cs_name) != 0);

  // Try getting statistics as normal cl. If not, try main cl again.
  snprintf(normal_cl_stats_sql, sizeof(normal_cl_stats_sql),
           NORMAL_CL_STATS_SQL, cs_name, cl_name);
  rc = execute(normal_cl_stats_sql);
  if (rc != SDB_ERR_OK) {
    goto error;
  }
  try {
    rc = next(obj, false);
    if (HA_ERR_END_OF_FILE == rc) {
      snprintf(main_cl_stats_sql, sizeof(main_cl_stats_sql), MAIN_CL_STATS_SQL,
               cs_name, cl_name);
      rc = execute(main_cl_stats_sql);
      if (rc != SDB_ERR_OK) {
        goto error;
      }
      rc = next(obj, false);
    }
    if (rc != SDB_ERR_OK) {
      goto error;
    }
  }
  SDB_EXCEPTION_CATCHER(
      rc, "Failed to move cursor to next cl name:%s.%s, exception:%s", cs_name,
      cl_name, e.what());
  stats.page_size = obj.getField(SDB_FIELD_PAGE_SIZE).numberInt();
  stats.total_data_pages = obj.getField(SDB_FIELD_TOTAL_DATA_PAGES).numberInt();
  stats.total_index_pages =
      obj.getField(SDB_FIELD_TOTAL_INDEX_PAGES).numberInt();
  stats.total_data_free_space =
      obj.getField(SDB_FIELD_TOTAL_DATA_FREE_SPACE).numberLong();
  stats.total_records = obj.getField(SDB_FIELD_TOTAL_RECORDS).numberLong();

done:
  execute_done();
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
                           errmsg));
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
  int rc = SDB_ERR_OK;
  try {
    rc = connection->getSessionAttr(*option);
  }
  SDB_EXCEPTION_CATCHER(rc, "Failed to get session attribute, exception:%s",
                        e.what());
done:
  return rc;
error:
  goto done;
}

int Sdb_conn::get_session_attr(bson::BSONObj &option) {
  return retry(boost::bind(conn_get_session_attr, &m_connection, &option));
}

int conn_set_session_attr(sdbclient::sdb *connection,
                          const bson::BSONObj *option) {
  int rc = SDB_ERR_OK;
  try {
    rc = connection->setSessionAttr(*option);
  }
  SDB_EXCEPTION_CATCHER(rc, "Failed to set session attribute, exception:%s",
                        e.what());
done:
  return rc;
error:
  goto done;
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
  int rc = SDB_ERR_OK;
  try {
    rc = connection->analyze(*options);
  }
  SDB_EXCEPTION_CATCHER(rc, "Failed to analyze, exception:%s", e.what());
done:
  return rc;
error:
  goto done;
}

int Sdb_conn::analyze(const bson::BSONObj &options) {
  return retry(boost::bind(conn_analyze, &m_connection, &options));
}
