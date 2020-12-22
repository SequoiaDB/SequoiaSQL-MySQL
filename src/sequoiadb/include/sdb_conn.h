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

#ifndef SDB_CONN__H
#define SDB_CONN__H

#include "ha_sdb_sql.h"
#include <client.hpp>
#include <boost/bind.hpp>
#include <boost/function.hpp>
#include "ha_sdb_def.h"
#include <mysql/plugin.h>

#if defined IS_MYSQL
#include <my_thread_local.h>
#elif defined IS_MARIADB
#include <my_pthread.h>
#endif

class Sdb_cl;
#ifdef IS_MARIADB
class Sdb_seq;
#endif
class Sdb_statistics;

class Sdb_session_attrs {
 public:
  Sdb_session_attrs() { reset(); }

  ~Sdb_session_attrs(){};

  void reset() {
    last_trans_isolation = SDB_TRANS_ISO_INVALID;
    last_trans_timeout = SDB_LOCK_WAIT_TIMEOUT_INVIAD;
    last_trans_auto_commit = SDB_DEFAULT_TRANS_AUTO_COMMIT;
    last_trans_use_rollback_segments = SDB_DEFAULT_TRANS_USE_RBS;
    last_check_collection_version = false;
    attr_count = 0;
    source_str[0] = '\0';
    trans_isolation = SDB_TRANS_ISO_RR;
    trans_auto_rollback = false;
    trans_auto_commit = true;
    trans_timeout = SDB_DEFAULT_LOCK_WAIT_TIMEOUT;
    trans_use_rollback_segments = true;
    check_collection_version = false;
    session_attrs_mask = 0;
  }

  void save_last_attrs();

  inline void set_attrs_mask(const ulonglong attr_mask) {
    session_attrs_mask |= attr_mask;
  }

  inline void clear_args() {
    session_attrs_mask = 0;
    attr_count = 0;
  }

  inline int get_attr_count() { return attr_count; }

  inline ulonglong get_attrs_mask() { return session_attrs_mask; }

  inline bool test_attrs_mask(const ulonglong attr_mask) {
    return (session_attrs_mask & attr_mask) ? true : false;
  }

  inline void clear_attrs_mask(const ulonglong attr_mask) {
    session_attrs_mask &= (~attr_mask);
  }

  void attrs_to_obj(bson::BSONObj *attr_obj);

  inline void set_source(const char *hostname, const int proc_id,
                         const ulonglong thread_id) {
    snprintf(source_str, sizeof(source_str), "%s%s%s:%d:%llu", PREFIX_THREAD_ID,
             strlen(hostname) ? ":" : "", hostname, proc_id, thread_id);
    set_attrs_mask(SDB_SESSION_ATTR_SOURCE_MASK);
    attr_count++;
  }

  inline const char *get_source() { return source_str; }

  inline void set_trans_isolation(const ulong tx_isolation) {
    if (last_trans_isolation != tx_isolation) {
      trans_isolation = tx_isolation;
      set_attrs_mask(SDB_SESSION_ATTR_TRANS_ISOLATION_MASK);
      attr_count++;
    }
  }

  inline void set_trans_auto_rollback(const bool auto_rollback) {
    trans_auto_rollback = auto_rollback;
    set_attrs_mask(SDB_SESSION_ATTR_TRANS_AUTO_ROLLBACK_MASK);
    attr_count++;
  }

  inline void set_trans_auto_commit(const bool auto_commit, bool init = false) {
    /*bool has no invalid value, init = true means always
      set the trans_auto_commit during the first connecting time.*/
    if (init || last_trans_auto_commit != auto_commit) {
      trans_auto_commit = auto_commit;
      set_attrs_mask(SDB_SESSION_ATTR_TRANS_AUTO_COMMIT_MASK);
      attr_count++;
    }
  }

  inline void set_trans_timeout(const int timeout) {
    if (last_trans_timeout != timeout) {
      trans_timeout = timeout;
      set_attrs_mask(SDB_SESSION_ATTR_TRANS_TIMEOUT_MASK);
      attr_count++;
    }
  }

  inline void set_trans_use_rollback_segments(const bool use_rbs,
                                              bool init = false) {
    /*bool has no invalid value, init = true means always set the
      trans_auto_commit during the first connecting time.*/
    if (init || last_trans_use_rollback_segments != use_rbs) {
      trans_use_rollback_segments = use_rbs;
      set_attrs_mask(SDB_SESSION_ATTR_TRANS_USE_RBS_MASK);
      attr_count++;
    }
  }

  inline void set_check_collection_version(bool check_version) {
    if (last_check_collection_version != check_version) {
      check_collection_version = check_version;
      set_attrs_mask(SDB_SESSION_ATTR_CHECK_CL_VERSION_MASK);
      attr_count++;
    }
  }

  inline bool get_last_trans_use_rollback_segments() {
    return last_trans_use_rollback_segments;
  }

  inline bool get_last_trans_auto_commit() { return last_trans_auto_commit; }

 private:
  ulong last_trans_isolation;
  int last_trans_timeout;
  bool last_trans_auto_commit;
  bool last_trans_use_rollback_segments;
  bool last_check_collection_version;
  int attr_count;

 private:
  /*session attributes on sequoiadb.*/
  char source_str[PREFIX_THREAD_ID_LEN + HOST_NAME_MAX + 64]; /*Source*/
  // 64 bytes is for string of proc_id and thread_id.
  ulong trans_isolation;    /*TransIsolation*/
  bool trans_auto_rollback; /*TransAutoRollback*/
  /*when sequoiadb_use_transaction changed, trans_auto_commit should changed
   * too.*/
  bool trans_auto_commit;           /*TransAutoCommit*/
  int trans_timeout;                /*TransTimeout*/
  bool trans_use_rollback_segments; /*TransUseRBS*/
  bool check_collection_version;    /*CheckClientCataVersion*/
  ulonglong session_attrs_mask;
};

class Sdb_conn {
 public:
  Sdb_conn(my_thread_id _tid, bool server_ha_conn = false);

  ~Sdb_conn();

  int connect();

  sdbclient::sdb &get_sdb();

  my_thread_id thread_id();

  int begin_transaction(uint tx_isolation = ISO_REPEATABLE_READ);

  int commit_transaction(const bson::BSONObj &hint = SDB_EMPTY_BSON);

  int rollback_transaction();

  inline bool is_transaction_on() { return m_transaction_on; }

  int get_cl(const char *cs_name, const char *cl_name, Sdb_cl &cl,
             const bool check_exist = false);

  int create_cl(const char *cs_name, const char *cl_name,
                const bson::BSONObj &options = SDB_EMPTY_BSON,
                bool *created_cs = NULL, bool *created_cl = NULL);

  int rename_cl(const char *cs_name, const char *old_cl_name,
                const char *new_cl_name);

  int drop_cl(const char *cs_name, const char *cl_name);

  int drop_cs(const char *cs_name);

  int drop_empty_cs(const char *cs_name,
                    const bson::BSONObj &option = SDB_EMPTY_BSON);

#ifdef IS_MARIADB
  int get_seq(const char *cs_name, const char *table_name, char *sequence_name,
              Sdb_seq &seq);

  int create_seq(const char *cs_name, const char *table_name,
                 char *sequence_name,
                 const bson::BSONObj &options = SDB_EMPTY_BSON,
                 bool *created_cs = NULL, bool *created_seq = NULL);

  int rename_seq(const char *cs_name, const char *old_table_name,
                 const char *new_table_name);

  int drop_seq(const char *cs_name, const char *table_name);
#endif

  int get_cl_statistics(const char *cs_name, const char *cl_name,
                        Sdb_statistics &stats);

  int snapshot(bson::BSONObj &obj, int snap_type,
               const bson::BSONObj &condition = SDB_EMPTY_BSON,
               const bson::BSONObj &selected = SDB_EMPTY_BSON,
               const bson::BSONObj &orderBy = SDB_EMPTY_BSON,
               const bson::BSONObj &hint = SDB_EMPTY_BSON,
               longlong numToSkip = 0);

  int get_last_result_obj(bson::BSONObj &result, bool get_owned = false);

  int get_session_attr(bson::BSONObj &option);

  int set_session_attr(const bson::BSONObj &option);

  int interrupt_operation();

  bool is_valid() { return m_connection.isValid(); }

  bool is_authenticated() { return m_is_authenticated; }

  int analyze(const bson::BSONObj &options);

  inline void set_pushed_autocommit() { pushed_autocommit = true; }

  inline bool get_pushed_autocommit() { return pushed_autocommit; }

  int get_last_error(bson::BSONObj &errObj) {
    return m_connection.getLastErrorObj(errObj);
  }

  inline ulong convert_to_sdb_isolation(const ulong tx_isolation,
                                        const int major = 3) {
    switch (tx_isolation) {
      case ISO_READ_UNCOMMITTED:
        return SDB_TRANS_ISO_RU;
        break;
      case ISO_READ_COMMITTED:
        return SDB_TRANS_ISO_RC;
        break;
      case ISO_READ_STABILITY:
        return SDB_TRANS_ISO_RS;
        break;
      case ISO_REPEATABLE_READ:
        // x < 5 not support RR. sdb use RC to mysql RR.
        if (major < 5) {
          return SDB_TRANS_ISO_RC;
        } else {
          return SDB_TRANS_ISO_RR;
        }
        break;
      case ISO_SERIALIZABLE:  // not supported current now.
      default:
        // never come to here.
        DBUG_ASSERT(0);
        return SDB_TRANS_ISO_RR;
    }
  }

  inline char *get_err_msg() { return errmsg; }
  inline void clear_err_msg() { errmsg[0] = '\0'; }

  inline void set_rollback_on_timeout(const bool rollback) {
    rollback_on_timeout = rollback;
  }

  inline bool get_rollback_on_timeout() const { return rollback_on_timeout; }

  inline Sdb_session_attrs *get_session_attrs() { return &session_attrs; }

  int set_my_session_attr();

  int exec(const char *sql, sdbclient::sdbCursor *cursor);

  void set_check_collection_version(bool check_cl_version) {
    m_check_collection_version = check_cl_version;
  }

  void get_version(int &major, int &minor, int &fix) {
    uint8 major_ver = 0, minor_ver = 0, fix_ver = 0;
    m_connection.getVersion(major_ver, minor_ver, fix_ver);
    major = major_ver;
    minor = minor_ver;
    fix = fix_ver;
  }

  bool is_cl_statistics_supported();

 private:
  int retry(boost::function<int()> func);

  int get_cl_stats_by_get_detail(const char *cs_name, const char *cl_name,
                                 Sdb_statistics &stats);

  int get_cl_stats_by_snapshot(const char *cs_name, const char *cl_name,
                               Sdb_statistics &stats);

 private:
  sdbclient::sdb m_connection;
  bool m_transaction_on;
  my_thread_id m_thread_id;
  bool pushed_autocommit;
  bool m_is_authenticated;
  bool m_is_server_ha_conn; /* Flag of server ha conn or normal conn. Server HA
                               conn always use transaction. */
  bool m_check_collection_version;
  char errmsg[SDB_ERR_BUFF_SIZE];
  bool rollback_on_timeout;

  Sdb_session_attrs session_attrs;
};

#endif
