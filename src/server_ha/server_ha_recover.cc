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

#include <my_global.h>
#include "server_ha_recover.h"
#include "server_ha.h"
#include "server_ha_util.h"
#include "sdb_conn.h"
#include "sdb_cl.h"
#include "mysql.h"
#include "ha_sdb_errcode.h"
#include "ha_sdb_util.h"
#include "ha_sdb_log.h"
#include <exception>
#include "errmsg.h"

// SQL statements
#define HA_STMT_EXEC_ONLY_IN_MYSQL "SET sequoiadb_execute_only_in_mysql = 1"
#define HA_STMT_SHOW_DATABASES "SHOW DATABASES"
#define HA_STMT_DROP_DATABASE "DROP DATABASE "
#define HA_STMT_FLUSH_PRIVILEGES "FLUSH PRIVILEGES"
#define HA_STMT_SET_CLIENT_CHARSET "SET character_set_client = utf8mb4"
#define HA_STMT_USELESS_SQL "SET @useless_sql='drop instance group user'"
#define HA_STMT_DELETE_USER "DELETE FROM mysql.user WHERE User != 'root'"
#define HA_STMT_DELETE_ROUTINES "DELETE FROM mysql.proc WHERE db != 'sys'"
#define HA_STMT_SET_NAMES "SET NAMES 'utf8mb4'"

// instance group user name
static char ha_inst_group_user[HA_MAX_MYSQL_USERNAME_LEN + 1] = {0};
// local host IP, it's same with 'bind_address' if 'bind_address' is set,
static char ha_local_host_ip[HA_MAX_IP_LEN + 1] = {0};
// instance group user's password
static char ha_inst_group_passwd[HA_MAX_PASSWD_LEN + 1] = {0};

// mysql connection used to replay SQL log
static MYSQL *ha_mysql = NULL;

// update instance state cached record, called in 'replay_sql_stmt_loop'
// and 'write_sql_log_and_states'
int ha_update_cached_record(HASH &cache, PSI_memory_key mem_key,
                            const char *cached_record_key, int sql_id) {
  int rc = 0;
  ha_cached_record *cached_record = get_cached_record(cache, cached_record_key);
  if (cached_record) {
    cached_record->sql_id = sql_id;
  } else {
    ha_cached_record *record = NULL;
    char *key = NULL;
    int key_len = strlen(cached_record_key);
    if (!sdb_multi_malloc(mem_key, MYF(MY_WME | MY_ZEROFILL), &record,
                          sizeof(ha_cached_record), &key, key_len + 1, NullS)) {
      rc = SDB_HA_OOM;
      goto error;
    }
    snprintf(key, key_len + 1, "%s", cached_record_key);
    key[key_len] = '\0';
    record->key = key;
    record->sql_id = sql_id;
    if (my_hash_insert(&cache, (uchar *)record)) {
      rc = SDB_HA_OOM;
    }
  }
done:
  return rc;
error:
  goto done;
}

// create THD for creating instance group user
static THD *create_ha_thd() {
  THD *thd = NULL;
  my_thread_init();
  my_thread_id thread_id;
#ifdef IS_MYSQL
  thd = create_thd(false, true, false, HA_KEY_HA_THD);
  DBUG_ASSERT(thd != NULL);
#else
  thd = create_thd();
  DBUG_ASSERT(thd != NULL);
  DBUG_ASSERT(thd->mysys_var != NULL);
  // remove current thread from server_threads, so it can be
  // notified to be finished in 'server_ha_deinit' function, or it will
  // be killed in 'close_connections' function, note the change
  // of thread_count in recover_and_replay_thread_end
  server_threads.erase(thd);
  thread_count--;
#endif
  thd->get_stmt_da()->reset_diagnostics_area();
  thd->get_stmt_da()->set_overwrite_status(true);
  return thd;
}

// build context for creating instance group user
static void set_lex_user(LEX_USER &lex_user, const char *name, const char *host,
                         const char *plugin, const char *auth_str) {
#ifdef IS_MYSQL
  lex_user.user.str = name;
  lex_user.user.length = strlen(name);
  lex_user.host.str = host;
  lex_user.host.length = strlen(host);
  lex_user.plugin.str = plugin;
  lex_user.plugin.length = strlen(plugin);
  lex_user.auth.str = auth_str;
  lex_user.auth.length = strlen(auth_str);
  lex_user.uses_identified_by_clause = false;
  lex_user.uses_identified_with_clause = true;
  lex_user.uses_authentication_string_clause = true;
  lex_user.uses_identified_by_password_clause = false;

  lex_user.alter_status.update_password_expired_fields = false;
  lex_user.alter_status.update_password_expired_column = false;
  lex_user.alter_status.use_default_password_lifetime = false;
  lex_user.alter_status.expire_after_days = EXPIRED_AFTER_DAYS;
  lex_user.alter_status.update_account_locked_column = false;
  lex_user.alter_status.account_locked = false;
#else
  lex_user.user = {name, strlen(name)};
  lex_user.host = {host, strlen(host)};
  lex_user.auth->plugin = {plugin, strlen(plugin)};
  lex_user.auth->auth_str = {auth_str, strlen(auth_str)};
  lex_user.auth->next = NULL;
  lex_user.auth->pwtext = {NULL, 0};
#endif
}

// set necessary flag for creating instance group user
static void set_lex_extra(THD *thd) {
#ifdef IS_MYSQL
  thd->lex->ssl_cipher = 0;
  thd->lex->x509_issuer = 0;
  thd->lex->x509_subject = 0;
  thd->lex->ssl_type = SSL_TYPE_NOT_SPECIFIED;
#else
  thd->lex->account_options.reset();
  thd->security_ctx->master_access |= CREATE_USER_ACL;
  // set query_time used to change 'password_last_changed'
  thd->set_start_time();
  thd->security_ctx->skip_grants();
  thd->lex->grant |= GRANT_ACL;
#endif
}

// read instance ID from local file
static int get_local_instance_id(int &instance_id) {
  int tmp = 0, rc = 0;
  char my_id[FN_REFLEN] = {0};
  char buff[HA_BUF_LEN] = {0};
  File file = -1;
  PSI_file_key myid_key = HA_KEY_MYID_FILE;

  snprintf(my_id, FN_REFLEN, "%s/%s", mysql_real_data_home_ptr, HA_MYID_FILE);
  file = mysql_file_open(myid_key, my_id, O_RDONLY, MYF(0));
  if (ENOENT == errno) {  // file not exist error
    sql_print_information("HA: Instance ID file '%s' does not exist", my_id);
    instance_id = 0;
    goto done;
  }
  HA_RC_CHECK(file < 0, error, "HA: Open instance ID file '%s' error: %s",
              my_id, strerror(errno));

  mysql_file_read(file, (uchar *)&buff[0], HA_BUF_LEN, MYF(0));
  tmp = atoi(buff);
  instance_id = tmp > 0 ? tmp : 0;
  if (0 == instance_id) {
    sql_print_warning("HA: Wrong data '%s' in instance ID file", buff);
  }
  mysql_file_close(file, MYF(0));
done:
  return rc;
error:
  rc = SDB_HA_GET_INST_ID;
  goto done;
}

// write instance ID into local file
static int write_local_instance_id(int instance_id) {
  int rc = 0;
  int open_flags = O_CREAT | O_RDWR;
  char my_id[FN_REFLEN] = {0};
  char buff[HA_MAX_INSTANCE_ID_LEN] = {0};
  File file = -1;
  PSI_file_key myid_key = HA_KEY_MYID_FILE;

  snprintf(my_id, FN_REFLEN, "%s/%s", mysql_real_data_home_ptr, HA_MYID_FILE);
  file = mysql_file_open(myid_key, my_id, open_flags, MYF(0));
  HA_RC_CHECK(file < 0, error,
              "HA: Couldn't open instance ID file '%s', system error: %s",
              my_id, strerror(errno));

  int10_to_str(instance_id, buff, 10);
  rc = mysql_file_write(file, (uchar *)buff, strlen(buff),
                        MYF(MY_WME + MY_NABP));
  mysql_file_close(file, MYF(0));
  HA_RC_CHECK(rc, error,
              "HA: Couldn't write instance ID file '%s', system error: %s",
              my_id, strerror(errno));
done:
  return rc;
error:
  rc = SDB_HA_WRITE_INST_ID;
  goto done;
}

// call mysql or mariadb interface, create user
static int mysql_create_user(THD *thd, LEX_USER &lex_user) {
#ifndef NO_EMBEDDED_ACCESS_CHECKS
#ifdef IS_MYSQL
  // can't use mysql_grant in mysql because of 'mysql_rewrite_grant'
  // open the mysql.user and mysql.db or mysql.proxies_priv tables
  TABLE_LIST tables[2];
  uint rights = ALL_PRIVILEGES;
  rights |= GRANT_ACL;
  thd->set_skip_readonly_check();
  thd->tx_read_only = false;
  tables[0].init_one_table(C_STRING_WITH_LEN(HA_MYSQL_DB),
                           C_STRING_WITH_LEN(HA_USER_TABLE), HA_USER_TABLE,
                           TL_WRITE);
  tables[1].init_one_table(C_STRING_WITH_LEN(HA_MYSQL_DB),
                           C_STRING_WITH_LEN(HA_DB_TABLE), HA_DB_TABLE,
                           TL_WRITE);
  tables[0].next_local = tables[0].next_global = tables + 1;

  if (open_and_lock_tables(
          thd, tables,
          MYSQL_LOCK_IGNORE_TIMEOUT)) {  // This should never happen
    return -1;
  }

  Partitioned_rwlock_write_guard lock(&LOCK_grant);
  mysql_mutex_lock(&acl_cache->lock);
  grant_version++;
  int ret = replace_user_table(thd, tables[0].table, &lex_user, rights, false,
                               true, (2 | ACCESS_RIGHTS_ATTR));
  mysql_mutex_unlock(&acl_cache->lock);
  lock.unlock();

  bool rollback_transaction = thd->transaction_rollback_request || ret;
  if (rollback_transaction) {
    trans_rollback_stmt(thd);
    trans_rollback_implicit(thd);
    return -1;
  } else {
    trans_commit_stmt(thd);
    trans_commit_implicit(thd);
  }
  close_thread_tables(thd);
  thd->mdl_context.release_transactional_locks();

  (void)acl_reload(thd);
  (void)grant_reload(thd);

  ha_close_connection(thd);
  return 0;
#else
  List<LEX_USER> list;
  uint rights = ALL_PRIVILEGES;
  rights |= GRANT_ACL;
  list.push_front(&lex_user);
  bool rc = 0;

  // if binlog is open, query must be set in mariadb
  char *query = HA_STMT_USELESS_SQL;
  thd->set_query(query, strlen(query));
  thd->tx_read_only = false;
  mysql_drop_user(thd, list, false);
  thd->clear_error();
  close_thread_tables(thd);

  query = HA_STMT_USELESS_SQL;
  thd->set_query(query, strlen(query));
  rc = mysql_grant(thd, NULL, list, rights, false, false);
  close_thread_tables(thd);
  thd->mdl_context.release_transactional_locks();
  return rc;
#endif
#endif
}

// decrypt encrypted password from 'HAUser' table
static int decrypt_inst_group_password(const char *base64_cipher,
                                       const char *sdb_md5_password,
                                       const char *key, const char *iv,
                                       char *passwd) {
  String src, dst, md5_key, md5_iv, md5_password, md5_hex_str;
  int rc = 0;
  bool oom = false;

  // decode base64 cipher to src
  int len = base64_needed_decoded_length(strlen(base64_cipher));
  oom = src.alloc(len);
  HA_RC_CHECK(oom, error,
              "HA: Out of memory while decrypting instance group password");

  len = base64_decode(base64_cipher, strlen(base64_cipher), src.c_ptr(), 0, 0);
  HA_RC_CHECK(len < 0, error,
              "HA: Failed to decode 'CipherPassword' in base64 format, "
              "please use 'IV' and instance group key to check "
              "if 'CipherPassword' in base64 format is correct");
  src.length(len);

  // calculcate MD5 for instance group key and 'IV', prepare for decryption
  oom = md5_key.alloc(HA_MD5_BYTE_LEN);
  oom |= md5_iv.alloc(HA_MD5_BYTE_LEN);
  HA_RC_CHECK(oom, error,
              "HA: Out of memory while decrypting instance group password");

  md5_key.length(HA_MD5_BYTE_LEN);
  md5_iv.length(HA_MD5_BYTE_LEN);
#ifdef IS_MYSQL
  compute_md5_hash(md5_key.c_ptr(), key, strlen(key));
  compute_md5_hash(md5_iv.c_ptr(), iv, strlen(iv));
#else
  compute_md5_hash((uchar *)md5_key.c_ptr(), key, strlen(key));
  compute_md5_hash((uchar *)md5_iv.c_ptr(), iv, strlen(iv));
#endif

  rc = sdb_aes_decrypt(MY_AES_CBC, (uchar *)md5_key.c_ptr(), HA_MD5_BYTE_LEN,
                       src, dst, (uchar *)md5_iv.c_ptr(), HA_MD5_BYTE_LEN);
  HA_RC_CHECK(rc, error, "HA: Decrypt aes cipher error: %d", rc);

  // check if password is correct
  oom = md5_password.alloc(HA_MD5_BYTE_LEN);
  oom |= md5_hex_str.alloc(HA_MD5_HEX_STR_LEN);
  HA_RC_CHECK(oom, error,
              "HA: Out of memory while decrypting instance group password");

  strcpy(passwd, dst.c_ptr_safe());
  md5_password.length(HA_MD5_BYTE_LEN);
  md5_hex_str.length(HA_MD5_HEX_STR_LEN);
#ifdef IS_MYSQL
  compute_md5_hash(md5_password.c_ptr(), dst.c_ptr(), dst.length());
#else
  compute_md5_hash((uchar *)md5_password.c_ptr(), dst.c_ptr(), dst.length());
#endif
  array_to_hex(md5_hex_str.c_ptr_safe(), (uchar *)md5_password.c_ptr_safe(),
               HA_MD5_BYTE_LEN);
  rc = strcasecmp(sdb_md5_password, md5_hex_str.c_ptr_safe());
  HA_RC_CHECK(rc, error,
              "HA: Instance group password verification failed, "
              "please check if instance group key is correct.");
done:
  return rc;
error:
  rc = oom ? SDB_HA_OOM : SDB_HA_DECRYPT_PASSWORD;
  goto done;
}

// check if user is available in current instance
static bool is_mysql_available(const char *ip, uint port, const char *user,
                               const char *passwd) {
  bool available = true;
  MYSQL *conn = mysql_init(NULL);
  if (NULL == conn) {
    sql_print_error("HA: Out of memory while initializing mysql connection");
    available = false;
    goto error;
  }

  if (!mysql_real_connect(conn, ip, user, passwd, HA_MYSQL_DB, port, 0, 0)) {
    // do not print error log, or mysql automated testing will fail in
    // some situation
    sql_print_information(
        "HA: MySQL server '%s:%d' is not available for user '%s', "
        "mysql error: %s",
        ip, port, user, mysql_error(conn));
    available = false;
  }
done:
  mysql_close(conn);
  return available;
error:
  goto done;
}

static int mysql_reconnect(MYSQL *conn) {
  int rc = 0;
  MYSQL *mysql = NULL;
  DBUG_ASSERT(NULL != conn);

  mysql_close(conn);
  mysql = mysql_init(conn);
  if (NULL == mysql) {
    sql_print_error("HA: Out of memory while initializing mysql connection");
    rc = SDB_HA_OOM;
  } else if (!mysql_real_connect(conn, ha_local_host_ip, ha_inst_group_user,
                                 ha_inst_group_passwd, conn->db, mysqld_port,
                                 NULL, 0)) {
    sql_print_information(
        "HA: Failed to connect current instance, mysql error: %s",
        mysql_error(conn));
    rc = mysql_errno(conn);
  }
  return rc;
}

static inline int mysql_query(MYSQL *conn, const char *query, ulong len,
                              bool exec_only_in_mysql = true) {
  int rc = mysql_real_query(conn, query, len);
  uint mysql_err_num = 0;
  if (0 == rc) {
    goto done;
  }

  mysql_err_num = mysql_errno(conn);
  DBUG_ASSERT(0 != mysql_err_num);
  if (CR_SERVER_GONE_ERROR != mysql_err_num &&
      CR_SERVER_LOST != mysql_err_num) {
    rc = mysql_err_num;
    goto error;
  }

  // if mysql errno is 'CR_SERVER_GONE_ERROR' or 'CR_SERVER_LOST'
  // reconnect to current instance
  sql_print_information(
      "HA: Lost connection to MySQL server during query, "
      "reconnect to current instance");
  rc = mysql_reconnect(conn);
  if (0 == rc) {
    if (exec_only_in_mysql) {
      rc =
          mysql_real_query(conn, C_STRING_WITH_LEN(HA_STMT_EXEC_ONLY_IN_MYSQL));
      rc = rc ? rc : mysql_real_query(conn, query, len);
      rc = rc ? mysql_errno(conn) : 0;
    }
  }
done:
  return rc;
error:
  goto done;
}

// check if current instance 'explicit_defaults_for_timestamp' is consistent
// to 'ExplicitDefaultsTimestamp' in 'HAInstGroupConfig'
static inline int check_explicit_defaults_timestamp(bool explicit_defaults_ts) {
#ifdef IS_MARIADB
  // check if 'explicit_defaults_for_timestamp' is consistent to configuration
  if (explicit_defaults_ts != opt_explicit_defaults_for_timestamp) {
    sql_print_error(
        "HA: The 'explicit_defaults_for_timestamp' variable in current "
        "instance is inconsistent with the configuration in the instance "
        "group, please change one of them");
    return SDB_HA_TIMESTAMP_INCONSIST;
  }
  return 0;
#else
  return 0;
#endif
}

static int init_ha_mysql_connection(const char *ip, uint port, const char *user,
                                    const char *passwd) {
  int rc = 0;
  static MYSQL ha_mysql_conn;
  ha_mysql = mysql_init(&ha_mysql_conn);
  if (NULL == ha_mysql) {
    sql_print_error("HA: Out of memory while initializing mysql connection");
    rc = SDB_HA_OOM;
    goto error;
  } else if (!mysql_real_connect(ha_mysql, ip, user, passwd, HA_MYSQL_DB, port,
                                 0, 0)) {
    sql_print_information(
        "HA: MySQL server '%s:%d' is not available for user '%s', "
        "mysql error: %s",
        ip, port, user, mysql_error(ha_mysql));
    rc = mysql_errno(ha_mysql);
  }
done:
  return rc;
error:
  goto done;
}

// get user info from 'HAUser' table and create user
static int ensure_inst_group_user(ha_recover_replay_thread *ha_thread,
                                  Sdb_conn &sdb_conn) {
  int rc = 0;
  bool explicit_defaults_ts = 0;
  const char *user = NULL, *host = NULL, *plugin = NULL;
  const char *iv = NULL, *auth_str = NULL;
  const char *cipher_password = NULL, *md5_password = NULL;
  LEX_USER lex_user;
  char err_buf[HA_BUF_LEN] = {0};

#ifdef IS_MARIADB
  USER_AUTH auth;
  lex_user.auth = &auth;
#endif

  bson::BSONObj obj;
  Sdb_cl inst_group_config_cl;

  rc = sdb_conn.get_cl(ha_thread->sdb_group_name, HA_INST_GROUP_CONFIG_CL,
                       inst_group_config_cl);
  HA_RC_CHECK(rc, error,
              "HA: Unable to get instance group configuration table: %s, "
              "sequoiadb error: %s",
              HA_INST_GROUP_CONFIG_CL, ha_error_string(sdb_conn, rc, err_buf));

  if (0 == (rc = inst_group_config_cl.query())) {
    rc = inst_group_config_cl.next(obj, false);
  }
  HA_RC_CHECK(rc, error,
              "HA: Failed to get instance group configuration, "
              "sequoiadb error: %s",
              ha_error_string(sdb_conn, rc, err_buf));

  explicit_defaults_ts = obj.getIntField(HA_FIELD_EXPLICITS_DEFAULTS_TIMESTAMP);
  rc = check_explicit_defaults_timestamp(explicit_defaults_ts);
  if (rc) {
    goto error;
  }

  user = obj.getStringField(HA_FIELD_USER);
  host = obj.getStringField(HA_FIELD_HOST);
  plugin = obj.getStringField(HA_FIELD_PLUGIN);
  iv = obj.getStringField(HA_FIELD_IV);
  cipher_password = obj.getStringField(HA_FIELD_CIPHER_PASSWORD);
  auth_str = obj.getStringField(HA_FIELD_AUTH_STRING);
  md5_password = obj.getStringField(HA_FIELD_MD5_PASSWORD);

  // each field values in 'HAInstGroupConfig' can't be empty
  DBUG_ASSERT(strlen(user) && strlen(host) && strlen(plugin));
  DBUG_ASSERT(strlen(iv) && strlen(cipher_password));
  DBUG_ASSERT(strlen(auth_str) && strlen(md5_password));

  rc = decrypt_inst_group_password(cipher_password, md5_password,
                                   ha_thread->group_key, iv,
                                   ha_inst_group_passwd);
  HA_RC_CHECK(rc, error, "HA: Failed to decrypt instance group user password");

  // check if instance group user is available
  strcpy(ha_inst_group_user, user);
  if (is_mysql_available(ha_local_host_ip, mysqld_port, user,
                         ha_inst_group_passwd)) {
    sql_print_information("HA: Instance group user '%s' already exists", user);
    // init mysql connection for 'HA' thread
    rc = init_ha_mysql_connection(ha_local_host_ip, mysqld_port, user,
                                  ha_inst_group_passwd);
    HA_RC_CHECK(rc, error, "HA: Failed to connect to current instance");
    goto done;
  }

  // create instance group user
  sql_print_information("HA: Create instance group user '%s'", user);
  set_lex_user(lex_user, user, host, plugin, auth_str);
  set_lex_extra(ha_thread->thd);
  rc = mysql_create_user(ha_thread->thd, lex_user);
  rc = rc ? SDB_HA_CREATE_INST_GROUP_USER : 0;
  HA_RC_CHECK(rc, error,
              "HA: Failed to create instance group user, "
              "please check mysql error log");

  // init mysql connection for 'HA' thread
  rc = init_ha_mysql_connection(ha_local_host_ip, mysqld_port, user,
                                ha_inst_group_passwd);
  HA_RC_CHECK(rc, error, "HA: Failed to connect to current instance");

  DBUG_ASSERT(NULL != ha_mysql);
  // execute flush privileges, update cache for 'mysql.user'
  rc = mysql_query(ha_mysql, C_STRING_WITH_LEN(HA_STMT_FLUSH_PRIVILEGES));
  HA_RC_CHECK(rc, error, "HA: Failed to execute 'flush privileges'");
done:
  return rc;
error:
  goto done;
}

// if 'bind_address' current instance is set and it's not '*'', set
// ip_addr to my_bind_addr_str, or get one of ip address(not include
// loopback address) from system
static int get_local_ip_address(char *ip_addr, int max_ip_len) {
  int rc = 0;
  if (my_bind_addr_str && strcmp(my_bind_addr_str, "*") != 0) {
    strncpy(ip_addr, my_bind_addr_str, max_ip_len);
  } else {
    struct ifaddrs *if_addr_struct = NULL, *ifa = NULL;
    void *addr_ptr = NULL;
    char addr_buf[INET_ADDRSTRLEN];

    rc = getifaddrs(&if_addr_struct);
    HA_RC_CHECK(rc, error, "HA: System call 'getifaddrs()' error: %s",
                strerror(errno));

    for (ifa = if_addr_struct; ifa != NULL; ifa = ifa->ifa_next) {
      if (ifa->ifa_addr && ifa->ifa_addr->sa_family == AF_INET) {
        addr_ptr = &((struct sockaddr_in *)ifa->ifa_addr)->sin_addr;
        const char *ip =
            inet_ntop(AF_INET, addr_ptr, addr_buf, INET_ADDRSTRLEN);
        if (ip && strcmp(ip, HA_LOOPBACK_ADDRESS) != 0) {
          strncpy(ip_addr, ip, max_ip_len);
          break;
        } else if (NULL == ip) {
          sql_print_error("HA: System call 'inet_ntop' error: %s",
                          strerror(errno));
        }
      }
    }
    rc = strlen(ip_addr) ? 0 : SDB_HA_GET_LOCAL_IP;
    freeifaddrs(if_addr_struct);
  }
done:
  return rc;
error:
  rc = SDB_HA_GET_LOCAL_IP;
  goto done;
}

static int set_dump_source(ha_recover_replay_thread *ha_thread,
                           Sdb_conn &sdb_conn, ha_dump_source &dump_source) {
  int rc = 0, sql_id = HA_INVALID_SQL_ID, instance_id = HA_INVALID_INST_ID;
  char err_buf[HA_BUF_LEN] = {0};
  int local_instance_id = ha_thread->instance_id;
  char *sdb_group_name = ha_thread->sdb_group_name;
  DBUG_ASSERT(local_instance_id > 0);

  Sdb_cl gstate_cl, config_cl, sql_log_cl;
  bson::BSONObj result, cond;

  rc = sdb_conn.get_cl(sdb_group_name, HA_GLOBAL_STATE_CL, gstate_cl);
  HA_RC_CHECK(rc, error,
              "HA: Unable to get global state table '%s', sequoiadb error: %s",
              HA_GLOBAL_STATE_CL, ha_error_string(sdb_conn, rc, err_buf));

  rc = sdb_conn.get_cl(HA_GLOBAL_INFO, HA_CONFIG_CL, config_cl);
  HA_RC_CHECK(rc, error,
              "HA: Unable to get global configuration "
              "table '%s', sequoiadb error: %s",
              HA_CONFIG_CL, ha_error_string(sdb_conn, rc, err_buf));

  rc = sdb_conn.get_cl(sdb_group_name, HA_SQL_LOG_CL, sql_log_cl);
  HA_RC_CHECK(rc, error,
              "HA: Unable to get SQL log table '%s', sequoiadb error: %s",
              HA_SQL_LOG_CL, ha_error_string(sdb_conn, rc, err_buf));

  cond = BSON(HA_FIELD_INSTANCE_ID << BSON("$ne" << local_instance_id));
  // fetch other instances's configuration from 'HAGlobalState'
  rc = gstate_cl.query(cond);
  do {
    rc = rc ? rc : gstate_cl.next(result, false);
    if (rc) {
      // do not print errors into error log, or automated testing may fail
      sql_print_information(
          "HA: Failed to get candidate dump source from "
          "'%s', sequoiadb error: %s",
          HA_GLOBAL_STATE_CL, ha_error_string(sdb_conn, rc, err_buf));
      goto error;
    }

    sql_id = result.getIntField(HA_FIELD_SQL_ID);
    instance_id = result.getIntField(HA_FIELD_INSTANCE_ID);

    // query 'HASQLLog' by sql_id, check if global executed SQL with sql_id
    // has been deleted from 'HASQLLog'
    cond = BSON(HA_FIELD_SQL_ID << sql_id);
    rc = sql_log_cl.query(cond);
    rc = rc ? rc : sql_log_cl.next(result, false);
    if (HA_ERR_END_OF_FILE == rc) {
      // global executed SQL with sql_id is deleted from 'HASQLLog'
      rc = 0;
      instance_id = HA_INVALID_INST_ID;
      continue;
    }

    // query 'HAConfig' by instance_id, get candidated instance information
    cond = BSON(HA_FIELD_INSTANCE_ID << instance_id);
    rc = config_cl.query(cond);
    rc = rc ? rc : config_cl.next(result, false);
    if (rc) {
      rc = 0;
      instance_id = HA_INVALID_INST_ID;
      continue;
    }

    const char *ip = result.getStringField(HA_FIELD_IP);
    uint port = result.getIntField(HA_FIELD_PORT);
    const char *db_type = result.getStringField(HA_FIELD_DB_TYPE);
    rc = strncmp(db_type, DB_TYPE, strlen(db_type));
    HA_RC_CHECK(rc, error, "HA: Can't sync metadata from '%s' to '%s'", db_type,
                DB_TYPE);

    // check if candidated instance is avaliable
    if (is_mysql_available(ip, port, ha_inst_group_user,
                           ha_inst_group_passwd)) {
      snprintf(dump_source.dump_host, HA_MAX_IP_LEN, "%s", ip);
      snprintf(dump_source.dump_files[0], FN_REFLEN, "%s/non_sysdb.sql",
               mysql_real_data_home_ptr);
      snprintf(dump_source.dump_files[1], FN_REFLEN, "%s/sysdb.sql",
               mysql_real_data_home_ptr);
      dump_source.dump_port = port;
      dump_source.dump_source_id = instance_id;
      sql_print_information("HA: Set dump source to '%s:%d'", ip, port);

      // found available dump source, persist instance ID to local file
      // note: 'DB_TYPE' of current instance must be the same as other
      // instances in the instance group.
      rc = write_local_instance_id(local_instance_id);
      HA_RC_CHECK(rc, error, "HA: Failed to persist instance ID: %d",
                  local_instance_id);
      // found an available dump source, exit the loop
      break;
    } else {
      instance_id = HA_INVALID_INST_ID;
      sql_print_information("HA: MySQL server '%s:%d' is not available", ip,
                            port);
    }
  } while (!rc);

  if (HA_INVALID_INST_ID == instance_id) {
    rc = SDB_HA_NO_AVAILABLE_DUMP_SRC;
    sql_print_information("HA: There is no available dump source");
    goto error;
  }
done:
  return rc;
error:
  goto done;
}

static int set_mysql_read_only(MYSQL *conn, bool read_only) {
  int rc = 0;
  static const int MAX_QUERY_LEN = 30;
  char query_buf[MAX_QUERY_LEN] = {0};
  snprintf(query_buf, MAX_QUERY_LEN, "set global read_only = %d", read_only);
  rc = mysql_real_query(conn, query_buf, strlen(query_buf));
  rc = rc ? mysql_errno(conn) : 0;
  return rc;
}

static void build_mysqldump_command(char *cmd, const ha_dump_source &src,
                                    bool dump_sysdb) {
  int end = 0;
  int max_cmd_len = FN_REFLEN * 2 + 100;
  end = snprintf(cmd, max_cmd_len, "%s/bin/mysqldump -B ", mysql_home_ptr);
  if (!dump_sysdb) {
    end += snprintf(cmd + end, max_cmd_len, "--no-data --all-databases ");
  } else {
    end += snprintf(cmd + end, max_cmd_len, "-a mysql -t ");
  }

  // ignore table 'mysql.innodb_index_stats' and 'mysql.innodb_table_stats'
  end += snprintf(cmd + end, max_cmd_len,
                  "--ignore-table mysql.innodb_index_stats ");
  end += snprintf(cmd + end, max_cmd_len,
                  "--ignore-table mysql.innodb_table_stats ");

  end += snprintf(cmd + end, max_cmd_len, "--events=1 ");
  end += snprintf(
      cmd + end, max_cmd_len, "-u%s -h%s -p%s -P%d --exec-only-in-mysql -f > ",
      ha_inst_group_user, src.dump_host, ha_inst_group_passwd, src.dump_port);

  int index = dump_sysdb ? 1 : 0;
  end += snprintf(cmd + end, max_cmd_len, "%s ", src.dump_files[index]);
  // redirect errors to mysqldump.log
  end += snprintf(cmd + end, max_cmd_len, "2>%s/mysqldump.log",
                  mysql_real_data_home_ptr);
}

// 1. dump metadata for non-system databases
// 2. dump full data for system databases
// 3. if dump data failed, don't report errors, recover metadata
//    by replaying SQL log if SQL log is complete
static int dump_meta_data(ha_recover_replay_thread *ha_thread,
                          ha_dump_source &dump_source) {
  int rc = 0, status = -1;
  char dump_cmd[FN_REFLEN * 2 + 100];
  char buff[HA_BUF_LEN] = {0};
  FILE *file = NULL;

  sql_print_information("HA: Start dump metadata");
  for (uint i = 0; i < HA_DUMP_FILE_NUM; i++) {
    bool is_dumpping_sysdb = (i == 1);
    build_mysqldump_command(dump_cmd, dump_source, is_dumpping_sysdb);

    file = popen(dump_cmd, "r");
    if (NULL == file && 0 == errno) {
      // if its oom error, need to report error
      rc = SDB_HA_OOM;
      sql_print_error("HA: Out of memory while dumping databases to '%s'",
                      dump_source.dump_files[i]);
      goto error;
    } else if (NULL == file) {
      rc = SDB_HA_DUMP_METADATA;
      sql_print_information(
          "HA: Failed to dump databases to '%s', 'popen' error: %s",
          dump_source.dump_files[i], strerror(errno));
      goto error;
    }

    status = pclose(file);
    // check pclose return status
    rc = (-1 != status && WIFEXITED(status) && !WEXITSTATUS(status)) ? 0 : -1;
    if (rc) {
      sql_print_information(
          "HA: Failed to dump databases to '%s', 'pclose' error: %s",
          dump_source.dump_files[i], strerror(errno));
      rc = SDB_HA_DUMP_METADATA;
      goto error;
    }
  }
  sql_print_information("HA: Dump metadata succeeded");
done:
  return rc;
error:
  goto done;
}

// initialize current instance global and instance state by copying
// candidated instance's global and instance state
static int copy_dump_source_state(ha_recover_replay_thread *ha_thread,
                                  Sdb_conn &sdb_conn,
                                  ha_dump_source &dump_source) {
  int rc = 0, sql_id = -1;
  bson::BSONObj cond, temp, obj, hint;
  Sdb_cl gstate_cl, istate_cl;
  char err_buf[HA_BUF_LEN] = {0};
  const char *sdb_group_name = ha_thread->sdb_group_name;

  // clear current instance state
  int instance_id = ha_thread->instance_id;
  DBUG_ASSERT(instance_id > 0);

  rc = ha_get_global_state_cl(sdb_conn, sdb_group_name, gstate_cl);
  HA_RC_CHECK(rc, done,
              "HA: Failed to get global state table '%s', sequoiadb error: %s",
              HA_GLOBAL_STATE_CL, ha_error_string(sdb_conn, rc, err_buf));

  rc = ha_get_instance_state_cl(sdb_conn, sdb_group_name, istate_cl);
  HA_RC_CHECK(rc, done,
              "HA: Failed to get instance state table '%s', "
              "sequoiadb error: %s",
              HA_INSTANCE_STATE_CL, ha_error_string(sdb_conn, rc, err_buf));

  cond = BSON(HA_FIELD_INSTANCE_ID << BSON("$et" << instance_id));
  rc = istate_cl.del(cond);
  HA_RC_CHECK(rc, done,
              "HA: Unable to delete instance state, sequoiadb error: %s",
              ha_error_string(sdb_conn, rc, err_buf));

  // copy dump source state(sql_id) to current instance
  cond = BSON(HA_FIELD_INSTANCE_ID << dump_source.dump_source_id);
  rc = gstate_cl.query(cond);
  rc = rc ? rc : gstate_cl.next(obj, false);
  HA_RC_CHECK(rc, done,
              "HA: Unable to get dump source's global state, "
              "sequoiadb error: %s",
              ha_error_string(sdb_conn, rc, err_buf));

  cond = BSON(HA_FIELD_INSTANCE_ID << instance_id);
  sql_id = obj.getIntField(HA_FIELD_SQL_ID);
  temp = BSON("$set" << BSON(HA_FIELD_SQL_ID << sql_id << HA_FIELD_INSTANCE_ID
                                             << instance_id));
  rc = gstate_cl.upsert(temp, cond);
  HA_RC_CHECK(rc, done,
              "HA: Failed to initizlize global state, sequoiadb error: %s",
              ha_error_string(sdb_conn, rc, err_buf));

  cond = BSON(HA_FIELD_INSTANCE_ID << dump_source.dump_source_id);
  rc = istate_cl.query(cond);
  do {
    const char *db_name = NULL, *tbl_name = NULL, *op_type = NULL;
    int sql_id = HA_INVALID_SQL_ID;
    rc = rc ? rc : istate_cl.next(obj, false);
    if (HA_ERR_END_OF_FILE == rc) {
      rc = 0;
      break;
    }
    HA_RC_CHECK(rc, done,
                "HA: Unable to get dump source instance state, "
                "sequoiadb error: %s",
                ha_error_string(sdb_conn, rc, err_buf));
    db_name = obj.getStringField(HA_FIELD_DB);
    tbl_name = obj.getStringField(HA_FIELD_TABLE);
    op_type = obj.getStringField(HA_FIELD_TYPE);
    sql_id = obj.getIntField(HA_FIELD_SQL_ID);
    temp =
        BSON(HA_FIELD_INSTANCE_ID << instance_id << HA_FIELD_DB << db_name
                                  << HA_FIELD_TABLE << tbl_name << HA_FIELD_TYPE
                                  << op_type << HA_FIELD_SQL_ID << sql_id);
    rc = istate_cl.insert(temp, hint);
    HA_RC_CHECK(rc, done,
                "HA: Failed to initialize instance state, sequoiadb error: %s",
                ha_error_string(sdb_conn, rc, err_buf));
  } while (!rc);
done:
  return rc;
}

static int register_instance_id(ha_recover_replay_thread *ha_thread,
                                Sdb_conn &sdb_conn) {
  int rc = 0;
  char err_buf[HA_BUF_LEN] = {0};
  int instance_id = 0;
  Sdb_cl config_cl;
  bson::BSONObj cond, result, obj, hint;
  bson::BSONObjBuilder obj_builder;

  sql_print_information("HA: Start register instance ID");
  rc = get_local_instance_id(instance_id);
  HA_RC_CHECK(rc, error, "HA: Unable to get instance ID from local file");

  rc = ha_get_config_cl(sdb_conn, HA_GLOBAL_INFO, config_cl);
  HA_RC_CHECK(rc, error, "HA: Unable to get global configuration table '%s'",
              HA_CONFIG_CL);

  rc = get_local_ip_address(ha_local_host_ip, HA_MAX_IP_LEN + 1);
  HA_RC_CHECK(rc, error, "HA: Failed to get local IP address");

  cond = BSON(HA_FIELD_INSTANCE_ID << instance_id);
  rc = config_cl.query(cond);
  if (0 == rc || HA_ERR_END_OF_FILE == rc) {
    rc = config_cl.next(obj, false);
    rc = (HA_ERR_END_OF_FILE == rc) ? 0 : rc;
  }
  HA_RC_CHECK(rc, error,
              "HA: Unable to get instance global configuration from "
              "'%s', sequoiadb error: %s",
              HA_CONFIG_CL, ha_error_string(sdb_conn, rc, err_buf));

  obj_builder.append(HA_FIELD_IP, ha_local_host_ip);
  obj_builder.append(HA_FIELD_PORT, mysqld_port);
  obj_builder.append(HA_FIELD_HOST_NAME, glob_hostname);
  obj_builder.append(HA_FIELD_INSTANCE_GROUP_NAME, ha_thread->group_name);

  // if can't find instance information from global config table
  if (obj.isEmpty()) {
    obj_builder.append(HA_FIELD_DB_TYPE, DB_TYPE);
    if (0 != instance_id) {
      // instance id is not 0, maybe current instance is moved to another
      // instance group
      obj_builder.append(HA_FIELD_INSTANCE_ID, instance_id);
    }
    // instance id is 0 means that 'myid' does not exists.
    obj = obj_builder.obj();
    rc = config_cl.insert(obj, hint, 0, &result);
    // the duplicate key error means the following situations:
    // 1. a new instance is added to the group and the port is already occupied
    //    by another instance on the same host
    // 2. an instance 'myid' file was deleted for some reason,
    //    user reinit the instance and join the group again
    HA_RC_CHECK((SDB_IXM_DUP_KEY == get_sdb_code(rc)), error,
                "HA: Mysql service port: %d is occupied by another instance on "
                "the same host, please choose another service port or clear the"
                "conflicted instance",
                mysqld_port);
    HA_RC_CHECK(rc, error,
                "HA: Failed to register instance ID in '%s', "
                "sequoiadb error: %s",
                HA_CONFIG_CL, ha_error_string(sdb_conn, rc, err_buf));
    if (0 == instance_id) {
      instance_id = result.getIntField(SDB_FIELD_LAST_GEN_ID);
    }
  } else {
    // already exists, update config information
    bson::BSONObj rule_obj;
    rule_obj = BSON("$set" << obj_builder.obj());
    rc = config_cl.update(rule_obj, cond);
    HA_RC_CHECK(rc, error,
                "HA: Failed to update 'IP', 'Port', 'HostName' and "
                "'InstGroupName' configuration in '%s', sequoiadb error: %s",
                HA_CONFIG_CL, ha_error_string(sdb_conn, rc, err_buf));
  }
  DBUG_ASSERT(instance_id > 0);
  ha_thread->instance_id = instance_id;
  sql_print_information("HA: Register instance ID complete, instance ID: %d",
                        instance_id);
done:
  return rc;
error:
  goto done;
}

static int check_if_local_data_expired(ha_recover_replay_thread *ha_thread,
                                       Sdb_conn &sdb_conn, int &expired) {
  int rc = 0;
  Sdb_cl gstate_cl, sql_log_cl;
  longlong count = 0;
  char err_buf[HA_BUF_LEN] = {0};
  int instance_id = ha_thread->instance_id, sql_id = -1;

  DBUG_ASSERT(instance_id > 0);
  rc = ha_get_global_state_cl(sdb_conn, ha_thread->sdb_group_name, gstate_cl);
  HA_RC_CHECK(rc, error,
              "HA: Unable to get global state table '%s', sequoiadb error: %s",
              HA_GLOBAL_STATE_CL, ha_error_string(sdb_conn, rc, err_buf));
  rc = gstate_cl.get_count(count);
  HA_RC_CHECK(rc, error,
              "HA: Unable to get the number of records for global "
              "state table '%s', sequoiadb error: %s",
              HA_GLOBAL_STATE_CL, ha_error_string(sdb_conn, rc, err_buf));

  // global state table is not empty, check if current
  // instance global sql_id is purged(removed from 'HASQLLog')
  if (count) {
    bson::BSONObj result, cond;
    cond = BSON(HA_FIELD_INSTANCE_ID << instance_id);
    rc = gstate_cl.query(cond);
    rc = rc ? rc : gstate_cl.next(result, false);
    if (HA_ERR_END_OF_FILE == rc) {
      sql_print_information(
          "HA: Unable to find global state in '%s', "
          "a new instance added to instance group",
          HA_GLOBAL_STATE_CL);
      rc = 0;
      expired = true;
      goto done;
    }
    HA_RC_CHECK(rc, error,
                "HA: Failed to query global state table '%s', "
                "sequoiadb error: %s",
                HA_GLOBAL_STATE_CL, ha_error_string(sdb_conn, rc, err_buf));

    sql_id = result.getIntField(HA_FIELD_SQL_ID);
    DBUG_ASSERT(sql_id >= 0);
    cond = BSON(HA_FIELD_SQL_ID << sql_id);

    rc = sdb_conn.get_cl(ha_thread->sdb_group_name, HA_SQL_LOG_CL, sql_log_cl);
    HA_RC_CHECK(rc, error,
                "HA: Unable to get sql log table '%s', sequoiadb error: %s",
                HA_SQL_LOG_CL, ha_error_string(sdb_conn, rc, err_buf));
    rc = sql_log_cl.query(cond);
    rc = rc ? rc : sql_log_cl.next(result, false);
    if (HA_ERR_END_OF_FILE == rc) {
      sql_print_information("HA: SQL log: %d is purged before replay", sql_id);
      expired = true;
      rc = 0;
      goto done;
    }
    HA_RC_CHECK(rc, error,
                "HA: Failed to query SQL log table '%s', sequoiadb error: %s",
                HA_SQL_LOG_CL, ha_error_string(sdb_conn, rc, err_buf));
    expired = false;
  } else {  // no records in 'HAGlobalState'
    bson::BSONObj obj, cond, hint;
    sql_print_information("HA: Initialize global state table '%s'",
                          HA_GLOBAL_STATE_CL);
    obj = BSON(HA_FIELD_JOIN_ID << 1 << HA_FIELD_INSTANCE_ID << instance_id
                                << HA_FIELD_SQL_ID << 0);
    rc = gstate_cl.insert(obj, hint);
    if (SDB_IXM_DUP_KEY == get_sdb_code(rc)) {
      sql_print_information(
          "HA: The global state table '%s' has been initialized",
          HA_GLOBAL_STATE_CL);
      cond = BSON(HA_FIELD_JOIN_ID << 1);
      // blocked until first instance finish registration
      gstate_cl.query(cond);
      expired = true;
      rc = 0;
      goto done;
    }
    HA_RC_CHECK(rc, error,
                "HA: Failed to initialize global state table '%s', "
                "sequoiadb error: %s",
                HA_GLOBAL_STATE_CL, ha_error_string(sdb_conn, rc, err_buf));

    // current instance is the first instance in current instance group,
    // persist instance ID to local file
    rc = write_local_instance_id(instance_id);
    HA_RC_CHECK(rc, error, "HA: Failed to persist instance ID: %d",
                instance_id);

    expired = false;
    sql_print_information("HA: Initialization of global state table succeeded");
  }

  if (expired) {
    sql_print_information("HA: Local metadata is expired");
  }
done:
  return rc;
error:
  goto done;
}

static int drop_non_system_databases(MYSQL *conn) {
  int rc = 0;
  MYSQL_RES *result = NULL;
  MYSQL_ROW row;
  char quoted_db_buf[NAME_LEN * 2 + 3] = {0};
  static const int MAX_DROP_DB_LEN = NAME_LEN * 2 + 20;
  char drop_db_sql[MAX_DROP_DB_LEN] = {0};

  rc = mysql_query(conn, C_STRING_WITH_LEN(HA_STMT_SHOW_DATABASES));
  HA_RC_CHECK(rc, error, "HA: Failed to execute '%s', mysql error: %s",
              HA_STMT_SHOW_DATABASES, mysql_error(conn));

  result = mysql_store_result(conn);
  DBUG_ASSERT(NULL != result);

  while ((row = mysql_fetch_row(result))) {
    const char *db = (char *)row[0];
    if (!strcmp(HA_MYSQL_DB, db) || !strcmp(HA_INFORMATION_DB, db) ||
        !strcmp(HA_PERFORMANCE_DB, db)
#ifdef IS_MYSQL
        || !strcmp(HA_SYS_DB, db)
#else
        || !strcmp(HA_TEST_DB, db)
#endif
    ) {
      continue;
    } else {  // drop database db
      char *qdatabase = ha_quote_name(db, quoted_db_buf);
      snprintf(drop_db_sql, MAX_DROP_DB_LEN, "%s %s", HA_STMT_DROP_DATABASE,
               quoted_db_buf);

      rc = mysql_query(conn, drop_db_sql, strlen(drop_db_sql));
      HA_RC_CHECK(rc, error, "HA: Failed to execute '%s', mysql error: %s",
                  drop_db_sql, mysql_error(conn));
    }
  }
done:
  mysql_free_result(result);
  return rc;
error:
  goto done;
}

static int clear_local_meta_data(MYSQL *conn) {
  bool rc = 0;
  static const int MAX_CLEAN_SQL_LEN = 100;
  char sql_buf[MAX_CLEAN_SQL_LEN] = {0};

  sql_print_information("HA: Clean local metadata");
  // 1. set sequoiadb_execute_only_in_mysql = 1
  rc = mysql_query(conn, C_STRING_WITH_LEN(HA_STMT_EXEC_ONLY_IN_MYSQL));
  HA_RC_CHECK(rc, error,
              "HA: Unable to open 'sequoiadb_execute_only_in_mysql'"
              "before clearing local metadata, mysql error: %s",
              mysql_error(conn));

  // 2. set names for conn
  rc = mysql_query(conn, C_STRING_WITH_LEN(HA_STMT_SET_NAMES));
  HA_RC_CHECK(rc, error, "HA: Failed to execute '%s', mysql error: %s",
              HA_STMT_SET_NAMES, mysql_error(conn));

  // 3. get databases by "show databases", delete non-system databases
  {
    rc = drop_non_system_databases(conn);
    HA_RC_CHECK(rc, error, "HA: Failed to drop non-system databases");
  }

  // 4. delete users not include instance group user and root
  {
    snprintf(sql_buf, MAX_CLEAN_SQL_LEN, "%s and User != '%s'",
             HA_STMT_DELETE_USER, ha_inst_group_user);
    rc = mysql_query(conn, sql_buf, strlen(sql_buf));
    HA_RC_CHECK(rc, error,
                "HA: Failed to clean user from 'mysql.user' table, "
                "mysql error: %s",
                mysql_error(conn));
  }

  // 5. delete functions and procedures
  {
    snprintf(sql_buf, MAX_CLEAN_SQL_LEN, "%s", HA_STMT_DELETE_ROUTINES);
    rc = mysql_query(conn, sql_buf, strlen(sql_buf));
    HA_RC_CHECK(rc, error,
                "HA: Failed to clean procedures and functions from "
                "'mysql.proc' table, mysql error: %s",
                mysql_error(conn));
  }
  sql_print_information("HA: Clearing local metadata succeeded");
done:
  return rc;
error:
  goto done;
}

// clear current instance state and init its global state
static int init_global_state(ha_recover_replay_thread *ha_thread,
                             Sdb_conn &sdb_conn) {
  int rc = 0;
  bson::BSONObj cond, obj;
  Sdb_cl gstate_cl, istate_cl, sql_log_cl;
  char err_buf[HA_BUF_LEN] = {0};
  char *sdb_group_name = ha_thread->sdb_group_name;

  // clear current instance state
  int instance_id = ha_thread->instance_id;
  DBUG_ASSERT(instance_id > 0);
  cond = BSON(HA_FIELD_INSTANCE_ID << BSON("$et" << instance_id));

  rc = ha_get_global_state_cl(sdb_conn, sdb_group_name, gstate_cl);
  HA_RC_CHECK(rc, error,
              "HA: Unable to get global state table '%s', sequoiadb error: %s",
              HA_GLOBAL_STATE_CL, ha_error_string(sdb_conn, rc, err_buf));

  rc = ha_get_instance_state_cl(sdb_conn, sdb_group_name, istate_cl);
  HA_RC_CHECK(
      rc, error,
      "HA: Unable to get instance state table '%s', sequoiadb error: %s",
      HA_INSTANCE_STATE_CL, ha_error_string(sdb_conn, rc, err_buf));

  rc = sdb_conn.get_cl(sdb_group_name, HA_SQL_LOG_CL, sql_log_cl);
  HA_RC_CHECK(rc, error,
              "HA: Unable to get sql log table '%s', sequoiadb error: %s",
              HA_SQL_LOG_CL, ha_error_string(sdb_conn, rc, err_buf));

  rc = istate_cl.del(cond);
  HA_RC_CHECK(rc, error,
              "HA: Failed to clear instance state, sequoiadb error: %s",
              ha_error_string(sdb_conn, rc, err_buf));
  obj = BSON("$set" << BSON(HA_FIELD_SQL_ID << 0 << HA_FIELD_INSTANCE_ID
                                            << instance_id));
  // init current instance's global state to 0
  rc = gstate_cl.upsert(obj, cond);
  HA_RC_CHECK(rc, error,
              "HA: Failed to initialize global state, sequoiadb error: %s",
              ha_error_string(sdb_conn, rc, err_buf));
done:
  return rc;
error:
  goto done;
}

// dump full metadata from dump source by executing 'mysqldump' command
// note: can't print error log here, or automated testing may fail
static int dump_full_meta_data(ha_recover_replay_thread *ha_thread,
                               Sdb_conn &sdb_conn,
                               ha_dump_source &dump_source) {
  int rc = 0;
  sql_print_information("HA: Start dump full metadata");
  rc = set_dump_source(ha_thread, sdb_conn, dump_source);
  if (rc) {
    sql_print_information("HA: Failed to set dump source");
    goto error;
  }

  rc = copy_dump_source_state(ha_thread, sdb_conn, dump_source);
  if (rc) {
    sql_print_information("HA: Failed to copy dump source state");
  }

  rc = dump_meta_data(ha_thread, dump_source);
  if (rc) {
    sql_print_information("HA: Failed to dump metadata from '%s:%d'",
                          dump_source.dump_host, dump_source.dump_port);
    goto error;
  }
  sql_print_information("HA: Dump full metadata succeeded");
done:
  return rc;
error:
  goto done;
}

// build source command and prepare recover current instance
static void build_source_command(char *cmd, int max_cmd_len,
                                 const char *file_name) {
  int end = 0;
  end += snprintf(cmd, max_cmd_len, "%s/bin/mysql -u%s -p%s -h%s -P%d ",
                  mysql_home_ptr, ha_inst_group_user, ha_inst_group_passwd,
                  ha_local_host_ip, mysqld_port);
  end += snprintf(cmd + end, max_cmd_len,
                  "-e 'set sequoiadb_execute_only_in_mysql = 1; source ");
  end += snprintf(cmd + end, max_cmd_len, "%s;' -f", file_name);
}

// recover current instance metadata by executing 'source' command
static int recover_meta_data(ha_recover_replay_thread *ha_thread,
                             Sdb_conn &sdb_conn, ha_dump_source &dump_source) {
  int rc = 0, status = -1;
  sql_print_information("HA: Start recover local instance");

  // before recovery set current mysql variable "read_only" to on
  rc = set_mysql_read_only(ha_mysql, true);
  if (rc) {
    sql_print_information("HA: Unable to enable 'read_only', mysql error: %s",
                          mysql_error(ha_mysql));
    goto error;
  }

  // clear local metadata before recovery
  rc = clear_local_meta_data(ha_mysql);
  HA_RC_CHECK(rc, error, "HA: Failed to clean local metadata");

  // recover metadata
  {
    char buff[HA_BUF_LEN] = {0};
    FILE *file = NULL;
    static const int MAX_SOURCE_CMD_LEN = FN_REFLEN * 2 + 100;
    char source_cmd[MAX_SOURCE_CMD_LEN] = {0};

    for (size_t i = 0; i < HA_DUMP_FILE_NUM; i++) {
      const char *file_name = dump_source.dump_files[i];
      build_source_command(source_cmd, MAX_SOURCE_CMD_LEN, file_name);

      file = popen(source_cmd, "r");
      if (NULL == file && 0 == errno) {
        rc = SDB_HA_OOM;
        sql_print_error("HA: Out of memory while recovering metadata from '%s'",
                        file_name);
        goto error;
      } else if (NULL == file) {  // errno has been set
        // do not print error log , or parallel automated testing may fails
        rc = SDB_HA_RECOVER_METADATA;
        sql_print_information(
            "HA: Failed to recover metadata from '%s' by 'source' command, "
            "'popen' error: %s",
            file_name, strerror(errno));
        goto error;
      }

      while (fgets(buff, HA_BUF_LEN, file)) {
        // remove '\n' in buff
        int len = strlen(buff);
        if (len > 0) {
          buff[len - 1] = '\0';
        }

        // Because the 'mysql' database was not deleted,  the following errors
        // will be reported when using the source command to restore 'mysql'
        // database
        // MySQL error code 1050 (ER_TABLE_EXISTS_ERROR): Table already exists
        // MySQL error code 1062 (ER_DUP_ENTRY): Duplicate entry
        if (0 == strncmp(buff, STRING_WITH_LEN("ERROR 1050"))) {
          continue;
        } else if (0 == strncmp(buff, STRING_WITH_LEN("ERROR 1062"))) {
          continue;
        } else if (0 == strncmp(buff, STRING_WITH_LEN("ERROR "))) {
          sql_print_information(
              "HA: A error '%s' that can't be ignored "
              "while executing 'source' command occurred",
              buff);
          pclose(file);
          rc = SDB_HA_RECOVER_METADATA;
          goto error;
        }
      }
      status = pclose(file);
      // check pclose return status
      rc = (-1 != status && WIFEXITED(status) && !WEXITSTATUS(status)) ? 0 : -1;
      if (rc) {
        rc = SDB_HA_RECOVER_METADATA;
        sql_print_information(
            "HA: Failed to recover metadata from '%s' "
            "by 'source' command, 'pclose' error: %s",
            dump_source.dump_files[i], strerror(errno));
        goto error;
      }
    }
  }

  // execute flush privileges, update cache for 'mysql.user'
  // after restoring 'mysql.user' table with 'source' command
  rc = mysql_query(ha_mysql, C_STRING_WITH_LEN(HA_STMT_FLUSH_PRIVILEGES));
  HA_RC_CHECK(rc, error,
              "HA: Failed to execute 'flush privileges' command"
              "after recovering metadata with 'source' command");

  rc = set_mysql_read_only(ha_mysql, false);
  if (rc) {
    sql_print_information("HA: Unable to disable 'read_only', mysql error: %s",
                          mysql_error(ha_mysql));
    goto error;
  }
  sql_print_information("HA: Recovery of metadata from dump source succeeded");
done:
  return rc;
error:
  goto done;
}

// before create instance group user, mysql service must been started
static bool wait_for_mysqld_service() {
  bool mysqld_failed = false;
#ifdef IS_MYSQL
  extern my_thread_handle signal_thread_id;
#endif
  mysql_mutex_lock(&LOCK_server_started);
  // end 'HA' thread as soon as possible if found mysqld failed to start
  while (!mysqld_server_started && !mysqld_failed) {
    timespec abstime;
    sdb_set_timespec(abstime, 1);
    mysql_cond_timedwait(&COND_server_started, &LOCK_server_started, &abstime);
    mysqld_failed |= abort_loop;
#ifdef IS_MARIADB
    // thread_scheduler will be set to NULL if found mysqld failed to start
    mysqld_failed |= (NULL == thread_scheduler);
#else
    // signal_thread_id.thread will be set to 0 if mysqld fails
    mysqld_failed |= (0 == signal_thread_id.thread);
#endif
  }
  mysql_mutex_unlock(&LOCK_server_started);
  return mysqld_failed;
}

static inline bool is_ignore_error(MYSQL *conn) {
  bool can_ignore = false;
  switch (mysql_errno(conn)) {
    case ER_DB_DROP_EXISTS:
    case ER_BAD_TABLE_ERROR:
    case ER_TABLE_EXISTS_ERROR:
    case ER_DB_CREATE_EXISTS:
    case ER_SP_DOES_NOT_EXIST:
    case ER_BAD_DB_ERROR:
    case ER_SP_ALREADY_EXISTS:
    case ER_CANNOT_USER:
    case ER_NO_SUCH_TABLE:
    case ER_DUP_FIELDNAME:
    case ER_CANT_DROP_FIELD_OR_KEY:
    case ER_DUP_KEYNAME:
    case ER_BAD_FIELD_ERROR:
    case ER_TRG_DOES_NOT_EXIST:
    case ER_TRG_ALREADY_EXISTS:
#ifdef IS_MARIADB
    case ER_UNKNOWN_VIEW:
#endif
      can_ignore = true;
      break;
  }
  return can_ignore;
}

// replay SQL statements fetched from 'HASQLLog'
static int replay_sql_stmt_loop(ha_recover_replay_thread *ha_thread,
                                Sdb_conn &sdb_conn) {
  // replay limit every time
  static const int REPLAY_LIMIT = 100;
  static const int SLEEP_SECONDS = 2;
  static const int MAX_TRY_COUNT = 3;

  int rc = 0;
  int glob_executed_sql_id;
  Sdb_cl gstate_cl, sql_log_cl, istate_cl;
  bson::BSONObjBuilder builder, simple_builder;
  bson::BSONObj result, cond, obj, order_by, attr;
  struct timespec abstime;
  char quoted_name_buf[NAME_LEN * 2 + 3] = {0};
  char err_buf[HA_BUF_LEN] = {0};
  char *sdb_group_name = ha_thread->sdb_group_name;
  int curr_executed = 0;
  const CHARSET_INFO *charset_info = NULL;
  String src_sql, dst_sql;
  char cached_record_key[HA_MAX_CACHED_RECORD_KEY_LEN] = {0};
  const char *query = NULL, *db_name = NULL, *table_name = NULL;
  const char *op_type = NULL, *session_attrs = NULL;
  int client_charset_num = 0;
  int owner = HA_INVALID_INST_ID;
  int sql_id = HA_INVALID_SQL_ID;

  DBUG_ENTER("replay_sql_stmt_loop");
  DBUG_ASSERT(ha_thread->instance_id > 0);

  rc = sdb_conn.get_cl(sdb_group_name, HA_GLOBAL_STATE_CL, gstate_cl);
  HA_RC_CHECK(rc, error,
              "HA: Unable to get global state table '%s', sequoiadb error: %s",
              HA_GLOBAL_STATE_CL, ha_error_string(sdb_conn, rc, err_buf));

  // get global executed SQL ID from 'HAGlobalState' for current instance
  cond = BSON(HA_FIELD_INSTANCE_ID << ha_thread->instance_id);
  rc = gstate_cl.query(cond);
  rc = rc ? rc : gstate_cl.next(result, false);
  HA_RC_CHECK(rc, error,
              "HA: Unable to find global state in '%s', sequoiadb error: %s",
              HA_GLOBAL_STATE_CL, ha_error_string(sdb_conn, rc, err_buf));

  rc = sdb_conn.get_cl(sdb_group_name, HA_SQL_LOG_CL, sql_log_cl);
  HA_RC_CHECK(rc, error,
              "HA: Unable to get SQL log table '%s', sequoiadb error: %s",
              HA_SQL_LOG_CL, ha_error_string(sdb_conn, rc, err_buf));

  rc = ha_get_instance_state_cl(sdb_conn, sdb_group_name, istate_cl);
  HA_RC_CHECK(rc, error,
              "HA: Unable to get instance state table '%s', "
              "sequoiadb error: %s",
              HA_INSTANCE_STATE_CL, ha_error_string(sdb_conn, rc, err_buf));

  glob_executed_sql_id = result.getIntField(HA_FIELD_SQL_ID);
  rc = (glob_executed_sql_id < 0) ? 1 : 0;
  HA_RC_CHECK(rc, error, "HA: Wrong global state '%d' in '%s'",
              glob_executed_sql_id, HA_GLOBAL_STATE_CL);

  SDB_LOG_INFO("HA: Instance %d start with global executed SQL ID: %d",
               ha_thread->instance_id, glob_executed_sql_id);
  sdb_conn.set_pushed_autocommit();
  attr = BSON(HA_TRANSACTION_LOCK_WAIT << true);
  rc = sdb_conn.set_session_attr(attr);
  HA_RC_CHECK(rc, error,
              "HA: Failed to set '%s' before replay SQL log, "
              "sequoiadb error: %s",
              HA_TRANSACTION_LOCK_WAIT, ha_error_string(sdb_conn, rc, err_buf));

  rc = mysql_query(ha_mysql, C_STRING_WITH_LEN(HA_STMT_EXEC_ONLY_IN_MYSQL));
  HA_RC_CHECK(rc, error,
              "HA: Unable to open 'sequoiadb_execute_only_in_mysql'"
              "before replay SQL log, mysql error: %s",
              mysql_error(ha_mysql));

  rc = set_mysql_read_only(ha_mysql, false);
  HA_RC_CHECK(rc, error, "HA: Unable to set 'read_only' to 0, mysql error: %s",
              mysql_error(ha_mysql));

  // set 'order_by' flag for querying SQL log order by 'SQLID' field
  order_by = BSON(HA_FIELD_SQL_ID << 1);

  while (true) {
    curr_executed = 0;
    builder.reset();

    {
      bson::BSONObjBuilder sub_builder(builder.subobjStart(HA_FIELD_SQL_ID));
      sub_builder.append("$gt", glob_executed_sql_id);
      sub_builder.doneFast();
    }
    cond = builder.done();
    rc = sql_log_cl.query(cond, SDB_EMPTY_BSON, order_by, SDB_EMPTY_BSON, 0,
                          REPLAY_LIMIT);
    if (SDB_DMS_NOTEXIST == get_sdb_code(rc)) {
      // stop current instance if the instance group has been cleared
      sql_print_error(
          "HA: SQL log table does not exists, please check if the instance "
          "group has been cleared");
      goto error;
    }
    HA_RC_CHECK(rc, sleep_secs,
                "HA: Failed to get SQL log, sequoiadb error: %s",
                ha_error_string(sdb_conn, rc, err_buf));

    while (0 == (rc = sql_log_cl.next(result, false))) {
      curr_executed++;
      bson::BSONObjIterator iter(result);
      while (iter.more()) {
        bson::BSONElement elem = iter.next();
        if (0 == strcmp(elem.fieldName(), HA_FIELD_SQL)) {
          query = elem.valuestr();
        } else if (0 == strcmp(elem.fieldName(), HA_FIELD_DB)) {
          db_name = elem.valuestr();
        } else if (0 == strcmp(elem.fieldName(), HA_FIELD_TABLE)) {
          table_name = elem.valuestr();
        } else if (0 == strcmp(elem.fieldName(), HA_FIELD_TYPE)) {
          op_type = elem.valuestr();
        } else if (0 == strcmp(elem.fieldName(), HA_FIELD_SESSION_ATTRS)) {
          session_attrs = elem.valuestr();
        } else if (0 == strcmp(elem.fieldName(), HA_FIELD_CLIENT_CHARSET_NUM)) {
          client_charset_num = elem.numberInt();
        } else if (0 == strcmp(elem.fieldName(), HA_FIELD_OWNER)) {
          owner = elem.numberInt();
        } else if (0 == strcmp(elem.fieldName(), HA_FIELD_SQL_ID)) {
          sql_id = elem.numberInt();
        }
      }

      DBUG_ASSERT(sql_id >= 0);
      if (owner == ha_thread->instance_id) {
        // update its own global state
        builder.reset();
        {
          bson::BSONObjBuilder sub_builder(builder.subobjStart("$set"));
          sub_builder.append(HA_FIELD_SQL_ID, sql_id);
          sub_builder.doneFast();
          obj = builder.done();
        }

        simple_builder.reset();
        simple_builder.append(HA_FIELD_INSTANCE_ID, ha_thread->instance_id);
        cond = simple_builder.done();

        rc = gstate_cl.upsert(obj, cond);
        HA_RC_CHECK(rc, sleep_secs,
                    "HA: Failed to update global state for current instance, "
                    "sequoiadb error: %s",
                    ha_error_string(sdb_conn, rc, err_buf));
        glob_executed_sql_id = sql_id;
        continue;
      }

      // set session attributes
      if (strlen(session_attrs)) {
        rc = mysql_query(ha_mysql, session_attrs, strlen(session_attrs));
        // if abort_loop become true, don't report errors, or mysql automated
        // testing will report errors if found '[ERROR]' in error log
        if (rc && abort_loop) {
          rc = 0;
          break;
        }
      }

      // get sql stmt client charset
      {
        charset_info = get_charset(client_charset_num, MYF(MY_WME));
        if (NULL == charset_info) {
          sql_print_error("HA: Failed to get charset by charset num");
          goto sleep_secs;
        }
      }

      // build change database command(use database) and execute it
      char use_db_cmd[HA_MAX_USE_DB_CMD_LEN] = {0};
      if (client_charset_num != (int)system_charset_info->number) {
        // convert database's name charset
        src_sql.set(db_name, strlen(db_name), system_charset_info);
        dst_sql.length(0);
        rc = sdb_convert_charset(src_sql, dst_sql, charset_info);
        HA_RC_CHECK(rc, sleep_secs, "HA: Convert charset error: %d", rc);

        ha_quote_name(dst_sql.c_ptr_safe(), quoted_name_buf);
      } else {
        ha_quote_name(db_name, quoted_name_buf);
      }
      snprintf(use_db_cmd, HA_MAX_USE_DB_CMD_LEN, "USE %s", quoted_name_buf);

      SDB_LOG_DEBUG("HA: Change database to: %s", use_db_cmd);
      rc = mysql_query(ha_mysql, use_db_cmd, strlen(use_db_cmd));

      if (0 == strcmp(op_type, HA_OPERATION_TYPE_DB)) {
        // change database failed
        rc = (ER_BAD_DB_ERROR == mysql_errno(ha_mysql)) ? 0 : rc;
      }

      // if abort_loop become true, don't report errors, or mysql automated
      // testing will report errors if found '[ERROR]' in error log
      if (rc && abort_loop) {
        rc = 0;
        break;
      }
      HA_RC_CHECK(rc, sleep_secs,
                  "HA: Failed to change database: %s for SQL ID: %d, "
                  "mysql error: %s",
                  use_db_cmd, sql_id, mysql_error(ha_mysql));

      SDB_LOG_DEBUG("HA: Replay thread replay SQL: %s with SQL ID: %d", query,
                    sql_id);

      if (strlen(query)) {
        rc = mysql_query(ha_mysql, query, strlen(query));
        mysql_free_result(mysql_store_result(ha_mysql));
      }
      if (rc && is_ignore_error(ha_mysql)) {
        sql_print_information(
            "HA: Failed to replay sql '%s' with SQL ID: %d, "
            "mysql error: %s, ignore this error",
            query, sql_id, mysql_error(ha_mysql));
        rc = 0;
      }
      // if abort_loop become true, don't report errors, or mysql automated
      // testing will report errors if found '[ERROR]' in error log
      if (rc && abort_loop) {
        rc = 0;
        break;
      }
      HA_RC_CHECK(rc, sleep_secs,
                  "HA: Failed to replay sql '%s' with SQL ID: %d, "
                  "mysql error: %s",
                  query, sql_id, mysql_error(ha_mysql));
      SDB_LOG_DEBUG("HA: Replay of '%s' succeeded", query);

      // update instance state and global state
      for (int try_count = MAX_TRY_COUNT; try_count; try_count--) {
        simple_builder.reset();
        simple_builder.append(HA_FIELD_DB, db_name);
        simple_builder.append(HA_FIELD_TABLE, table_name);
        simple_builder.append(HA_FIELD_TYPE, op_type);
        simple_builder.append(HA_FIELD_INSTANCE_ID, ha_thread->instance_id);
        cond = simple_builder.done();

        builder.reset();
        {
          bson::BSONObjBuilder sub_builder(builder.subobjStart("$set"));
          sub_builder.append(HA_FIELD_SQL_ID, sql_id);
          sub_builder.doneFast();
        }
        obj = builder.done();
        rc = istate_cl.upsert(obj, cond);
        if (rc) {
          sleep(1);
          continue;
        }

        simple_builder.reset();
        simple_builder.append(HA_FIELD_INSTANCE_ID, ha_thread->instance_id);
        cond = simple_builder.done();
        rc = gstate_cl.upsert(obj, cond);
        if (rc) {
          sleep(1);
          continue;
        }
        glob_executed_sql_id = sql_id;
        break;
      }
      // update cached instance state
      snprintf(cached_record_key, HA_MAX_CACHED_RECORD_KEY_LEN, "%s-%s-%s",
               db_name, table_name, op_type);
      if (0 == rc) {
        rc = ha_update_cached_record(ha_thread->inst_state_cache,
                                     HA_KEY_MEM_INST_STATE_CACHE,
                                     cached_record_key, sql_id);
        // if its oom, stop current instance
        if (rc) {
          goto error;
        }
      }
      HA_RC_CHECK(rc, sleep_secs,
                  "HA: Failed to update instance and global state, "
                  "sequoiadb error: %s",
                  ha_error_string(sdb_conn, rc, err_buf));
      // flush privileges after replay DCL
      if (0 == strcmp(op_type, HA_OPERATION_TYPE_DCL)) {
        rc = mysql_query(ha_mysql, C_STRING_WITH_LEN(HA_STMT_FLUSH_PRIVILEGES));
        // if the main thread is about to terminate
        if (rc && abort_loop) {
          rc = 0;
          break;
        } else if (rc) {
          sql_print_warning(
              "HA: Failed to flush privileges after replay DCL statement");
          rc = 0;
        }
      }
    }

    if (rc && HA_ERR_END_OF_FILE != rc) {
      sql_print_error("HA: Failed to fetch SQL log, sequoiadb error: %s",
                      ha_error_string(sdb_conn, rc, err_buf));
    }
  sleep_secs:
    if (!sdb_conn.is_valid()) {
      // TODO: if failed to connect, write error information into state table
      rc = sdb_conn.connect();
      if (rc) {
        sql_print_error(
            "HA: Unable to connect to sequoiadb, sequoiadb error: %s",
            ha_error_string(sdb_conn, rc, err_buf));
      }
    }
    if (curr_executed < REPLAY_LIMIT) {
      sdb_set_timespec(abstime, SLEEP_SECONDS);
      rc = mysql_cond_timedwait(&ha_thread->replay_stopped_cond,
                                &ha_thread->replay_stopped_mutex, &abstime);
      DBUG_ASSERT(rc == 0 || rc == ETIMEDOUT);
      // get stop signal, stop this loop
      if (0 == rc) {
        sql_print_information("HA: Receive stop signal, stop replay thread");
        break;
      }
      rc = 0;
    }
  }
done:
  DBUG_RETURN(rc);
error:
  goto done;
}

// necessary work for ending HA thread
void recover_and_replay_thread_end(THD *thd) {
#ifdef IS_MYSQL
  my_thread_end();
  my_thread_exit(0);
#else
  if (thd) {
    thread_count++;
    delete thd;
  }
  my_thread_end();
#endif
}

// load 'HAInstanceState' table's records into cached instance state
// (ha_thread.inst_state_cache) for current instance
int load_inst_state_into_cache(ha_recover_replay_thread *ha_thread,
                               Sdb_conn &sdb_conn, HASH &cache) {
  DBUG_ENTER("load_inst_state_into_cache");
  int rc = 0;
  Sdb_cl inst_state_cl;
  bson::BSONObj cond, result;
  char err_buf[HA_BUF_LEN] = {0};

  SDB_LOG_INFO("HA: Load instance state into cache");
  rc = ha_get_instance_state_cl(sdb_conn, ha_thread->sdb_group_name,
                                inst_state_cl);
  HA_RC_CHECK(rc, error,
              "HA: Unable to get instance state table '%s', "
              "sequoiadb error: %s",
              HA_INSTANCE_STATE_CL, ha_error_string(sdb_conn, rc, err_buf));

  cond = BSON(HA_FIELD_INSTANCE_ID << ha_thread->instance_id);
  rc = inst_state_cl.query(cond);
  HA_RC_CHECK(rc, error,
              "HA: Failed to get records from %s for instance %d, "
              "sequoiadb error: %s",
              HA_INSTANCE_STATE_CL, ha_thread->instance_id,
              ha_error_string(sdb_conn, rc, err_buf));
  while (!inst_state_cl.next(result, false) && !result.isEmpty()) {
    const char *db_name = result.getStringField(HA_FIELD_DB);
    const char *table_name = result.getStringField(HA_FIELD_TABLE);
    const char *op_type = result.getStringField(HA_FIELD_TYPE);
    int sql_id = result.getIntField(HA_FIELD_SQL_ID);
    int key_len = strlen(db_name) + strlen(table_name) + strlen(op_type) + 2;
    ha_cached_record *record = NULL;
    char *key = NULL;

    if (NULL == sdb_multi_malloc(HA_KEY_MEM_INST_STATE_CACHE,
                                 MYF(MY_WME | MY_ZEROFILL), &record,
                                 sizeof(ha_cached_record), &key, key_len + 1,
                                 NullS)) {
      rc = SDB_HA_OOM;
      sql_print_information(
          "HA: Out of memory while initializing local instance cache");
      goto error;
    }

    snprintf(key, key_len + 1, "%s-%s-%s", db_name, table_name, op_type);
    key[key_len] = '\0';
    record->key = key;
    record->sql_id = sql_id;
    if (my_hash_insert(&cache, (uchar *)record)) {
      rc = SDB_HA_OOM;
      sql_print_information(
          "HA: Out of memory while loading instance state to cache");
      goto error;
    }
  }
  sql_print_information("HA: Loading instance state complete, load %ld records",
                        cache.records);
done:
  DBUG_RETURN(rc);
error:
  goto done;
}

// call kill_mysql notifies main thread to abort current instance
void ha_kill_mysqld(THD *thd) {
#ifndef NO_EMBEDDED_ACCESS_CHECKS
#ifdef IS_MARIADB
  kill_mysql(thd);
#else
  extern my_thread_handle signal_thread_id;
  if (0 != signal_thread_id.thread) {
    kill_mysql();
  }
#endif
#endif
}

// wake up blocked threads in 'persist_sql_stmt'
void wake_up_sql_persistence_threads(ha_recover_replay_thread *ha_thread) {
  mysql_mutex_lock(&ha_thread->recover_finished_mutex);
  // wake up blocked threads in 'persist_sql_stmt'
  if (!ha_thread->recover_finished) {
    ha_thread->recover_finished = true;
  }
  mysql_cond_broadcast(&ha_thread->recover_finished_cond);
  mysql_mutex_unlock(&ha_thread->recover_finished_mutex);
}

// HA thread entry
void *ha_recover_and_replay(void *arg) {
// HA function is not supported for embedded mysql
#ifndef NO_EMBEDDED_ACCESS_CHECKS
  int rc = 0, local_metadata_expired = false;
  char err_buf[HA_BUF_LEN] = {0};
  ha_recover_replay_thread *ha_thread = (ha_recover_replay_thread *)arg;
  DBUG_ASSERT(NULL != ha_thread);

  // init thd for 'HA' thread
  ha_thread->thd = create_ha_thd();
  if (NULL == ha_thread->thd) {
    sql_print_error("HA: Out of memory in 'HA' thread");
    ha_kill_mysqld(ha_thread->thd);
    return NULL;
  }

  // 1. create thread local var and init sequoiadb connection
  Sdb_conn sdb_conn(sdb_thd_id(ha_thread->thd), true);

  sql_print_information("HA: Start 'HA' thread");
  mysql_mutex_lock(&ha_thread->replay_stopped_mutex);

  try {
    // 2. start dump and recover process after mysqld service
    bool mysqld_failed = wait_for_mysqld_service();
    HA_RC_CHECK(mysqld_failed, error,
                "HA: The mysqld process was detected to be terminating, "
                "stop 'HA' thread, please check MySQL startup log");

    rc = sdb_conn.connect();
    HA_RC_CHECK(rc, error,
                "HA: Unable to connect to sequoiadb, sequoiadb error: %s",
                ha_error_string(sdb_conn, rc, err_buf));
    // set isolation level: SDB_TRANS_ISO_RC
    rc = sdb_conn.begin_transaction(ISO_READ_COMMITTED);
    HA_RC_CHECK(rc, error,
                "HA: Failed to start sequoiadb transaction in 'HA' thread, "
                "sequoiadb error: %s",
                ha_error_string(sdb_conn, rc, err_buf));

    // 3. register instance ID
    rc = register_instance_id(ha_thread, sdb_conn);
    HA_RC_CHECK(rc, error, "HA: Failed to register instance ID");

    // 4. ensure that instance group user is available
    rc = ensure_inst_group_user(ha_thread, sdb_conn);
    HA_RC_CHECK(rc, error, "HA: Failed to check instance group user");

    rc = check_if_local_data_expired(ha_thread, sdb_conn,
                                     local_metadata_expired);
    HA_RC_CHECK(rc, error, "HA: Failed to check local metadata");
    if (local_metadata_expired) {
      ha_dump_source dump_source;
      // 5. choose instance and get metadata from another instance
      rc = dump_full_meta_data(ha_thread, sdb_conn, dump_source);
      HA_RC_CHECK(rc, error, "HA: Failed to dump full metadata");

      // 6. recover metadata
      rc = recover_meta_data(ha_thread, sdb_conn, dump_source);
      HA_RC_CHECK(rc, error, "HA: Failed to recover metadata");
    }

    // load current instance state into cache
    rc = load_inst_state_into_cache(ha_thread, sdb_conn,
                                    ha_thread->inst_state_cache);
    HA_RC_CHECK(rc, error, "HA: Failed to load instance state to cache");

    rc = sdb_conn.commit_transaction();
    HA_RC_CHECK(rc, error,
                "HA: Commit transaction failed in 'HA' thread, "
                "sequoiadb error: %s",
                ha_error_string(sdb_conn, rc, err_buf));

    // wake up blocked threads in 'persist_sql_stmt'
    wake_up_sql_persistence_threads(ha_thread);

    // 7. goto replay "HASQLLog" loop
    rc = replay_sql_stmt_loop(ha_thread, sdb_conn);
    HA_RC_CHECK(rc, error, "HA: Playback process terminated due to an error");
  } catch (std::bad_alloc &e) {
    sql_print_error("HA: Out of memory in 'HA' thread");
    goto error;
  } catch (std::exception &e) {
    sql_print_error("HA: Unexpected error: %s", e.what());
    goto error;
  }
done:
  mysql_close(ha_mysql);
  mysql_mutex_unlock(&ha_thread->replay_stopped_mutex);
  sql_print_information("HA: 'HA' thread terminated");
  // set stop flag, main thread will no longer sleep by checking this flag
  ha_thread->stopped = true;
  recover_and_replay_thread_end(ha_thread->thd);
  return NULL;
error:
  sdb_conn.rollback_transaction();
  wake_up_sql_persistence_threads(ha_thread);
  ha_kill_mysqld(ha_thread->thd);
  goto done;
#endif
}
