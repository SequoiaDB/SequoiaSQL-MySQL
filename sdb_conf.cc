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

#include "sdb_conf.h"
#include "sdb_lock.h"

static const char *SDB_ADDR_DFT = "localhost:11810";
static const char *SDB_USER_DFT = "";
static const char *SDB_PASSWORD_DFT = "";
static const my_bool SDB_USE_PARTITION_DFT = TRUE;
static const my_bool SDB_DEBUG_LOG_DFT = FALSE;
static const my_bool SDB_DEFAULT_USE_BULK_INSERT = TRUE;
static const my_bool SDB_DEFAULT_USE_AUTOCOMMIT = TRUE;
static const int SDB_DEFAULT_BULK_INSERT_SIZE = 100;
static const int SDB_DEFAULT_REPLICA_SIZE = -1;
static const uint SDB_SELECTOR_PUSHDOWN_THRESHOLD = 30;
/*temp parameter "OPTIMIZER_SWITCH_SELECT_COUNT", need remove later*/
static const my_bool OPTIMIZER_SWITCH_SELECT_COUNT = TRUE;
my_bool sdb_optimizer_select_count = OPTIMIZER_SWITCH_SELECT_COUNT;

char *sdb_conn_str = NULL;
char *sdb_user = NULL;
char *sdb_password = NULL;
my_bool sdb_use_partition = SDB_USE_PARTITION_DFT;
my_bool sdb_use_bulk_insert = SDB_DEFAULT_USE_BULK_INSERT;
int sdb_bulk_insert_size = SDB_DEFAULT_BULK_INSERT_SIZE;
int sdb_replica_size = SDB_DEFAULT_REPLICA_SIZE;
my_bool sdb_use_autocommit = SDB_DEFAULT_USE_AUTOCOMMIT;
my_bool sdb_debug_log = SDB_DEBUG_LOG_DFT;

static String sdb_encoded_password;
static Sdb_encryption sdb_passwd_encryption;
static Sdb_rwlock sdb_password_lock;

static int sdb_conn_addr_validate(THD *thd, struct st_mysql_sys_var *var,
                                  void *save, struct st_mysql_value *value) {
  // The buffer size is not important. Because st_mysql_value::val_str
  // internally calls the Item_string::val_str, which doesn't need a buffer.
  static const uint SDB_CONN_ADDR_BUF_SIZE = 3072;
  char buff[SDB_CONN_ADDR_BUF_SIZE];
  int len = sizeof(buff);
  const char *arg_conn_addr = value->val_str(value, buff, &len);

  Sdb_conn_addrs parser;
  int rc = parser.parse_conn_addrs(arg_conn_addr);
  *static_cast<const char **>(save) = (0 == rc) ? arg_conn_addr : NULL;
  return rc;
}

static void sdb_password_update(THD *thd, struct st_mysql_sys_var *var,
                                void *var_ptr, const void *save) {
  Sdb_rwlock_write_guard guard(sdb_password_lock);
  const char *new_password = *static_cast<const char *const *>(save);
  sdb_password = const_cast<char *>(new_password);
  sdb_encrypt_password();
}

static MYSQL_SYSVAR_STR(conn_addr, sdb_conn_str,
                        PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_MEMALLOC,
                        "SequoiaDB addresses (Default: localhost:11810).",
                        sdb_conn_addr_validate, NULL, SDB_ADDR_DFT);
static MYSQL_SYSVAR_STR(user, sdb_user,
                        PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_MEMALLOC,
                        "SequoiaDB authentication user "
                        "(Default value is empty).",
                        NULL, NULL, SDB_USER_DFT);
static MYSQL_SYSVAR_STR(password, sdb_password,
                        PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_MEMALLOC,
                        "SequoiaDB authentication password "
                        "(Default value is empty).",
                        NULL, sdb_password_update, SDB_PASSWORD_DFT);
static MYSQL_SYSVAR_BOOL(use_partition, sdb_use_partition, PLUGIN_VAR_OPCMDARG,
                         "Create partition table on SequoiaDB. "
                         "Enabled by default.",
                         NULL, NULL, SDB_USE_PARTITION_DFT);
static MYSQL_SYSVAR_BOOL(use_bulk_insert, sdb_use_bulk_insert,
                         PLUGIN_VAR_OPCMDARG,
                         "Enable bulk insert to SequoiaDB. Enabled by default.",
                         NULL, NULL, SDB_DEFAULT_USE_BULK_INSERT);
static MYSQL_SYSVAR_INT(bulk_insert_size, sdb_bulk_insert_size,
                        PLUGIN_VAR_OPCMDARG,
                        "Maximum number of records per bulk insert "
                        "(Default: 100).",
                        NULL, NULL, SDB_DEFAULT_BULK_INSERT_SIZE, 1, 100000, 0);
static MYSQL_SYSVAR_INT(replica_size, sdb_replica_size, PLUGIN_VAR_OPCMDARG,
                        "Replica size of write operations "
                        "(Default: -1).",
                        NULL, NULL, SDB_DEFAULT_REPLICA_SIZE, -1, 7, 0);
static MYSQL_SYSVAR_BOOL(use_autocommit, sdb_use_autocommit,
                         PLUGIN_VAR_OPCMDARG,
                         "Enable autocommit of SequoiaDB storage engine. "
                         "Enabled by default.",
                         NULL, NULL, SDB_DEFAULT_USE_AUTOCOMMIT);
static MYSQL_SYSVAR_BOOL(debug_log, sdb_debug_log, PLUGIN_VAR_OPCMDARG,
                         "Turn on debug log of SequoiaDB storage engine. "
                         "Disabled by default.",
                         NULL, NULL, SDB_DEBUG_LOG_DFT);
static MYSQL_SYSVAR_BOOL(optimizer_select_count, sdb_optimizer_select_count,
                         PLUGIN_VAR_OPCMDARG,
                         "Optimizer switch for simple select count. "
                         "Enabled by default.",
                         NULL, NULL, TRUE);
static MYSQL_THDVAR_UINT(selector_pushdown_threshold, PLUGIN_VAR_OPCMDARG,
                         "The threshold of selector push down to SequoiaDB.",
                         NULL, NULL, SDB_SELECTOR_PUSHDOWN_THRESHOLD, 0, 100,
                         0);
struct st_mysql_sys_var *sdb_sys_vars[] = {
    MYSQL_SYSVAR(conn_addr),
    MYSQL_SYSVAR(user),
    MYSQL_SYSVAR(password),
    MYSQL_SYSVAR(use_partition),
    MYSQL_SYSVAR(use_bulk_insert),
    MYSQL_SYSVAR(bulk_insert_size),
    MYSQL_SYSVAR(replica_size),
    MYSQL_SYSVAR(use_autocommit),
    MYSQL_SYSVAR(debug_log),
    MYSQL_SYSVAR(optimizer_select_count),
    MYSQL_SYSVAR(selector_pushdown_threshold),
    NULL};

Sdb_conn_addrs::Sdb_conn_addrs() : conn_num(0) {
  for (int i = 0; i < SDB_COORD_NUM_MAX; i++) {
    addrs[i] = NULL;
  }
}

Sdb_conn_addrs::~Sdb_conn_addrs() {
  clear_conn_addrs();
}

void Sdb_conn_addrs::clear_conn_addrs() {
  for (int i = 0; i < conn_num; i++) {
    if (addrs[i]) {
      free(addrs[i]);
      addrs[i] = NULL;
    }
  }
  conn_num = 0;
}

int Sdb_conn_addrs::parse_conn_addrs(const char *conn_addr) {
  int rc = 0;
  const char *p = conn_addr;

  if (NULL == conn_addr || 0 == strlen(conn_addr)) {
    rc = -1;
    goto error;
  }

  clear_conn_addrs();

  while (*p != 0) {
    const char *p_tmp = NULL;
    size_t len = 0;
    if (conn_num >= SDB_COORD_NUM_MAX) {
      goto done;
    }

    p_tmp = strchr(p, ',');
    if (NULL == p_tmp) {
      len = strlen(p);
    } else {
      len = p_tmp - p;
    }
    if (len > 0) {
      char *p_addr = NULL;
      const char *comma_pos = strchr(p, ',');
      const char *colon_pos = strchr(p, ':');
      if (!colon_pos || (comma_pos && comma_pos < colon_pos)) {
        rc = -1;
        goto error;
      }
      p_addr = (char *)malloc(len + 1);
      if (NULL == p_addr) {
        rc = -1;
        goto error;
      }
      memcpy(p_addr, p, len);
      p_addr[len] = 0;
      addrs[conn_num] = p_addr;
      ++conn_num;
    }
    p += len;
    if (*p == ',') {
      p++;
    }
  }

done:
  return rc;
error:
  goto done;
}

const char **Sdb_conn_addrs::get_conn_addrs() const {
  return (const char **)addrs;
}

int Sdb_conn_addrs::get_conn_num() const {
  return conn_num;
}

int sdb_encrypt_password() {
  static const uint DISPLAY_MAX_LEN = 1;
  int rc = 0;
  String src_password(sdb_password, &my_charset_bin);

  rc = sdb_passwd_encryption.encrypt(src_password, sdb_encoded_password);
  if (rc) {
    goto error;
  }

  for (uint i = 0; i < src_password.length(); ++i) {
    src_password[i] = '*';
  }

  if (src_password.length() > DISPLAY_MAX_LEN) {
    src_password[DISPLAY_MAX_LEN] = 0;
  }
done:
  return rc;
error:
  goto done;
}

int sdb_get_password(String &res) {
  Sdb_rwlock_read_guard guard(sdb_password_lock);
  return sdb_passwd_encryption.decrypt(sdb_encoded_password, res);
}

uint sdb_selector_pushdown_threshold(THD *thd) {
  return THDVAR(thd, selector_pushdown_threshold);
}
