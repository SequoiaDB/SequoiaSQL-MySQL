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
#include "sdb_conf.h"

static const char *SDB_ADDR_DFT = "localhost:11810";
static const my_bool SDB_USE_PARTITION_DFT = TRUE;
static const my_bool SDB_DEBUG_LOG_DFT = FALSE;
static const my_bool SDB_DEFAULT_USE_BULK_INSERT = FALSE;
static const int SDB_DEFAULT_BULK_INSERT_SIZE = 100;

char *sdb_conn_str = NULL;
my_bool sdb_use_partition = SDB_USE_PARTITION_DFT;
my_bool sdb_use_bulk_insert = SDB_DEFAULT_USE_BULK_INSERT;
int sdb_bulk_insert_size = SDB_DEFAULT_BULK_INSERT_SIZE;
my_bool sdb_debug_log = SDB_DEBUG_LOG_DFT;

static void sdb_use_partition_update(THD *thd, struct st_mysql_sys_var *var,
                                     void *var_ptr, const void *save) {
  *static_cast<my_bool *>(var_ptr) = *static_cast<const my_bool *>(save);
}

static void sdb_use_bulk_insert_update(THD *thd, struct st_mysql_sys_var *var,
                                       void *var_ptr, const void *save) {
  *static_cast<my_bool *>(var_ptr) = *static_cast<const my_bool *>(save);
}

static void sdb_bulk_insert_size_update(THD *thd, struct st_mysql_sys_var *var,
                                        void *var_ptr, const void *save) {
  *static_cast<int *>(var_ptr) = *static_cast<const int *>(save);
}

static void sdb_debug_log_update(THD *thd, struct st_mysql_sys_var *var,
                                 void *var_ptr, const void *save) {
  *static_cast<my_bool *>(var_ptr) = *static_cast<const my_bool *>(save);
}

static MYSQL_SYSVAR_STR(conn_addr, sdb_conn_str,
                        PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_READONLY,
                        "Sequoiadb addr", NULL, NULL, SDB_ADDR_DFT);
static MYSQL_SYSVAR_BOOL(use_partition, sdb_use_partition, PLUGIN_VAR_OPCMDARG,
                         "create partition table on sequoiadb", NULL,
                         sdb_use_partition_update, SDB_USE_PARTITION_DFT);
static MYSQL_SYSVAR_BOOL(use_bulk_insert, sdb_use_bulk_insert,
                         PLUGIN_VAR_OPCMDARG, "enable bulk insert to sequoiadb",
                         NULL, sdb_use_bulk_insert_update,
                         SDB_DEFAULT_USE_BULK_INSERT);
static MYSQL_SYSVAR_INT(bulk_insert_size, sdb_bulk_insert_size,
                        PLUGIN_VAR_OPCMDARG, "bulk insert size", NULL,
                        sdb_bulk_insert_size_update,
                        SDB_DEFAULT_BULK_INSERT_SIZE, 1, 100000, 0);
static MYSQL_SYSVAR_BOOL(debug_log, sdb_debug_log, PLUGIN_VAR_OPCMDARG,
                         "turn on debug log of sequoiadb storage engine", NULL,
                         sdb_debug_log_update, SDB_DEBUG_LOG_DFT);

struct st_mysql_sys_var *sdb_sys_vars[] = {
    MYSQL_SYSVAR(conn_addr),       MYSQL_SYSVAR(use_partition),
    MYSQL_SYSVAR(use_bulk_insert), MYSQL_SYSVAR(bulk_insert_size),
    MYSQL_SYSVAR(debug_log),       NULL};

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