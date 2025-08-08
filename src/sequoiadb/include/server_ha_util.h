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
#ifndef SERVER_HA_UTIL__H
#define SERVER_HA_UTIL__H
#include <sys/types.h>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#ifndef MYSQL_SERVER
#define MYSQL_SERVER
#endif

#include "sdb_conn.h"
#include "sdb_cl.h"
#include "server_ha_def.h"

#ifdef IS_MYSQL
extern void sql_print_error(const char *format, ...);
extern void sql_print_information(const char *format, ...);
extern void sql_print_warning(const char *format, ...);
void kill_mysql(void);
#else
extern void destroy_thd(MYSQL_THD thd);
extern MYSQL_THD create_thd();
#endif

#define HA_RC_CHECK(rc, label, fmt, ...)   \
  do {                                     \
    if ((rc)) {                            \
      sql_print_error(fmt, ##__VA_ARGS__); \
      goto label;                          \
    }                                      \
  } while (0)

const char *ha_error_string(Sdb_conn &sdb_conn, int rc, char err_buf[]);

// quote database or table name with ``, use '`' as escape character
char *ha_quote_name(const char *name, char *buff);

// get handler of HA tables
int ha_get_instance_object_state_cl(Sdb_conn &sdb_conn, const char *group_name,
                                    Sdb_cl &cl, const char *data_group = NULL);
int ha_get_instance_state_cl(Sdb_conn &sdb_conn, const char *group_name,
                             Sdb_cl &cl, const char *data_group = NULL);
int ha_get_object_state_cl(Sdb_conn &sdb_conn, const char *group_name,
                           Sdb_cl &cl, const char *data_group = NULL);
int ha_get_lock_cl(Sdb_conn &sdb_conn, const char *group_name, Sdb_cl &cl,
                   const char *data_group = NULL);
int ha_get_registry_cl(Sdb_conn &sdb_conn, const char *group_name, Sdb_cl &cl,
                       const char *data_group = NULL);
int ha_get_pending_log_cl(Sdb_conn &sdb_conn, const char *group_name,
                          Sdb_cl &pending_log_cl,
                          const char *data_group = NULL);
int ha_get_pending_object_cl(Sdb_conn &sdb_conn, const char *group_name,
                             Sdb_cl &pending_object_cl,
                             const char *data_group = NULL);
#endif
