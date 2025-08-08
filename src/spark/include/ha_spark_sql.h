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
#ifndef SPARK_SQL__H
#define SPARK_SQL__H

#if (defined(IS_MYSQL) and defined(IS_MARIADB)) or \
    (not defined(IS_MYSQL) and not defined(IS_MARIADB))
#error Project type(MySQL/MariaDB) was not declared.
#endif

#include <mysql_version.h>
#include <my_global.h>
#include <sql_class.h>
#include <sql_table.h>
#include <sql_insert.h>
#include <mysql/psi/psi_memory.h>
#include <sql_lex.h>

#if defined IS_MYSQL
#include <my_aes.h>
#include <item_cmpfunc.h>
#include <sql_optimizer.h>
#elif defined IS_MARIADB
#include <mysql/service_my_crypt.h>
#include <sql_select.h>
#endif

#ifndef MY_ATTRIBUTE
#if defined(__GNUC__)
#define MY_ATTRIBUTE(A) __attribute__(A)
#else
#define MY_ATTRIBUTE(A)
#endif
#endif

const char *spk_field_name(const Field *f);
ulong spk_thd_current_row(THD *thd);
const char *spk_thd_query(THD *thd);
bool spk_datetime_to_timeval(THD *thd, const MYSQL_TIME *ltime,
                             struct timeval *tm, int *error_code);

#endif
