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
#ifndef _SPARK_VARS__H
#define _SPARK_VARS__H

#include <my_global.h>
#include <mysql/plugin.h>
#include <sql_string.h>

extern ulong srv_enum_var;
extern ulong srv_ulong_var;
extern double srv_double_var;
extern char *spk_dsn_str;

extern struct st_mysql_sys_var *spark_system_variables[];
extern struct st_mysql_show_var func_status[];

extern my_bool spark_debug_log;
extern char *spk_odb_ini_path;

bool spk_execute_only_in_mysql(THD *thd);

#endif
