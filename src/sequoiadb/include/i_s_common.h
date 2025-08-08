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
#ifndef INFO_SCHEMA__H
#define INFO_SCHEMA__H

#include <my_global.h>
#include <sql_class.h>
#include <table.h>
#include "ha_sdb_thd.h"

#define I_S_END_FIELD_INFO \
  { NULL, 0, MYSQL_TYPE_NULL, 0, 0, "", SKIP_OPEN_TABLE }

const char plugin_author[] = "SequoiaDB";
extern struct st_mysql_information_schema i_s_info;

struct st_i_s_name_id_pair {
  const char *name;
  uint name_len;
  uint id;
};
typedef struct st_i_s_name_id_pair i_s_name_id_pair;

uchar *i_s_pair_get_key(i_s_name_id_pair *pair, size_t *length,
                        my_bool not_used MY_ATTRIBUTE((unused)));

extern struct st_sdb_plugin i_s_sdb_session_attr;

#endif
