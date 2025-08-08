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
#ifndef SDB_CTX__H
#define SDB_CTX__H

#ifdef IS_MYSQL
#include <table.h>
#include <partition_info.h>
#include <partitioning/partition_handler.h>
#include <list>
#elif IS_MARIADB
#include <my_global.h>
#include <ha_partition.h>
#endif

#include "ha_sdb_def.h"

#ifdef IS_MARIADB
typedef struct Sdb_partition_del_ren_ctx {
  char skip_delete_cl[SDB_CL_NAME_MAX_SIZE + 1];
  char skip_rename_main_cl[SDB_CL_NAME_MAX_SIZE + 1];
  char skip_rename_sub_cl[SDB_CL_NAME_MAX_SIZE + 1];
  longlong query_id;

  Sdb_partition_del_ren_ctx() {
    skip_delete_cl[0] = '\0';
    skip_rename_main_cl[0] = '\0';
    skip_rename_sub_cl[0] = '\0';
    query_id = 0;
  }
} Sdb_part_del_ren_ctx;
#endif

class Sdb_part_alter_ctx {
 public:
  Sdb_part_alter_ctx() {}

  ~Sdb_part_alter_ctx();

  int init(partition_info* part_info);

  bool skip_delete_table(const char* table_name);

  bool skip_rename_table(const char* new_table_name);

  bool empty();

 private:
  int push_table_name2list(std::list<char*>& list, const char* org_table_name,
                           const char* part_name,
                           const char* sub_part_name = NULL);

  std::list<char*> m_skip_list4delete;
  std::list<char*> m_skip_list4rename;
};

#endif  // SDB_CTX__H
