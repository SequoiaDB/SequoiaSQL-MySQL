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

   Source File Name = mapping_context_impl.h

   Descriptive Name = N/A

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          08/08/2025  SequoiaDB  Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef MAPPING_CONTEXT_IMPL__H
#define MAPPING_CONTEXT_IMPL__H
#include "ha_sdb_def.h"
#include "mapping_context.h"
#include "ha_sdb_share.h"

class Mapping_context_impl : public Mapping_context {
 private:
  boost::shared_ptr<Sdb_share> *m_share;
  enum_mapping_state m_state;
  bool m_is_part_table;
  bool m_is_specified_mapping;
  char m_cs_name[SDB_CS_NAME_MAX_SIZE + 1];
  char m_cl_name[SDB_CL_NAME_MAX_SIZE + 1];
  char m_db[SDB_CS_NAME_MAX_SIZE + 1];
  char m_table[SDB_CL_NAME_MAX_SIZE + 1];

 public:
  Mapping_context_impl()
      : m_share(NULL),
        m_state(NM_STATE_NONE),
        m_is_part_table(false),
        m_is_specified_mapping(false) {
    m_cs_name[0] = '\0';
    m_cl_name[0] = '\0';
    m_db[0] = '\0';
    m_table[0] = '\0';
  }
  Mapping_context_impl(bool is_part_table)
      : m_share(NULL),
        m_state(NM_STATE_NONE),
        m_is_part_table(is_part_table),
        m_is_specified_mapping(false) {
    m_cs_name[0] = '\0';
    m_cl_name[0] = '\0';
    m_db[0] = '\0';
    m_table[0] = '\0';
  }
  void set_share_cache(boost::shared_ptr<Sdb_share> *share) { m_share = share; }

  const char *get_mapping_cs();
  void set_mapping_cs(const char *cs_name);
  const char *get_mapping_cl();
  void set_mapping_cl(const char *cl_name);
  enum_mapping_state get_mapping_state() { return m_state; }
  void set_mapping_state(enum_mapping_state state) { m_state = state; }
  bool is_part_table() { return m_is_part_table; }
  void set_part_table(bool is_part_table) { m_is_part_table = is_part_table; }
  bool is_specified_mapping() { return m_is_specified_mapping; }
  void set_specified_mapping_flag(bool specified) {
    m_is_specified_mapping = specified;
  }
  const char *get_db() { return m_db; }
  const char *get_table() { return m_table; }
  void set_db(const char *db_name) { strcpy(m_db, db_name); }
  void set_table(const char *table_name) { strcpy(m_table, table_name); }

  void reset() {
    m_share = NULL;
    m_state = NM_STATE_NONE;
    m_is_part_table = false;
    m_is_specified_mapping = false;
    m_cs_name[0] = '\0';
    m_cl_name[0] = '\0';
    m_db[0] = '\0';
    m_table[0] = '\0';
  }
};
#endif
