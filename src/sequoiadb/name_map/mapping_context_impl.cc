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

   Source File Name = mapping_context_impl.cc

   Descriptive Name = N/A

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          18/08/2025  SequoiaDB  Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef MYSQL_SERVER
#define MYSQL_SERVER
#endif

#include "ha_sdb_sql.h"
#include "mapping_context_impl.h"
#include "ha_sdb.h"
#include "ha_sdb_log.h"

const char *Mapping_context_impl::get_mapping_cs() {
  if (m_share && '\0' != m_share->mapping_cs[0] &&
      '\0' != m_share->mapping_cl[0] && '\0' == m_cs_name[0]) {
    strncpy(m_cs_name, m_share->mapping_cs, SDB_CS_NAME_MAX_SIZE);
  }
  return m_cs_name;
}

void Mapping_context_impl::set_mapping_cs(const char *cs_name) {
  strncpy(m_cs_name, cs_name, SDB_CS_NAME_MAX_SIZE);
  if (NULL != m_share) {
    strncpy(m_share->mapping_cs, m_cs_name, SDB_CS_NAME_MAX_SIZE);
  }
}

const char *Mapping_context_impl::get_mapping_cl() {
  if (m_share && '\0' != m_share->mapping_cs[0] &&
      '\0' != m_share->mapping_cl[0] && '\0' == m_cl_name[0]) {
    strncpy(m_cl_name, m_share->mapping_cl, SDB_CL_NAME_MAX_SIZE);
  }
  return m_cl_name;
}

void Mapping_context_impl::set_mapping_cl(const char *cl_name) {
  strncpy(m_cl_name, cl_name, SDB_CL_NAME_MAX_SIZE);
  if (NULL != m_share) {
    strncpy(m_share->mapping_cl, m_cl_name, SDB_CL_NAME_MAX_SIZE);
  }
}
