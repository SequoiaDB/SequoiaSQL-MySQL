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
#ifndef SERVER_HA_DIRECT_QUERY__H
#define SERVER_HA_DIRECT_QUERY__H

// set THD context before run SQL query
int set_thd_context(THD *thd);

// set context and call mysql_parse
int server_ha_query(THD *thd, const char *query, size_t q_len);
#endif
