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
#ifndef SERVER_HA_THD_COMMON__H
#define SERVER_HA_THD_COMMON__H

#include "sql_class.h"

THD *ha_create_thd();

void ha_kill_mysqld(THD *thd);

bool ha_wait_for_mysqld_service();

void ha_watch_kill_signal(THD *thd, mysql_cond_t *cond, mysql_mutex_t *mutex);

void ha_audit_disconnect(THD *thd);

void ha_thread_end(THD *thd);

extern PSI_stage_info stage_sleeping;

#endif
