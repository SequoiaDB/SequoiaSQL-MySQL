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

#include <my_global.h>
#include "server_ha_thd_common.h"
#include <sql_class.h>
#include "server_ha.h"
#include "server_ha_util.h"
#include "ha_sdb_sql.h"
#include "server_ha_query.h"
#include "sql_audit.h"
#include "debug_sync.h"

THD *ha_create_thd() {
  THD *thd = NULL;
  try {
#ifdef IS_MYSQL
    my_thread_init();
    thd = new THD;
    DBUG_ASSERT(NULL != thd);
    thd->set_command(COM_DAEMON);
    thd->system_thread = SYSTEM_THREAD_BACKGROUND;
    thd->security_context()->set_host_or_ip_ptr(C_STRING_WITH_LEN(""));
    NET *net = (NET *)thd->get_net();
    if (NULL != net) {
      net->reading_or_writing = 0;
    }
#else
    my_thread_init();
    thd = new THD(next_thread_id());
    DBUG_ASSERT(NULL != thd);
    thd->set_command(COM_DAEMON);
    thd->system_thread = SYSTEM_THREAD_GENERIC;
    thd->security_ctx->host_or_ip = "";
#endif
  } catch (std::bad_alloc &e) {
    sql_print_error("HA: Out of memory in 'HA' thread");
    goto error;
  } catch (std::exception &e) {
    sql_print_error("HA: Unexpected error: %s", e.what());
    goto error;
  }
  // set thd context
  thd->thread_stack = (char *)&thd;
  if (0 != set_thd_context(thd)) {
    delete thd;
    thd = NULL;
    my_error(ER_OUT_OF_RESOURCES, MYF(0));
    goto error;
  }
done:
  return thd;
error:
  goto done;
}

// call kill_mysql notifies main thread to abort current instance
void ha_kill_mysqld(THD *thd) {
#ifndef NO_EMBEDDED_ACCESS_CHECKS
  if (ha_is_aborting()) {
    return;
  }
#ifdef IS_MARIADB
  if (thd) {
    kill_mysql(thd);
  } else {
    exit(0);
  }
#else
  extern my_thread_handle signal_thread_id;
  if (0 != signal_thread_id.thread) {
    kill_mysql();
  }
#endif
#endif
}

// before create instance group user, mysql service must been started
bool ha_wait_for_mysqld_service() {
  bool mysqld_failed = false;
  mysql_mutex_lock(&LOCK_server_started);
  // end 'HA' thread as soon as possible if found mysqld failed to start
  while (!mysqld_server_started && !mysqld_failed) {
    timespec abstime;
    sdb_set_timespec(abstime, 1);
    mysql_cond_timedwait(&COND_server_started, &LOCK_server_started, &abstime);
    mysqld_failed |= abort_loop;
    mysqld_failed |= ha_is_aborting();
  }
  mysql_mutex_unlock(&LOCK_server_started);
  return mysqld_failed;
}

// set current mutex and cond for current thd, the main thread will send
// stop signal after getting 'shutdown' command
void ha_watch_kill_signal(THD *thd, mysql_cond_t *cond, mysql_mutex_t *mutex) {
#ifdef IS_MYSQL
  // refer to "close_connections and Set_kill_conn()" function
  thd->current_mutex = mutex;
  thd->current_cond = cond;
#else
  // refer to "kill_thread_phase_1 and kill_thread" function
  thd->mysys_var->current_mutex = mutex;
  thd->mysys_var->current_cond = cond;
#endif
}

void ha_audit_disconnect(THD *thd) {
#ifdef IS_MARIADB
  mysql_audit_notify_connection_disconnect(thd, 0);
#else
#ifndef EMBEDDED_LIBRARY
  mysql_audit_notify(thd, AUDIT_EVENT(MYSQL_AUDIT_CONNECTION_DISCONNECT), 0);
#endif
#endif
}

// necessary work for ending HA thread
void ha_thread_end(THD *thd) {
#ifdef IS_MYSQL
  if (thd) {
    Global_THD_manager *thd_manager = Global_THD_manager::get_instance();
    thd->release_resources();
    thd_manager->remove_thd(thd);
    delete thd;
  }
  my_thread_end();
  my_thread_exit(0);
#else
  if (thd) {
    thd->add_status_to_global();
    server_threads.erase(thd);
    delete thd;
  }
  my_thread_end();
#endif
}

PSI_stage_info stage_sleeping = {0, "Sleeping", 0};
