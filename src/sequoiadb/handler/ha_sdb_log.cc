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
#ifndef MYSQL_SERVER
#define MYSQL_SERVER
#endif

#include <my_global.h>
#include <log.h>
#include "ha_sdb_log.h"

void sdb_log(loglevel lvl, const char *format, ...) {
  va_list args;
  char format2[SDB_LOG_BUF_SIZE];

  va_start(args, format);
  snprintf(format2, SDB_LOG_BUF_SIZE - 1, "SequoiaDB: %s", format);
  error_log_print(lvl, format2, args);
  va_end(args);
}
