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
#ifndef SDB_LOG__H
#define SDB_LOG__H

#include <log.h>
#include "ha_sdb_conf.h"

#define SDB_LOG_BUF_SIZE 1024

#define SDB_LOG_DEBUG(format, ...)                       \
  do {                                                   \
    if (sdb_debug_log(current_thd)) {                    \
      sdb_log(INFORMATION_LEVEL, format, ##__VA_ARGS__); \
    }                                                    \
  } while (0)

#define SDB_LOG_INFO(format, ...)                      \
  do {                                                 \
    sdb_log(INFORMATION_LEVEL, format, ##__VA_ARGS__); \
  } while (0)

#define SDB_LOG_WARNING(format, ...)               \
  do {                                             \
    sdb_log(WARNING_LEVEL, format, ##__VA_ARGS__); \
  } while (0)

#define SDB_LOG_ERROR(format, ...)               \
  do {                                           \
    sdb_log(ERROR_LEVEL, format, ##__VA_ARGS__); \
  } while (0)

#define SDB_PRINT_ERROR(code, format, ...)                \
  do {                                                    \
    my_printf_error(code, format, MYF(0), ##__VA_ARGS__); \
    SDB_LOG_ERROR(format, ##__VA_ARGS__);                 \
  } while (0)

void sdb_log(loglevel lvl, const char *format, ...);

#define SDB_EXCEPTION_CATCHER(ret, format, ...) \
  catch (std::bad_alloc & e) {                  \
    ret = SDB_ERR_OOM;                          \
    SDB_LOG_ERROR(format, ##__VA_ARGS__);       \
    convert_sdb_code(ret);                      \
    goto error;                                 \
  }                                             \
  catch (std::exception & e) {                  \
    SDB_LOG_ERROR(format, ##__VA_ARGS__);       \
    ret = SDB_ERR_BUILD_BSON;                   \
    convert_sdb_code(ret);                      \
    goto error;                                 \
  }

#endif
