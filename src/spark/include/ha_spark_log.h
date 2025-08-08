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
#ifndef SPARK_LOG__H
#define SPARK_LOG__H

#include <log.h>
#include "ha_spark_vars.h"

#define SPARK_LOG_BUF_SIZE 1024

#define SPARK_LOG_DEBUG(format, ...)                       \
  do {                                                     \
    if (spark_debug_log) {                                 \
      spark_log(INFORMATION_LEVEL, format, ##__VA_ARGS__); \
    }                                                      \
  } while (0)

#define SPARK_LOG_INFO(format, ...)                      \
  do {                                                   \
    spark_log(INFORMATION_LEVEL, format, ##__VA_ARGS__); \
  } while (0)

#define SPARK_LOG_WARNING(format, ...)               \
  do {                                               \
    spark_log(WARNING_LEVEL, format, ##__VA_ARGS__); \
  } while (0)

#define SPARK_LOG_ERROR(format, ...)               \
  do {                                             \
    spark_log(ERROR_LEVEL, format, ##__VA_ARGS__); \
  } while (0)

#define SPARK_PRINT_ERROR(code, format, ...)              \
  do {                                                    \
    my_printf_error(code, format, MYF(0), ##__VA_ARGS__); \
    SPARK_LOG_ERROR(format, ##__VA_ARGS__);               \
  } while (0)

void spark_log(loglevel lvl, const char *format, ...);

#endif
