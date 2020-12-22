#ifndef MYSQL_SERVER
#define MYSQL_SERVER
#endif

#include <log.h>
#include "ha_spark_log.h"

void spark_log(loglevel lvl, const char *format, ...) {
  va_list args;
  char format2[SPARK_LOG_BUF_SIZE];

  va_start(args, format);
  snprintf(format2, SPARK_LOG_BUF_SIZE - 1, "Spark: %s", format);
  error_log_print(lvl, format2, args);
  va_end(args);
}

