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
#include "sdb_util.h"
#include <sql_table.h>
#include <string.h>
#include "sdb_log.h"
#include "sdb_errcode.h"
#include "sdb_def.h"

int sdb_parse_table_name(const char *from, char *db_name, int db_name_max_size,
                         char *table_name, int table_name_max_size) {
  int rc = 0;
  int name_len = 0;
  char *end = NULL;
  char *ptr = NULL;
  char *tmp_name = NULL;
  char tmp_buff[SDB_CL_NAME_MAX_SIZE + SDB_CS_NAME_MAX_SIZE + 1];

  tmp_name = tmp_buff;

  // scan table_name from the end
  end = strend(from) - 1;
  ptr = end;
  while (ptr >= from && *ptr != '\\' && *ptr != '/') {
    ptr--;
  }
  name_len = (int)(end - ptr);
  if (name_len > table_name_max_size) {
    rc = ER_TOO_LONG_IDENT;
    goto error;
  }
  memcpy(tmp_name, ptr + 1, end - ptr);
  tmp_name[name_len] = '\0';
  filename_to_tablename(tmp_name, table_name, sizeof(tmp_buff) - 1);

  // scan db_name
  ptr--;
  end = ptr;
  while (ptr >= from && *ptr != '\\' && *ptr != '/') {
    ptr--;
  }
  name_len = (int)(end - ptr);
  if (name_len > db_name_max_size) {
    rc = ER_TOO_LONG_IDENT;
    goto error;
  }
  memcpy(tmp_name, ptr + 1, end - ptr);
  tmp_name[name_len] = '\0';
  filename_to_tablename(tmp_name, db_name, sizeof(tmp_buff) - 1);

done:
  return rc;
error:
  goto done;
}

int sdb_get_db_name_from_path(const char *path, char *db_name,
                              int db_name_max_size) {
  int rc = 0;
  int name_len = 0;
  char *end = NULL;
  char *ptr = NULL;
  char *tmp_name = NULL;
  char tmp_buff[SDB_CS_NAME_MAX_SIZE + 1];

  tmp_name = tmp_buff;

  // scan from the end
  end = strend(path) - 1;
  ptr = end;
  while (ptr >= path && *ptr != '\\' && *ptr != '/') {
    ptr--;
  }
  ptr--;
  end = ptr;
  while (ptr >= path && *ptr != '\\' && *ptr != '/') {
    ptr--;
  }
  name_len = (int)(end - ptr);
  if (name_len > db_name_max_size) {
    rc = ER_TOO_LONG_IDENT;
    goto error;
  }
  memcpy(tmp_name, ptr + 1, end - ptr);
  tmp_name[name_len] = '\0';
  filename_to_tablename(tmp_name, db_name, sizeof(tmp_buff) - 1);

done:
  return rc;
error:
  goto done;
}

int sdb_convert_charset(const String &src_str, String &dst_str,
                        const CHARSET_INFO *dst_charset) {
  int rc = SDB_ERR_OK;
  uint conv_errors = 0;
  if (dst_str.copy(src_str.ptr(), src_str.length(), src_str.charset(),
                   dst_charset, &conv_errors)) {
    rc = SDB_ERR_OOM;
    goto error;
  }
  if (conv_errors) {
    SDB_LOG_DEBUG("String[%s] cannot be converted from %s to %s.",
                  src_str.ptr(), src_str.charset()->csname,
                  dst_charset->csname);
    rc = HA_ERR_UNKNOWN_CHARSET;
    goto error;
  }
done:
  return rc;
error:
  goto done;
}
