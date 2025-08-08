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
#ifndef SDB_UTIL__H
#define SDB_UTIL__H

#include <sql_class.h>
#include <thr_mutex.h>

int sdb_parse_table_name(const char *from, char *db_name, int db_name_max_size,
                         char *table_name, int table_name_max_size);

int sdb_get_db_name_from_path(const char *path, char *db_name,
                              int db_name_max_size);

int sdb_convert_charset(const String &src_str, String &dst_str,
                        const CHARSET_INFO *dst_charset);

class Sdb_mutex_guard {
  native_mutex_t &m_mutex;

 public:
  Sdb_mutex_guard(native_mutex_t &mutex) : m_mutex(mutex) {
    native_mutex_lock(&m_mutex);
  }

  ~Sdb_mutex_guard() { native_mutex_unlock(&m_mutex); }
};

#endif
