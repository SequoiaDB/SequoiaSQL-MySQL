/* Copyright (c) 2018-2019, SequoiaDB and/or its affiliates. All rights
  reserved.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef SDB_UTIL__H
#define SDB_UTIL__H

#include <sql_class.h>
#include <my_aes.h>
#include <client.hpp>
#include "sdb_errcode.h"

#define SDB_MIN(x, y) (((x) < (y)) ? (x) : (y))

int sdb_parse_table_name(const char *from, char *db_name, int db_name_max_size,
                         char *table_name, int table_name_max_size);

int sdb_get_db_name_from_path(const char *path, char *db_name,
                              int db_name_max_size);

int sdb_rebuild_db_name_of_temp_table(char *db_name, int db_name_max_size);

bool sdb_is_tmp_table(const char *path, const char *table_name);

int sdb_convert_charset(const String &src_str, String &dst_str,
                        const CHARSET_INFO *dst_charset);

bool sdb_field_is_floating(enum_field_types type);

bool sdb_field_is_date_time(enum_field_types type);

class Sdb_encryption {
  static const uint KEY_LEN = 32;
  static const enum my_aes_opmode AES_OPMODE = my_aes_128_ecb;

  unsigned char m_key[KEY_LEN];

 public:
  Sdb_encryption();
  int encrypt(const String &src, String &dst);
  int decrypt(const String &src, String &dst);
};

template <class T>
class Sdb_obj_cache {
 public:
  Sdb_obj_cache();
  ~Sdb_obj_cache();

  int ensure(uint size);
  void release();

  inline const T &operator[](int i) const {
    DBUG_ASSERT(i >= 0 && i < (int)m_cache_size);
    return m_cache[i];
  }

  inline T &operator[](int i) {
    DBUG_ASSERT(i >= 0 && i < (int)m_cache_size);
    return m_cache[i];
  }

 private:
  T *m_cache;
  uint m_cache_size;
};

template <class T>
Sdb_obj_cache<T>::Sdb_obj_cache() {
  m_cache = NULL;
  m_cache_size = 0;
}

template <class T>
Sdb_obj_cache<T>::~Sdb_obj_cache() {
  release();
}

template <class T>
int Sdb_obj_cache<T>::ensure(uint size) {
  DBUG_ASSERT(size > 0);

  if (size <= m_cache_size) {
    // reset all objects to be used
    for (uint i = 0; i < size; i++) {
      m_cache[i] = T();
    }
    return SDB_ERR_OK;
  }

  release();

  m_cache = new (std::nothrow) T[size];
  if (NULL == m_cache) {
    return HA_ERR_OUT_OF_MEM;
  }
  m_cache_size = size;

  return SDB_ERR_OK;
}

template <class T>
void Sdb_obj_cache<T>::release() {
  if (NULL != m_cache) {
    delete[] m_cache;
    m_cache = NULL;
    m_cache_size = 0;
  }
}

#endif
