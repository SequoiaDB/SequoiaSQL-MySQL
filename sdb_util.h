/* Copyright (c) 2018, SequoiaDB and/or its affiliates. All rights reserved.

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
#include <thr_mutex.h>
#include <my_aes.h>

#define SDB_MIN(x, y) (((x) < (y)) ? (x) : (y))

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

class Sdb_encryption {
  static const uint KEY_LEN = 32;
  static const enum my_aes_opmode AES_OPMODE = my_aes_128_ecb;

  unsigned char m_key[KEY_LEN];

 public:
  Sdb_encryption();
  int encrypt(const String &src, String &dst);
  int decrypt(const String &src, String &dst);
};

extern Sdb_encryption sdb_encryption;

#endif
