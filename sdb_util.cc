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

#include "sdb_util.h"
#include <sql_table.h>
#include "sdb_log.h"
#include "sdb_errcode.h"
#include "sdb_def.h"
#include <my_rnd.h>

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

Sdb_encryption::Sdb_encryption() {
  my_rand_buffer(m_key, KEY_LEN);
}

int Sdb_encryption::encrypt(const String &src, String &dst) {
  int rc = SDB_ERR_OK;
  int real_enc_len = 0;
  int dst_len = my_aes_get_size(src.length(), AES_OPMODE);

  if (dst.alloc(dst_len)) {
    rc = SDB_ERR_OOM;
    goto error;
  }

  dst.set_charset(&my_charset_bin);
  real_enc_len = my_aes_encrypt((unsigned char *)src.ptr(), src.length() + 1,
                                (unsigned char *)dst.c_ptr(), m_key, KEY_LEN,
                                AES_OPMODE, NULL);
  dst.length(real_enc_len);

  if (real_enc_len != dst_len) {
    // Bad parameters.
    rc = SDB_ERR_INVALID_ARG;
    goto error;
  }

done:
  return rc;
error:
  goto done;
}

int Sdb_encryption::decrypt(const String &src, String &dst) {
  int rc = SDB_ERR_OK;
  int real_dec_len = 0;

  if (dst.alloc(src.length())) {
    rc = SDB_ERR_OOM;
    goto error;
  }

  dst.set_charset(&my_charset_bin);
  real_dec_len = my_aes_decrypt((unsigned char *)src.ptr(), src.length(),
                                (unsigned char *)dst.c_ptr(), m_key, KEY_LEN,
                                AES_OPMODE, NULL);
  dst.length(real_dec_len);

  if (real_dec_len < 0) {
    // Bad parameters.
    rc = SDB_ERR_INVALID_ARG;
    goto error;
  }

done:
  return rc;
error:
  goto done;
}
