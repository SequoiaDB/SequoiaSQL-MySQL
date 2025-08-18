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

#include "sdb_idx.h"
#include "sdb_cl.h"
#include "sdb_errcode.h"
#include "sdb_def.h"
#include "sdb_log.h"
#include "sdb_util.h"
#include "sql_table.h"
#include "bson/bsonDecimal.h"

static bson::BSONObj empty_obj;

#define mi_sint2korr(A)                   \
  ((int16)(((int16)(((uchar *)(A))[1])) + \
           ((int16)((int16)((char *)(A))[0]) << 8)))
#define mi_sint3korr(A)                                                        \
  ((int32)(((((uchar *)(A))[0]) & 128)                                         \
               ? (((uint32)255L << 24) | (((uint32)((uchar *)(A))[0]) << 16) | \
                  (((uint32)((uchar *)(A))[1]) << 8) |                         \
                  ((uint32)((uchar *)(A))[2]))                                 \
               : (((uint32)((uchar *)(A))[0]) << 16) |                         \
                     (((uint32)((uchar *)(A))[1]) << 8) |                      \
                     ((uint32)((uchar *)(A))[2])))

#define mi_uint5korr(A)                                 \
  ((ulonglong)(((uint32)(((uchar *)(A))[4])) +          \
               (((uint32)(((uchar *)(A))[3])) << 8) +   \
               (((uint32)(((uchar *)(A))[2])) << 16) +  \
               (((uint32)(((uchar *)(A))[1])) << 24)) + \
   (((ulonglong)(((uchar *)(A))[0])) << 32))

#define DATETIMEF_INT_OFS 0x8000000000LL

enum sdb_search_match_mode {
  SDB_ET = 0,
  SDB_GT = 1,
  SDB_GTE = 2,
  SDB_LT = 3,
  SDB_LTE = 4,
  SDB_UNSUPP = -1,
};

enum sdb_read_null_mode {
  SDB_READ_ALL_RECORDS = 0,
  SDB_READ_ONLY_NULL_RECORDS = 1,
  SDB_READ_NON_NULL_RECORDS = 2,
};

static inline int get_variable_key_length(const uchar *A) {
  return (int)(((uint16)(A[0])) + ((uint16)(A[1]) << 8));
}

int get_key_direction(sdb_search_match_mode mode) {
  switch (mode) {
    case SDB_ET:
    case SDB_GT:
    case SDB_GTE:
      return 1;
    case SDB_LT:
    case SDB_LTE:
      return -1;
    default:
      return 1;
  }
  return 1;
}

sdb_search_match_mode conver_search_mode_to_sdb_mode(
    ha_rkey_function find_flag) {
  switch (find_flag) {
    case HA_READ_KEY_EXACT:
      return SDB_ET;
    case HA_READ_KEY_OR_NEXT:
      return SDB_GTE;
    case HA_READ_AFTER_KEY:
      return SDB_GT;
    case HA_READ_BEFORE_KEY:
      return SDB_LT;
    case HA_READ_KEY_OR_PREV:
    case HA_READ_PREFIX_LAST:
    case HA_READ_PREFIX_LAST_OR_PREV:
      return SDB_LTE;
    case HA_READ_PREFIX:
    default:
      return SDB_UNSUPP;
  }
  return SDB_UNSUPP;
}

BOOLEAN is_field_indexable(const Field *field) {
  switch (field->type()) {
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_FLOAT:
    case MYSQL_TYPE_DOUBLE:
    case MYSQL_TYPE_LONGLONG:
    case MYSQL_TYPE_DATETIME:
      return TRUE;
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_BLOB: {
      if (!field->binary())
        return TRUE;
      else
        return FALSE;
    }
    default:
      return FALSE;
  }
}

int sdb_create_index(const KEY *keyInfo, Sdb_cl &cl) {
  const KEY_PART_INFO *keyPart;
  const KEY_PART_INFO *keyEnd;
  int rc = 0;
  bson::BSONObj keyObj;
  BOOLEAN isUnique = FALSE, isEnforced = FALSE;

  bson::BSONObjBuilder keyObjBuilder;
  keyPart = keyInfo->key_part;
  keyEnd = keyPart + keyInfo->user_defined_key_parts;
  for (; keyPart != keyEnd; ++keyPart) {
    if (!is_field_indexable(keyPart->field)) {
      rc = HA_ERR_UNSUPPORTED;
      SDB_PRINT_ERROR(rc,
                      "column '%-.192s' cannot be used in key specification.",
                      keyPart->field->field_name);
      goto error;
    }
    // TODO: ASC or DESC
    keyObjBuilder.append(keyPart->field->field_name, 1);
  }
  keyObj = keyObjBuilder.obj();

  if (!strcmp(keyInfo->name, primary_key_name)) {
    isUnique = TRUE;
    isEnforced = TRUE;
  }

  if (keyInfo->flags & HA_NOSAME) {
    isUnique = TRUE;
  }

  rc = cl.create_index(keyObj, keyInfo->name, isUnique, isEnforced);
  if (rc) {
    goto error;
  }
done:
  return rc;
error:
  goto done;
}

const char *sdb_get_idx_name(KEY *key_info) {
  if (key_info) {
    return key_info->name;
  }
  return NULL;
}

int sdb_get_idx_order(KEY *key_info, bson::BSONObj &order,
                      int order_direction) {
  int rc = SDB_ERR_OK;
  const KEY_PART_INFO *keyPart;
  const KEY_PART_INFO *keyEnd;
  bson::BSONObjBuilder obj_builder;
  if (!key_info) {
    rc = SDB_ERR_INVALID_ARG;
    goto error;
  }
  keyPart = key_info->key_part;
  keyEnd = keyPart + key_info->user_defined_key_parts;
  for (; keyPart != keyEnd; ++keyPart) {
    obj_builder.append(keyPart->field->field_name, order_direction);
  }
  order = obj_builder.obj();

done:
  return rc;
error:
  goto done;
}

typedef union _sdb_key_common_type {
  char sz_data[8];
  int8 int8_val;
  uint8 uint8_val;
  int16 int16_val;
  uint16 uint16_val;
  int32 int24_val;
  uint32 uint24_val;
  int32 int32_val;
  uint32 uint32_val;
  int64 int64_val;
  uint64 uint64_val;
} sdb_key_common_type;

void get_unsigned_key_val(const uchar *key_ptr, const KEY_PART_INFO *key_part,
                          bson::BSONObjBuilder &obj_builder,
                          const char *op_str) {
  sdb_key_common_type val_tmp;
  val_tmp.uint64_val = 0;
  if (NULL == key_ptr || key_part->length > sizeof(val_tmp)) {
    goto done;
  }

  memcpy(&(val_tmp.sz_data[0]),
         key_ptr + key_part->store_length - key_part->length, key_part->length);
  switch (key_part->length) {
    case 1: {
      obj_builder.append(op_str, val_tmp.uint8_val);
      break;
    }
    case 2: {
      obj_builder.append(op_str, val_tmp.uint16_val);
      break;
    }
    case 3:
    case 4: {
      if (val_tmp.int32_val >= 0) {
        obj_builder.append(op_str, val_tmp.int32_val);
      } else {
        obj_builder.append(op_str, val_tmp.int64_val);
      }
      break;
    }
    case 8: {
      if (val_tmp.int64_val >= 0) {
        obj_builder.append(op_str, val_tmp.int64_val);
      } else {
        bson::bsonDecimal decimal_tmp;
        char buf_tmp[24] = {0};
        sprintf(buf_tmp, "%llu", val_tmp.uint64_val);
        decimal_tmp.fromString(buf_tmp);
        obj_builder.append(op_str, decimal_tmp);
      }
      break;
    }
    default:
      break;
  }
done:
  return;
}

void get_signed_key_val(const uchar *key_ptr, const KEY_PART_INFO *key_part,
                        bson::BSONObjBuilder &obj_builder, const char *op_str) {
  sdb_key_common_type val_tmp;
  val_tmp.uint64_val = 0;
  if (NULL == key_ptr || key_part->length > sizeof(val_tmp)) {
    goto done;
  }

  memcpy(&(val_tmp.sz_data[0]),
         key_ptr + key_part->store_length - key_part->length, key_part->length);
  switch (key_part->length) {
    case 1: {
      obj_builder.append(op_str, val_tmp.int8_val);
      break;
    }
    case 2: {
      obj_builder.append(op_str, val_tmp.int16_val);
      break;
    }
    case 3: {
      if (val_tmp.int32_val & 0X800000) {
        val_tmp.sz_data[3] = 0XFF;
      }
      obj_builder.append(op_str, val_tmp.int32_val);
      break;
    }
    case 4: {
      obj_builder.append(op_str, val_tmp.int32_val);
      break;
    }
    case 8: {
      obj_builder.append(op_str, val_tmp.int64_val);
      break;
    }
    default:
      break;
  }

done:
  return;
}

void get_int_key_obj(const uchar *key_ptr, const KEY_PART_INFO *key_part,
                     bson::BSONObj &obj, const char *op_str) {
  bson::BSONObjBuilder obj_builder;
  if (!((Field_num *)(key_part->field))->unsigned_flag) {
    get_signed_key_val(key_ptr, key_part, obj_builder, op_str);
  } else {
    get_unsigned_key_val(key_ptr, key_part, obj_builder, op_str);
  }
  obj = obj_builder.obj();
}

void get_text_key_val(const uchar *key_ptr, bson::BSONObjBuilder &obj_builder,
                      const char *op_str, int length) {
  obj_builder.appendStrWithNoTerminating(op_str, (const char *)key_ptr, length);
}

void get_enum_key_val(const uchar *key_ptr, const KEY_PART_INFO *key_part,
                      String &str_val) {
  longlong org_val = key_part->field->val_int();

  // get the enum-string by field
  longlong new_val = 0;
  memcpy(&new_val, key_ptr + key_part->store_length - key_part->length,
         key_part->length);
  if (org_val != new_val) {
    key_part->field->store(new_val, false);
  }

  // enum charset must be field_charset(latin1), so this convertion is
  // necessary. we assert content is convertable, because error will be return
  // when insert.
  String org_str;
  key_part->field->val_str(&org_str);
  sdb_convert_charset(org_str, str_val, &SDB_CHARSET);

  // restore the original value
  if (org_val != new_val) {
    key_part->field->store(org_val, false);
  }
}

void get_enum_key_val(const uchar *key_ptr, const KEY_PART_INFO *key_part,
                      bson::BSONObjBuilder &obj_builder, const char *op_str) {
  char buf[SDB_IDX_FIELD_SIZE_MAX + 1] = {0};
  String str_val(buf, SDB_IDX_FIELD_SIZE_MAX, key_part->field->charset());
  get_enum_key_val(key_ptr, key_part, str_val);
  obj_builder.appendStrWithNoTerminating(op_str, (const char *)(str_val.ptr()),
                                         str_val.length());
}

int get_text_key_obj(const uchar *key_ptr, const KEY_PART_INFO *key_part,
                     bson::BSONObj &obj, const char *op_str) {
  int rc = SDB_ERR_OK;
  bson::BSONObjBuilder obj_builder;
  uchar key_field_str_buf[SDB_IDX_FIELD_SIZE_MAX + 32] = {
      0};  // reserve 32bytes for operators and '\0'

  String *str = NULL;
  String org_str;
  String conv_str;
  const char *char_ptr;

  uchar pad_char = ' ';
  int key_start_pos = key_part->store_length - key_part->length;
  int pos;
  int new_length;
  int key_length = 0;

#define NULL_BITS 1

  if (NULL == key_ptr) {
    goto done;
  }

  /*if key's length is variable, remove spaces filled by mysql from the end of
    varibale key string. otherwise remove from the end of store_length.*/
  if (key_part->store_length - key_part->length > NULL_BITS) {
    key_length = key_part->null_bit ? get_variable_key_length(&key_ptr[1])
                                    : get_variable_key_length(&key_ptr[0]);
  } else {
    key_length = key_part->length;
  }

  org_str.set((const char *)(key_ptr + key_start_pos), key_length,
              key_part->field->charset());
  str = &org_str;
  if (!my_charset_same(org_str.charset(), &SDB_CHARSET)) {
    rc = sdb_convert_charset(org_str, conv_str, &SDB_CHARSET);
    if (rc) {
      goto error;
    }
    str = &conv_str;
  }

  /*we ignore the spaces end of key string which was filled by mysql.*/
  pos = str->length() - 1;
  char_ptr = str->ptr();
  if (' ' == char_ptr[pos] || '\t' == char_ptr[pos] || '\0' == char_ptr[pos]) {
    pad_char = char_ptr[pos];
    while (pos >= 0 && pad_char == char_ptr[pos]) {
      --pos;
    }
    new_length = pos + 1;
    str->set(str->ptr(), new_length, str->charset());
  }

  if (str->length() >= SDB_IDX_FIELD_SIZE_MAX) {
    rc = SDB_ERR_SIZE_OVF;
    goto error;
  }

  if (0 == strcmp("$et", op_str)) {
    if (!(key_part->key_part_flag & HA_PART_KEY_SEG && str->length())) {
      // TODO: it is exact match if start_key_ptr is same as end_key_ptr.
      /*sdb is sensitive to spaces belong to end string, while mysql is not
      sensitive so we return more results to the HA_READ_KEY_EXACT search.
      'where a = "hello"'
      euqal search in sdb with
      '({a:{$regex:"^hello( ){0,}$"})'
      */
      int cur_pos = 0;
      if (key_part->field->real_type() == MYSQL_TYPE_ENUM ||
          key_part->field->real_type() == MYSQL_TYPE_SET) {
        char enum_val_buf[SDB_IDX_FIELD_SIZE_MAX] = {0};
        String str_val((char *)enum_val_buf + cur_pos,
                       SDB_IDX_FIELD_SIZE_MAX - cur_pos,
                       key_part->field->charset());
        get_enum_key_val(key_ptr, key_part, str_val);
        if (str_val.length() > 0) {
          memcpy(key_field_str_buf + cur_pos, str_val.ptr(), str_val.length());
          cur_pos += str_val.length();
        }
      } else {
        memcpy(key_field_str_buf + cur_pos, str->ptr(), str->length());
        cur_pos += str->length();
      }

      /*replace {a:{$et:"hello"}} with {a:{$regex:"^hello( ){0,}$"}}*/
      obj_builder.appendStrWithNoTerminating(
          "$et", (const char *)key_field_str_buf, cur_pos);
    }
    /* Find next rec. after key-record, or part key where a="abcdefg" (a(10),
       key(a(5)->"abcde")) */
    else {
      if (key_part->field->real_type() == MYSQL_TYPE_ENUM ||
          key_part->field->real_type() == MYSQL_TYPE_SET) {
        get_enum_key_val(key_ptr + key_start_pos, key_part, obj_builder,
                         "$gte");
      } else {
        get_text_key_val((const uchar *)str->ptr(), obj_builder, "$gte",
                         str->length());
      }
    }
  } else {
    if (key_part->field->real_type() == MYSQL_TYPE_ENUM ||
        key_part->field->real_type() == MYSQL_TYPE_SET) {
      get_enum_key_val(key_ptr + key_start_pos, key_part, obj_builder, op_str);
    } else {
      get_text_key_val((const uchar *)str->ptr(), obj_builder, op_str,
                       str->length());
    }
  }
  obj = obj_builder.obj();

done:
  return rc;
error:
  goto done;
}

void get_float_key_val(const uchar *key_ptr, const KEY_PART_INFO *key_part,
                       bson::BSONObjBuilder &obj_builder, const char *op_str) {
  if (NULL == key_ptr) {
    return;
  }

  if (4 == key_part->length) {
    float tmp =
        *((float *)(key_ptr + key_part->store_length - key_part->length));
    obj_builder.append(op_str, tmp);
  } else if (8 == key_part->length) {
    double tmp =
        *((double *)(key_ptr + key_part->store_length - key_part->length));
    obj_builder.append(op_str, tmp);
  }
}

void get_float_key_obj(const uchar *key_ptr, const KEY_PART_INFO *key_part,
                       bson::BSONObj &obj, const char *op_str) {
  bson::BSONObjBuilder obj_builder;
  get_float_key_val(key_ptr, key_part, obj_builder, op_str);
  obj = obj_builder.obj();
}

void get_datetime_key_val(const uchar *key_ptr, const KEY_PART_INFO *key_part,
                          bson::BSONObjBuilder &obj_builder,
                          const char *op_str) {
  MYSQL_TIME ltime;
  longlong ymd = 0, hms = 0;
  longlong ymdhms = 0, ym = 0;
  longlong tmp = 0;
  int frac = 0;
  uint dec = key_part->field->decimals();
  char buff[MAX_FIELD_WIDTH];
  int key_start_pos = key_part->store_length - key_part->length;

  if (NULL == key_ptr) {
    return;
  }

  longlong intpart = mi_uint5korr(key_ptr + key_start_pos) - DATETIMEF_INT_OFS;

  switch (dec) {
    case 0:
    default:
      tmp = intpart << 24;
      break;
    case 1:
    case 2:
      frac = ((int)(signed char)(key_ptr + key_start_pos)[5] * 10000);
      break;
    case 3:
    case 4:
      frac = mi_sint2korr(key_ptr + key_start_pos + 5) * 100;
      break;
    case 5:
    case 6:
      frac = mi_sint3korr(key_ptr + key_start_pos + 5);
      break;
  }
  tmp = (intpart << 24) + frac;

  if ((ltime.neg = (tmp < 0)))
    tmp = -tmp;

  ltime.second_part = tmp % (1LL << 24);
  ymdhms = tmp >> 24;

  ymd = ymdhms >> 17;
  ym = ymd >> 5;
  hms = ymdhms % (1 << 17);

  ltime.day = ymd % (1 << 5);
  ltime.month = ym % 13;
  ltime.year = (uint)(ym / 13);

  ltime.second = hms % (1 << 6);
  ltime.minute = (hms >> 6) % (1 << 6);
  ltime.hour = (uint)(hms >> 12);

  ltime.time_type = MYSQL_TIMESTAMP_DATETIME;

  int len = sprintf(buff, "%04u-%02u-%02u %s%02u:%02u:%02u", ltime.year,
                    ltime.month, ltime.day, (ltime.neg ? "-" : ""), ltime.hour,
                    ltime.minute, ltime.second);
  if (dec) {
    len += sprintf(buff + len, ".%0*lu", (int)dec, ltime.second_part);
  }
  obj_builder.appendStrWithNoTerminating(op_str, (const char *)buff, len);
}

void get_datetime_key_obj(const uchar *key_ptr, const KEY_PART_INFO *key_part,
                          bson::BSONObj &obj, const char *op_str) {
  bson::BSONObjBuilder obj_builder;
  get_datetime_key_val(key_ptr, key_part, obj_builder, op_str);
  obj = obj_builder.obj();
}

int build_start_end_key_obj(
    const uchar *start_key_ptr, key_part_map start_key_part_map,
    const uchar *end_key_ptr, key_part_map end_key_part_map,
    const KEY_PART_INFO *key_part, bson::BSONObj &start_key_obj,
    bson::BSONObj &end_key_obj, const char *start_key_op_str,
    const char *end_key_op_str, enum ha_rkey_function end_find_flag,
    sdb_read_null_mode read_null_mode) {
  int rc = SDB_ERR_OK;
  bool start_key_part_used_and_key_isnull =
      (start_key_part_map & 1) && key_part->null_bit && *start_key_ptr;
  bool end_key_part_used_and_key_isnull =
      (end_key_part_map & 1) && key_part->null_bit && *end_key_ptr;
  bool start_key_part_used = start_key_part_map & 1;
  bool end_key_part_used = end_key_part_map & 1;

  if (start_key_part_used_and_key_isnull || end_key_part_used_and_key_isnull) {
    bson::BSONObjBuilder start_obj_builder;
    bson::BSONObjBuilder end_obj_builder;
    if (read_null_mode == SDB_READ_ONLY_NULL_RECORDS) {
      start_obj_builder.append("$isnull", 1);
      end_obj_builder.append("$isnull", 1);
    }

    else if (read_null_mode == SDB_READ_NON_NULL_RECORDS) {
      start_obj_builder.append("$isnull", 0);
      end_obj_builder.append("$isnull", 0);
    }

    else {
      goto done;
    }

    if (start_key_part_used_and_key_isnull) {
      start_key_obj = start_obj_builder.obj();
    }

    if (end_key_part_used_and_key_isnull) {
      end_key_obj = end_obj_builder.obj();
    }

    goto done;
  }

  switch (key_part->field->type()) {
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONGLONG: {
      if (start_key_part_used) {
        get_int_key_obj(start_key_ptr, key_part, start_key_obj,
                        start_key_op_str);
      }
      if (end_key_part_used) {
        get_int_key_obj(end_key_ptr, key_part, end_key_obj, end_key_op_str);
      }
      break;
    }
    case MYSQL_TYPE_FLOAT:
    case MYSQL_TYPE_DOUBLE: {
      if (start_key_part_used) {
        get_float_key_obj(start_key_ptr, key_part, start_key_obj,
                          start_key_op_str);
      }
      if (end_key_part_used) {
        get_float_key_obj(end_key_ptr, key_part, end_key_obj, end_key_op_str);
      }
      break;
    }
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_BLOB: {
      if (!key_part->field->binary()) {
        if (start_key_part_used) {
          rc = get_text_key_obj(start_key_ptr, key_part, start_key_obj,
                                start_key_op_str);
          if (rc)
            goto error;
        }
        if (end_key_part_used && HA_READ_BEFORE_KEY == end_find_flag) {
          rc = get_text_key_obj(end_key_ptr, key_part, end_key_obj,
                                end_key_op_str);
          if (rc)
            goto error;
        }
      } else {
        // TODO: process the binary
        rc = HA_ERR_UNSUPPORTED;
        goto error;
      }
      break;
    }
    case MYSQL_TYPE_DATETIME: {
      if (start_key_part_used) {
        get_datetime_key_obj(start_key_ptr, key_part, start_key_obj,
                             start_key_op_str);
      }
      if (end_key_part_used) {
        get_datetime_key_obj(end_key_ptr, key_part, end_key_obj,
                             end_key_op_str);
      }
      break;
    }

    default:
      rc = HA_ERR_UNSUPPORTED;
      goto error;
  }

done:
  return rc;
error:
  goto done;
}

int build_match_obj_by_key_parts(uint keynr, const uchar *key_ptr,
                                 key_part_map keypart_map, key_range *end_range,
                                 TABLE *table, bson::BSONObj &matchObj,
                                 const char *last_part_op_str,
                                 const char *pre_k_1_parts_op_str,
                                 bool exact_read,
                                 sdb_read_null_mode read_null_mode) {
  int rc = 0;
  KEY *keyInfo;
  bson::BSONObj start_key_obj, end_key_obj;
  bson::BSONObj start_obj, end_obj, start, end;
  bson::BSONArrayBuilder start_obj_arr, end_obj_arr;
  bson::BSONArrayBuilder array_builder;
  const char *end_key_op_str;
  const KEY_PART_INFO *key_start;
  const KEY_PART_INFO *key_part;
  const KEY_PART_INFO *key_part_temp;
  const KEY_PART_INFO *key_end;
  const uchar *start_key_ptr = key_ptr;
  key_part_map start_key_part_map = keypart_map;
  const uchar *end_key_ptr = NULL;
  key_part_map end_key_part_map = 0;

  const uchar *start_key_ptr_tmp;
  const uchar *end_key_ptr_tmp;
  key_part_map start_key_part_map_tmp = 0;
  key_part_map end_key_part_map_tmp = 0;
  enum ha_rkey_function end_find_flag = HA_READ_INVALID;
  const char *final_op_str = NULL;
  bool start_obj_arr_empty = true;
  bool end_obj_arr_empty = true;

  if (MAX_KEY == keynr || table->s->keys <= 0) {
    return rc;
  }

  keyInfo = table->key_info + keynr;
  if (NULL == keyInfo || NULL == keyInfo->key_part) {
    return rc;
  }

  key_start = keyInfo->key_part;
  key_end = key_start + keyInfo->user_defined_key_parts;
  start_key_ptr = key_ptr;
  start_key_part_map = keypart_map;

  if (NULL != end_range) {
    end_key_ptr = end_range->key;
    end_key_part_map = end_range->keypart_map;
    end_find_flag = end_range->flag;
    end_key_op_str = "$lte";
  } else {
    end_key_ptr = NULL;
    end_key_part_map_tmp = 0;
  }

  for (key_part = key_start;
       key_part != key_end && (start_key_part_map | end_key_part_map);
       ++key_part) {
    bson::BSONArrayBuilder start_key_array_builder;
    bson::BSONArrayBuilder end_key_array_builder;
    bool start_key_array_empty = true;
    bool end_key_array_empty = true;

    start_key_ptr_tmp = key_ptr;
    start_key_part_map_tmp = keypart_map;
    if (NULL != end_range) {
      end_key_ptr_tmp = end_range->key;
      end_key_part_map_tmp = end_range->keypart_map;
    } else {
      end_key_ptr_tmp = NULL;
      end_key_part_map_tmp = 0;
    }

    for (key_part_temp = (exact_read) ? key_part : key_start;
         key_part_temp < key_part; ++key_part_temp) {
      rc = build_start_end_key_obj(start_key_ptr_tmp, start_key_part_map_tmp,
                                   end_key_ptr_tmp, end_key_part_map_tmp,
                                   key_part_temp, start_key_obj, end_key_obj,
                                   "$et", "$et", end_find_flag, read_null_mode);
      if (rc) {
        goto error;
        ;
      }

      if (!start_key_obj.isEmpty()) {
        start_key_array_builder.append(
            BSON(key_part_temp->field->field_name << start_key_obj));
        start_key_array_empty = false;
        start_key_obj = empty_obj;
      }
      if (!end_key_obj.isEmpty()) {
        end_key_array_builder.append(
            BSON(key_part_temp->field->field_name << end_key_obj));
        end_key_array_empty = false;
        end_key_obj = empty_obj;
      }

      start_key_ptr_tmp += key_part_temp->store_length;
      end_key_ptr_tmp += key_part_temp->store_length;
      start_key_part_map_tmp >>= 1;
      end_key_part_map_tmp >>= 1;
    }

    if (key_part_temp == key_part) {
      rc = build_start_end_key_obj(
          start_key_ptr, start_key_part_map, end_key_ptr, end_key_part_map,
          key_part_temp, start_key_obj, end_key_obj,
          (key_part == key_end - 1 || start_key_part_map == 1)
              ? last_part_op_str
              : pre_k_1_parts_op_str,
          end_key_op_str, end_find_flag, read_null_mode);
      if (rc) {
        goto error;
        ;
      }

      if (!start_key_obj.isEmpty()) {
        start_key_array_builder.append(
            BSON(key_part_temp->field->field_name << start_key_obj));
        start_key_array_empty = false;
        start_key_obj = empty_obj;
      }
      if (!end_key_obj.isEmpty()) {
        end_key_array_builder.append(
            BSON(key_part_temp->field->field_name << end_key_obj));
        end_key_array_empty = false;
        end_key_obj = empty_obj;
      }
    }

    start_key_ptr += key_part_temp->store_length;
    end_key_ptr += key_part_temp->store_length;
    start_key_part_map >>= 1;
    end_key_part_map >>= 1;
    if (!start_key_array_empty) {
      start_obj = BSON("$and" << start_key_array_builder.arr());
      start_obj_arr.append(start_obj);
      start_obj_arr_empty = false;
    }
    if (!end_key_array_empty) {
      end_obj = BSON("$and" << end_key_array_builder.arr());
      end_obj_arr.append(end_obj);
      end_obj_arr_empty = false;
    }
  }

  final_op_str = (exact_read) ? "$and" : "$or";
  if (!start_obj_arr_empty) {
    start = BSON(final_op_str << start_obj_arr.arr());
    array_builder.append(start);
  }

  if (!end_obj_arr_empty) {
    end = BSON(final_op_str << end_obj_arr.arr());
    array_builder.append(end);
  }

  matchObj = BSON("$and" << array_builder.arr());

done:
  return rc;
error:
  goto done;
}

int build_match_obj_by_start_stop_key(uint keynr, const uchar *key_ptr,
                                      key_part_map keypart_map,
                                      enum ha_rkey_function find_flag,
                                      key_range *end_range, TABLE *table,
                                      bson::BSONObj &matchObj,
                                      int *order_direction) {
  int rc = 0;
  sdb_search_match_mode start_match_mode = SDB_UNSUPP;
  const char *last_part_op_str = NULL;
  const char *pre_k_1_parts_op_str = NULL;
  sdb_read_null_mode read_null_mode = SDB_READ_ALL_RECORDS;
  bool exact_read = false;

  start_match_mode = conver_search_mode_to_sdb_mode(find_flag);
  if (SDB_UNSUPP == start_match_mode) {
    rc = HA_ERR_UNSUPPORTED;
  }

  *order_direction = get_key_direction(start_match_mode);
  /*multiple-column index which have k parts, pre_k_1_parts_op_str is the first
   k-1 parts op_str. For the query SELECT MIN(key_part_k) FROM t1 WHERE
   key_part_1 >= const and ... and key_part_k >= const when build start obj, if
   the op_str is "$gte", then pre_k_1_parts_op_str should be "$gt", $or:
   $gt:{k_part_1:const1} $et:{k_part_1:const1}, $gt:{k_part_2:const2}
        ...
        $et:{k_part_1:const1},
   $et:{k_part_2:const2},...,$et:{k_part_k-1:constk-1},$gte:{k_part_k:constk} as
   same, if the op_str is "$lte", then pre_k_1_parts_op_str should be "$lt",
  */
  switch (start_match_mode) {
    case SDB_ET:
      last_part_op_str = "$et";
      pre_k_1_parts_op_str = "$et";
      exact_read = true;
      /*$et NULL means reading in all null records.*/
      read_null_mode = SDB_READ_ONLY_NULL_RECORDS;
      break;
    case SDB_GTE:
      last_part_op_str = "$gte";
      pre_k_1_parts_op_str = "$gt";
      read_null_mode = SDB_READ_ALL_RECORDS;
      break;
    case SDB_GT:
      last_part_op_str = "$gt";
      pre_k_1_parts_op_str = "$gt";
      read_null_mode = SDB_READ_NON_NULL_RECORDS;
      break;
    case SDB_LT:
      last_part_op_str = "$lt";
      pre_k_1_parts_op_str = "$lt";
      read_null_mode = SDB_READ_ONLY_NULL_RECORDS;
      break;
    case SDB_LTE:
      last_part_op_str = "$lte";
      pre_k_1_parts_op_str = "$lt";
      /*$lte NULL means reading in all null records.*/
      read_null_mode = SDB_READ_ONLY_NULL_RECORDS;
      break;
    default:
      rc = HA_ERR_UNSUPPORTED;
      goto error;
  }
  build_match_obj_by_key_parts(keynr, key_ptr, keypart_map, end_range, table,
                               matchObj, last_part_op_str, pre_k_1_parts_op_str,
                               exact_read, read_null_mode);
done:
  return rc;
error:
  goto done;
}
