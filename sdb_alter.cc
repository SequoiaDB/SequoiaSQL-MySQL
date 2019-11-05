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

#ifndef MYSQL_SERVER
#define MYSQL_SERVER
#endif

#include "sdb_sql.h"
#include "ha_sdb.h"
#include <sql_class.h>
#include <mysql/plugin.h>
#include <sql_time.h>
#include "sdb_log.h"
#include "sdb_idx.h"
#include "sdb_thd.h"
#include "sdb_item.h"

#ifdef IS_MYSQL
#include <json_dom.h>
#endif

static const alter_table_operations INPLACE_ONLINE_ADDIDX =
    ALTER_ADD_NON_UNIQUE_NON_PRIM_INDEX | ALTER_ADD_UNIQUE_INDEX |
    ALTER_ADD_PK_INDEX | ALTER_COLUMN_NOT_NULLABLE;

static const alter_table_operations INPLACE_ONLINE_DROPIDX =
    ALTER_DROP_NON_UNIQUE_NON_PRIM_INDEX | ALTER_DROP_UNIQUE_INDEX |
    ALTER_DROP_PK_INDEX | ALTER_COLUMN_NULLABLE;

static const alter_table_operations INPLACE_ONLINE_OPERATIONS =
    INPLACE_ONLINE_ADDIDX | INPLACE_ONLINE_DROPIDX | ALTER_ADD_COLUMN |
    ALTER_DROP_COLUMN | ALTER_STORED_COLUMN_ORDER | ALTER_STORED_COLUMN_TYPE |
    ALTER_COLUMN_DEFAULT | ALTER_COLUMN_EQUAL_PACK_LENGTH |
    ALTER_CHANGE_CREATE_OPTION | ALTER_RENAME_INDEX | ALTER_RENAME |
    ALTER_COLUMN_INDEX_LENGTH | ALTER_ADD_FOREIGN_KEY | ALTER_DROP_FOREIGN_KEY |
    ALTER_INDEX_COMMENT | ALTER_COLUMN_STORAGE_TYPE |
    ALTER_COLUMN_COLUMN_FORMAT | ALTER_RECREATE_TABLE;

static const int SDB_TYPE_NUM = 23;
static const uint INT_TYPE_NUM = 5;
static const enum_field_types INT_TYPES[INT_TYPE_NUM] = {
    MYSQL_TYPE_TINY, MYSQL_TYPE_SHORT, MYSQL_TYPE_INT24, MYSQL_TYPE_LONG,
    MYSQL_TYPE_LONGLONG};
static const uint FLOAT_EXACT_DIGIT_LEN = 5;
static const uint DOUBLE_EXACT_DIGIT_LEN = 15;

uint get_int_bit_num(enum_field_types type) {
  static const uint INT_BIT_NUMS[INT_TYPE_NUM] = {8, 16, 24, 32, 64};
  uint i = 0;
  while (type != INT_TYPES[i]) {
    ++i;
  }
  return INT_BIT_NUMS[i];
}

uint get_int_max_strlen(Field_num *field) {
  static const uint INT_LEN8 = 4;
  static const uint UINT_LEN8 = 3;
  static const uint INT_LEN16 = 6;
  static const uint UINT_LEN16 = 5;
  static const uint INT_LEN24 = 8;
  static const uint UINT_LEN24 = 8;
  static const uint INT_LEN32 = 11;
  static const uint UINT_LEN32 = 10;
  static const uint INT_LEN64 = 20;
  static const uint UINT_LEN64 = 20;
  static const uint SIGNED_INT_LEN[INT_TYPE_NUM] = {
      INT_LEN8, INT_LEN16, INT_LEN24, INT_LEN32, INT_LEN64};
  static const uint UNSIGNED_INT_LEN[INT_TYPE_NUM] = {
      UINT_LEN8, UINT_LEN16, UINT_LEN24, UINT_LEN32, UINT_LEN64};

  uint i = 0;
  while (INT_TYPES[i] != field->type()) {
    ++i;
  }

  uint len = 0;
  if (!field->unsigned_flag) {
    len = SIGNED_INT_LEN[i];
  } else {
    len = UNSIGNED_INT_LEN[i];
  }

  return len;
}

void get_int_range(Field_num *field, longlong &low_bound, ulonglong &up_bound) {
  static const ulonglong UINT_MAX64 = 0xFFFFFFFFFFFFFFFFLL;
  static const ulonglong UNSIGNED_UP_BOUNDS[INT_TYPE_NUM] = {
      UINT_MAX8, UINT_MAX16, UINT_MAX24, UINT_MAX32, UINT_MAX64};
  static const longlong SIGNED_LOW_BOUNDS[INT_TYPE_NUM] = {
      INT_MIN8, INT_MIN16, INT_MIN24, INT_MIN32, INT_MIN64};
  static const longlong SIGNED_UP_BOUNDS[INT_TYPE_NUM] = {
      INT_MAX8, INT_MAX16, INT_MAX24, INT_MAX32, INT_MAX64};

  uint i = 0;
  while (INT_TYPES[i] != field->type()) {
    ++i;
  }

  if (!field->unsigned_flag) {
    low_bound = SIGNED_LOW_BOUNDS[i];
    up_bound = SIGNED_UP_BOUNDS[i];
  } else {
    low_bound = 0;
    up_bound = UNSIGNED_UP_BOUNDS[i];
  }
}

/*
  Interface to build the cast rule.
  @return false if success.
*/
class I_build_cast_rule {
 public:
  virtual ~I_build_cast_rule() {}
  virtual bool operator()(bson::BSONObjBuilder &builder, Field *old_field,
                          Field *new_field) = 0;
};

class Return_success : public I_build_cast_rule {
 public:
  bool operator()(bson::BSONObjBuilder &builder, Field *old_field,
                  Field *new_field) {
    return false;
  }
};

Return_success suc;

class Return_failure : public I_build_cast_rule {
 public:
  bool operator()(bson::BSONObjBuilder &builder, Field *old_field,
                  Field *new_field) {
    return true;
  }
};

Return_failure fai;

class Cast_int2int : public I_build_cast_rule {
 public:
  bool operator()(bson::BSONObjBuilder &builder, Field *old_field,
                  Field *new_field) {
    bool rs = true;
    longlong old_low = 0;
    ulonglong old_up = 0;
    longlong new_low = 0;
    ulonglong new_up = 0;

    get_int_range((Field_num *)old_field, old_low, old_up);
    get_int_range((Field_num *)new_field, new_low, new_up);
    if (new_low <= old_low && old_up <= new_up) {
      rs = false;
      goto done;
    }

  done:
    return rs;
  }
};

Cast_int2int i2i;

class Cast_int2float : public I_build_cast_rule {
 public:
  bool operator()(bson::BSONObjBuilder &builder, Field *old_field,
                  Field *new_field) {
    bool rs = true;
    Field_num *int_field = (Field_num *)old_field;
    Field_real *float_field = (Field_real *)new_field;
    bool signed2unsigned =
        !int_field->unsigned_flag && float_field->unsigned_flag;
    uint float_len = 0;
    uint float_len_max = 0;
    uint int_len = get_int_max_strlen(int_field);

    if (float_field->type() == MYSQL_TYPE_FLOAT) {
      float_len_max = FLOAT_EXACT_DIGIT_LEN;
    } else {  // == MYSQL_TYPE_DOUBLE
      float_len_max = DOUBLE_EXACT_DIGIT_LEN;
    }

    if (float_field->not_fixed) {
      float_len = float_len_max;
    } else {
      uint m = new_field->field_length;
      uint d = new_field->decimals();
      float_len = m - d;
      if (float_len > float_len_max) {
        float_len = float_len_max;
      }
    }

    if (!int_field->unsigned_flag) {
      --int_len;  // ignore the '-'
    }
    if (!signed2unsigned && float_len >= int_len) {
      rs = false;
      goto done;
    }

  done:
    return rs;
  }
};

Cast_int2float i2f;

class Cast_int2decimal : public I_build_cast_rule {
 public:
  bool operator()(bson::BSONObjBuilder &builder, Field *old_field,
                  Field *new_field) {
    DBUG_ASSERT(new_field->type() == MYSQL_TYPE_NEWDECIMAL);

    bool rs = true;
    Field_num *int_field = (Field_num *)old_field;
    Field_new_decimal *dec_field = (Field_new_decimal *)new_field;
    bool signed2unsigned =
        !int_field->unsigned_flag && dec_field->unsigned_flag;

    uint m = dec_field->precision;
    uint d = dec_field->decimals();
    uint int_len = get_int_max_strlen(int_field);

    if (!int_field->unsigned_flag) {
      --int_len;  // ignore the '-'
    }
    if (!signed2unsigned && (m - d) >= int_len) {
      rs = false;
      goto done;
    }

  done:
    return rs;
  }
};

Cast_int2decimal i2d;

class Cast_int2bit : public I_build_cast_rule {
 public:
  bool operator()(bson::BSONObjBuilder &builder, Field *old_field,
                  Field *new_field) {
    bool rs = true;
    Field_num *int_field = (Field_num *)old_field;
    uint int_bit_num = get_int_bit_num(old_field->type());

    if (int_field->unsigned_flag && new_field->field_length >= int_bit_num) {
      rs = false;
      goto done;
    }

  done:
    return rs;
  }
};

Cast_int2bit i2b;

class Cast_int2set : public I_build_cast_rule {
 public:
  bool operator()(bson::BSONObjBuilder &builder, Field *old_field,
                  Field *new_field) {
    bool rs = true;
    uint int_bit_num = get_int_bit_num(old_field->type());
    uint set_bit_num = ((Field_set *)new_field)->typelib->count;
    bool is_unsigned = ((Field_num *)old_field)->unsigned_flag;
    if (is_unsigned && set_bit_num >= int_bit_num) {
      rs = false;
      goto done;
    }

  done:
    return rs;
  }
};

Cast_int2set i2s;

class Cast_int2enum : public I_build_cast_rule {
 public:
  bool operator()(bson::BSONObjBuilder &builder, Field *old_field,
                  Field *new_field) {
    bool rs = true;
    uint enum_max = ((Field_enum *)new_field)->typelib->count;
    bool is_unsigned = ((Field_num *)old_field)->unsigned_flag;
    if (is_unsigned && enum_max >= old_field->get_max_int_value()) {
      rs = false;
      goto done;
    }

  done:
    return rs;
  }
};

Cast_int2enum i2e;

class Cast_float2int : public I_build_cast_rule {
 public:
  bool operator()(bson::BSONObjBuilder &builder, Field *old_field,
                  Field *new_field) {
    bool rs = true;
    Field_real *float_field = (Field_real *)old_field;
    Field_num *int_field = (Field_num *)new_field;
    bool signed2unsigned =
        !float_field->unsigned_flag && int_field->unsigned_flag;

    if (!signed2unsigned && !float_field->not_fixed &&
        0 == float_field->decimals()) {
      uint float_len = float_field->field_length;
      uint int_len = get_int_max_strlen(int_field);
      if (!int_field->unsigned_flag) {
        --int_len;  // ignore the '-'
      }
      if (int_len > float_len) {
        rs = false;
        goto done;
      }
    }

  done:
    return rs;
  }
};

Cast_float2int f2i;

class Cast_float2float : public I_build_cast_rule {
 public:
  bool operator()(bson::BSONObjBuilder &builder, Field *old_field,
                  Field *new_field) {
    bool rs = true;
    Field_real *old_float = (Field_real *)old_field;
    Field_real *new_float = (Field_real *)new_field;
    bool signed2unsigned =
        !old_float->unsigned_flag && new_float->unsigned_flag;
    bool may_be_truncated = false;

    if (old_float->type() == MYSQL_TYPE_DOUBLE &&
        new_float->type() == MYSQL_TYPE_FLOAT) {
      may_be_truncated = true;
    } else {
      if (!old_float->not_fixed && !new_float->not_fixed) {
        uint old_m = old_float->field_length;
        uint new_m = new_float->field_length;
        uint old_d = old_float->decimals();
        uint new_d = new_float->decimals();
        if (old_d > new_d || (old_m - old_d) > (new_m - new_d)) {
          may_be_truncated = true;
        }
      } else if (old_float->not_fixed && !new_float->not_fixed) {
        may_be_truncated = true;
      }
    }

    if (!signed2unsigned && !may_be_truncated) {
      rs = false;
      goto done;
    }

  done:
    return rs;
  }
};

Cast_float2float f2f;

class Cast_float2decimal : public I_build_cast_rule {
 public:
  bool operator()(bson::BSONObjBuilder &builder, Field *old_field,
                  Field *new_field) {
    bool rs = true;
    Field_real *float_field = (Field_real *)old_field;
    Field_new_decimal *dec_field = (Field_new_decimal *)new_field;

    bool signed2unsigned =
        !float_field->unsigned_flag && dec_field->unsigned_flag;
    bool may_be_truncated = false;

    if (float_field->not_fixed) {
      may_be_truncated = true;
    } else {
      uint old_m = float_field->field_length;
      uint new_m = dec_field->precision;
      uint old_d = float_field->decimals();
      uint new_d = dec_field->decimals();
      if (old_d > new_d || (old_m - old_d) > (new_m - new_d)) {
        may_be_truncated = true;
      }
    }

    if (!signed2unsigned && !may_be_truncated) {
      rs = false;
      goto done;
    }

  done:
    return rs;
  }
};

Cast_float2decimal f2d;

class Cast_decimal2int : public I_build_cast_rule {
 public:
  bool operator()(bson::BSONObjBuilder &builder, Field *old_field,
                  Field *new_field) {
    DBUG_ASSERT(old_field->type() == MYSQL_TYPE_NEWDECIMAL);

    bool rs = true;
    Field_new_decimal *dec_field = (Field_new_decimal *)old_field;
    Field_num *int_field = (Field_num *)new_field;
    bool signed2unsigned =
        !dec_field->unsigned_flag && int_field->unsigned_flag;

    if (!signed2unsigned && 0 == dec_field->decimals()) {
      uint dec_len = dec_field->precision;
      uint int_len = get_int_max_strlen(int_field);
      if (!int_field->unsigned_flag) {
        --int_len;  // ignore the '-'
      }
      if (int_len > dec_len) {
        rs = false;
        goto done;
      }
    }

  done:
    return rs;
  }
};

Cast_decimal2int d2i;

class Cast_decimal2decimal : public I_build_cast_rule {
 public:
  bool operator()(bson::BSONObjBuilder &builder, Field *old_field,
                  Field *new_field) {
    DBUG_ASSERT(old_field->type() == MYSQL_TYPE_NEWDECIMAL);
    DBUG_ASSERT(new_field->type() == MYSQL_TYPE_NEWDECIMAL);

    bool rs = true;
    Field_new_decimal *old_dec = (Field_new_decimal *)old_field;
    Field_new_decimal *new_dec = (Field_new_decimal *)new_field;
    bool signed2unsigned = !old_dec->unsigned_flag && new_dec->unsigned_flag;

    uint old_m = old_dec->precision;
    uint new_m = new_dec->precision;
    uint old_d = old_dec->decimals();
    uint new_d = new_dec->decimals();
    if (!signed2unsigned && old_d <= new_d &&
        (old_m - old_d) <= (new_m - new_d)) {
      rs = false;
      goto done;
    }

  done:
    return rs;
  }
};

Cast_decimal2decimal d2d;

class Cast_char2char : public I_build_cast_rule {
 public:
  bool operator()(bson::BSONObjBuilder &builder, Field *old_field,
                  Field *new_field) {
    /*
      Binary-like types and string-like types are similar.
      Their data types are ambiguous. The BINARY type is MYSQL_TYPE_STRING, and
      the TEXT type is MYSQL_TYPE_BLOB. The exact flag to distinguish them is
      Field::binary().
     */
    bool rs = true;
    uint old_len = old_field->char_length();
    uint new_len = new_field->char_length();
    if (old_field->binary() == new_field->binary() && new_len >= old_len) {
      rs = false;
      goto done;
    }

  done:
    return rs;
  }
};

Cast_char2char c2c;

class Cast_bit2bit : public I_build_cast_rule {
 public:
  bool operator()(bson::BSONObjBuilder &builder, Field *old_field,
                  Field *new_field) {
    bool rs = true;
    if (old_field->field_length <= new_field->field_length) {
      rs = false;
      goto done;
    }

  done:
    return rs;
  }
};

Cast_bit2bit b2b;

class Cast_time2time : public I_build_cast_rule {
 public:
  bool operator()(bson::BSONObjBuilder &builder, Field *old_field,
                  Field *new_field) {
    bool rs = true;
    if (old_field->decimals() <= new_field->decimals()) {
      rs = false;
      goto done;
    }

  done:
    return rs;
  }
};

Cast_time2time t2t;

class Cast_datetime2datetime : public I_build_cast_rule {
 public:
  bool operator()(bson::BSONObjBuilder &builder, Field *old_field,
                  Field *new_field) {
    bool rs = true;
    if (old_field->decimals() <= new_field->decimals()) {
      rs = false;
      goto done;
    }

  done:
    return rs;
  }
};

Cast_datetime2datetime m2m;

class Cast_timestamp2timestamp : public I_build_cast_rule {
 public:
  bool operator()(bson::BSONObjBuilder &builder, Field *old_field,
                  Field *new_field) {
    bool rs = true;
    if (old_field->decimals() <= new_field->decimals()) {
      rs = false;
      goto done;
    }

  done:
    return rs;
  }
};

Cast_timestamp2timestamp p2p;

class Cast_set2set : public I_build_cast_rule {
 public:
  bool operator()(bson::BSONObjBuilder &builder, Field *old_field,
                  Field *new_field) {
    bool rs = true;
    TYPELIB *old_typelib = ((Field_set *)old_field)->typelib;
    TYPELIB *new_typelib = ((Field_set *)new_field)->typelib;
    bool is_append = true;
    if (old_typelib->count <= new_typelib->count) {
      for (uint i = 0; i < old_typelib->count; ++i) {
        if (strcmp(old_typelib->type_names[i], new_typelib->type_names[i])) {
          is_append = false;
          break;
        }
      }
    } else {
      is_append = false;
    }
    if (is_append) {
      rs = false;
      goto done;
    }

  done:
    return rs;
  }
};

Cast_set2set s2s;

class Cast_enum2enum : public I_build_cast_rule {
 public:
  bool operator()(bson::BSONObjBuilder &builder, Field *old_field,
                  Field *new_field) {
    bool rs = true;
    TYPELIB *old_typelib = ((Field_enum *)old_field)->typelib;
    TYPELIB *new_typelib = ((Field_enum *)new_field)->typelib;
    bool is_append = true;
    if (old_typelib->count <= new_typelib->count) {
      for (uint i = 0; i < old_typelib->count; ++i) {
        if (strcmp(old_typelib->type_names[i], new_typelib->type_names[i])) {
          is_append = false;
          break;
        }
      }
    } else {
      is_append = false;
    }
    if (is_append) {
      rs = false;
      goto done;
    }

  done:
    return rs;
  }
};

Cast_enum2enum e2e;

I_build_cast_rule *build_cast_funcs[SDB_TYPE_NUM][SDB_TYPE_NUM] = {
    /* 00,   01,   02,   03,   04,   05,   06,   07,   08,   09,   10,   11,
       12,   13,   14,   15,   16,   17,   18,   19,   20,   21,   22 */
    /*00 TINY*/
    {&i2i, &i2i, &i2i, &i2i, &i2i, &i2f, &i2f, &i2d, &fai, &fai, &fai, &fai,
     &fai, &fai, &i2b, &fai, &fai, &fai, &fai, &fai, &i2s, &i2e, &fai},
    /*01 SHORT*/
    {&i2i, &i2i, &i2i, &i2i, &i2i, &i2f, &i2f, &i2d, &fai, &fai, &fai, &fai,
     &fai, &fai, &i2b, &fai, &fai, &fai, &fai, &fai, &i2s, &i2e, &fai},
    /*02 INT24*/
    {&i2i, &i2i, &i2i, &i2i, &i2i, &i2f, &i2f, &i2d, &fai, &fai, &fai, &fai,
     &fai, &fai, &i2b, &fai, &fai, &fai, &fai, &fai, &i2s, &i2e, &fai},
    /*03 LONG*/
    {&i2i, &i2i, &i2i, &i2i, &i2i, &i2f, &i2f, &i2d, &fai, &fai, &fai, &fai,
     &fai, &fai, &i2b, &fai, &fai, &fai, &fai, &fai, &i2s, &i2e, &fai},
    /*04 LONGLONG*/
    {&i2i, &i2i, &i2i, &i2i, &i2i, &i2f, &i2f, &i2d, &fai, &fai, &fai, &fai,
     &fai, &fai, &i2b, &fai, &fai, &fai, &fai, &fai, &i2s, &i2e, &fai},
    /*05 FLOAT*/
    {&f2i, &f2i, &f2i, &f2i, &f2i, &f2f, &f2f, &f2d, &fai, &fai, &fai, &fai,
     &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai},
    /*06 DOUBLE*/
    {&f2i, &f2i, &f2i, &f2i, &f2i, &f2f, &f2f, &f2d, &fai, &fai, &fai, &fai,
     &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai},
    /*07 DECIMAL*/
    {&d2i, &d2i, &d2i, &d2i, &d2i, &fai, &fai, &d2d, &fai, &fai, &fai, &fai,
     &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai},
    /*08 STRING*/
    {&fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &c2c, &c2c, &c2c, &c2c,
     &c2c, &c2c, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai},
    /*09 VAR_STRING*/
    {&fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &c2c, &c2c, &c2c,
     &c2c, &c2c, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai},
    /*10 TINY_BLOB*/
    {&fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &c2c, &c2c, &c2c,
     &c2c, &c2c, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai},
    /*11 BLOB*/
    {&fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &c2c, &c2c, &c2c,
     &c2c, &c2c, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai},
    /*12 MEDIUM_BLOB*/
    {&fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &c2c, &c2c, &c2c,
     &c2c, &c2c, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai},
    /*13 LONG_BLOB*/
    {&fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &c2c, &c2c, &c2c,
     &c2c, &c2c, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai},
    /*14 BIT*/
    {&fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai,
     &fai, &fai, &b2b, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai},
    /*15 YEAR*/
    {&fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai,
     &fai, &fai, &fai, &suc, &fai, &fai, &fai, &fai, &fai, &fai, &fai},
    /*16 TIME*/
    {&fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai,
     &fai, &fai, &fai, &fai, &t2t, &fai, &fai, &fai, &fai, &fai, &fai},
    /*17 DATE*/
    {&fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai,
     &fai, &fai, &fai, &fai, &fai, &suc, &fai, &fai, &fai, &fai, &fai},
    /*18 DATETIME*/
    {&fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai,
     &fai, &fai, &fai, &fai, &fai, &fai, &m2m, &fai, &fai, &fai, &fai},
    /*19 TIMESTAMP*/
    {&fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai,
     &fai, &fai, &fai, &fai, &fai, &fai, &fai, &p2p, &fai, &fai, &fai},
    /*20 SET*/
    {&fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai,
     &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &s2s, &fai, &fai},
    /*21 ENUM*/
    {&fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai,
     &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &e2e, &fai},
    /*22 JSON*/
    {&fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai,
     &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &fai, &suc}};

int get_type_idx(enum enum_field_types type) {
  switch (type) {
    case MYSQL_TYPE_TINY:
      return 0;
    case MYSQL_TYPE_SHORT:
      return 1;
    case MYSQL_TYPE_INT24:
      return 2;
    case MYSQL_TYPE_LONG:
      return 3;
    case MYSQL_TYPE_LONGLONG:
      return 4;
    case MYSQL_TYPE_FLOAT:
      return 5;
    case MYSQL_TYPE_DOUBLE:
      return 6;
    case MYSQL_TYPE_NEWDECIMAL:
    case MYSQL_TYPE_DECIMAL:
      return 7;
    case MYSQL_TYPE_STRING:
      return 8;
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_VAR_STRING:
      return 9;
    case MYSQL_TYPE_TINY_BLOB:
      return 10;
    case MYSQL_TYPE_BLOB:
      return 11;
    case MYSQL_TYPE_MEDIUM_BLOB:
      return 12;
    case MYSQL_TYPE_LONG_BLOB:
      return 13;
    case MYSQL_TYPE_BIT:
      return 14;
    case MYSQL_TYPE_YEAR:
      return 15;
    case MYSQL_TYPE_TIME:
    case MYSQL_TYPE_TIME2:
      return 16;
    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_NEWDATE:
      return 17;
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_DATETIME2:
      return 18;
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_TIMESTAMP2:
      return 19;
    case MYSQL_TYPE_SET:
      return 20;
    case MYSQL_TYPE_ENUM:
      return 21;
#ifdef IS_MYSQL
    case MYSQL_TYPE_JSON:
      return 22;
#endif
    default: {
      // impossible types
      DBUG_ASSERT(false);
      return 0;
    }
  }
}

const char *sdb_fieldtype2str(enum enum_field_types type) {
  switch (type) {
    case MYSQL_TYPE_BIT:
      return "BIT";
    case MYSQL_TYPE_BLOB:
      return "BLOB";
    case MYSQL_TYPE_DATE:
      return "DATE";
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_DATETIME2:
      return "DATETIME";
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_NEWDECIMAL:
      return "DECIMAL";
    case MYSQL_TYPE_DOUBLE:
      return "DOUBLE";
    case MYSQL_TYPE_ENUM:
      return "ENUM";
    case MYSQL_TYPE_FLOAT:
      return "FLOAT";
    case MYSQL_TYPE_GEOMETRY:
      return "GEOMETRY";
    case MYSQL_TYPE_INT24:
      return "INT24";
#ifdef IS_MYSQL
    case MYSQL_TYPE_JSON:
      return "JSON";
#endif
    case MYSQL_TYPE_LONG:
      return "LONG";
    case MYSQL_TYPE_LONGLONG:
      return "LONGLONG";
    case MYSQL_TYPE_LONG_BLOB:
      return "LONG_BLOB";
    case MYSQL_TYPE_MEDIUM_BLOB:
      return "MEDIUM_BLOB";
    case MYSQL_TYPE_NEWDATE:
      return "NEWDATE";
    case MYSQL_TYPE_NULL:
      return "NULL";
    case MYSQL_TYPE_SET:
      return "SET";
    case MYSQL_TYPE_SHORT:
      return "SHORT";
    case MYSQL_TYPE_STRING:
      return "STRING";
    case MYSQL_TYPE_TIME:
    case MYSQL_TYPE_TIME2:
      return "TIME";
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_TIMESTAMP2:
      return "TIMESTAMP";
    case MYSQL_TYPE_TINY:
      return "TINY";
    case MYSQL_TYPE_TINY_BLOB:
      return "TINY_BLOB";
    case MYSQL_TYPE_VARCHAR:
      return "VARCHAR";
    case MYSQL_TYPE_VAR_STRING:
      return "VAR_STRING";
    case MYSQL_TYPE_YEAR:
      return "YEAR";
    default:
      return "unknown type";
  }
}

struct Col_alter_info : public Sql_alloc {
  static const int CHANGE_DATA_TYPE = 1;
  static const int ADD_AUTO_INC = 2;
  static const int DROP_AUTO_INC = 4;
  static const int TURN_TO_NOT_NULL = 8;

  Field *before;
  Field *after;
  int op_flag;
  bson::BSONObj cast_rule;
};

struct Sdb_alter_ctx : public inplace_alter_handler_ctx {
  List<Field> dropped_columns;
  List<Field> added_columns;
  List<Col_alter_info> changed_columns;
};

bool is_strict_mode(sql_mode_t sql_mode) {
  return (sql_mode & MODE_STRICT_ALL_TABLES) ||
         (sdb_use_transaction && (sql_mode & MODE_STRICT_TRANS_TABLES));
}

int ha_sdb::append_default_value(bson::BSONObjBuilder &builder, Field *field) {
  int rc = 0;

  // Avoid assertion in ::store()
  bool is_set = bitmap_is_set(field->table->write_set, field->field_index);
  if (is_set) {
    bitmap_set_bit(field->table->write_set, field->field_index);
  }

  switch (field->type()) {
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_LONGLONG:
    case MYSQL_TYPE_BIT:
    case MYSQL_TYPE_YEAR:
    case MYSQL_TYPE_DATE: {
      longlong org_val = field->val_int();
      field->set_default();
      rc = field_to_obj(field, builder);
      field->store(org_val);
      break;
    }
    case MYSQL_TYPE_FLOAT:
    case MYSQL_TYPE_DOUBLE:
    case MYSQL_TYPE_TIME: {
      double org_val = field->val_real();
      field->set_default();
      rc = field_to_obj(field, builder);
      field->store(org_val);
      break;
    }
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_TIMESTAMP: {
      struct timeval org_val;
      sdb_field_get_timestamp(field, &org_val);
      field->set_default();
      rc = field_to_obj(field, builder);
      sdb_field_store_timestamp(field, &org_val);
      break;
    }
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_NEWDECIMAL:
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_VARCHAR: {
      String org_val;
      field->val_str(&org_val);
      field->set_default();
      rc = field_to_obj(field, builder);
      field->store(org_val.ptr(), org_val.length(), org_val.charset());
      break;
    }
#ifdef IS_MYSQL
    case MYSQL_TYPE_JSON:
#endif
    case MYSQL_TYPE_BLOB:
    default: {
      // These types never have default.
      DBUG_ASSERT(0);
      rc = HA_ERR_INTERNAL_ERROR;
    }
  }

  if (is_set) {
    bitmap_clear_bit(field->table->write_set, field->field_index);
  }
  return rc;
}

void append_zero_value(bson::BSONObjBuilder &builder, Field *field) {
  switch (field->real_type()) {
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_LONGLONG:
    case MYSQL_TYPE_YEAR:
    case MYSQL_TYPE_BIT:
    case MYSQL_TYPE_SET: {
      builder.append(sdb_field_name(field), (int)0);
      break;
    }
    case MYSQL_TYPE_FLOAT:
    case MYSQL_TYPE_DOUBLE:
    case MYSQL_TYPE_TIME:
    case MYSQL_TYPE_TIME2: {
      builder.append(sdb_field_name(field), (double)0.0);
      break;
    }
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_BLOB: {
      if (!field->binary()) {
        builder.append(sdb_field_name(field), "");
      } else {
        builder.appendBinData(sdb_field_name(field), 0, bson::BinDataGeneral,
                              "");
      }
      break;
    }
    case MYSQL_TYPE_NEWDECIMAL:
    case MYSQL_TYPE_DECIMAL: {
      static const char *ZERO_DECIMAL = "0";
      builder.appendDecimal(sdb_field_name(field), ZERO_DECIMAL);
      break;
    }
    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_NEWDATE: {
      // '0000-00-00'
      static const bson::Date_t ZERO_DATE((longlong)-62170013143000);
      builder.appendDate(sdb_field_name(field), ZERO_DATE);
      break;
    }
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_TIMESTAMP2: {
      static const longlong ZERO_TIMESTAMP = 0;
      builder.appendTimestamp(sdb_field_name(field), ZERO_TIMESTAMP);
      break;
    }
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_DATETIME2: {
      static const char *ZERO_DATETIME = "0000-00-00 00:00:00";
      builder.append(sdb_field_name(field), ZERO_DATETIME);
      break;
    }
    case MYSQL_TYPE_ENUM: {
      static const int FIRST_ENUM = 1;
      builder.append(sdb_field_name(field), FIRST_ENUM);
      break;
    }
#ifdef IS_MYSQL
    case MYSQL_TYPE_JSON: {
      static const char *EMPTY_JSON_STR = "null";
      static const int EMPTY_JSON_STRLEN = 4;
      static String json_bin;

      if (json_bin.length() == 0) {
        const char *parse_err;
        size_t err_offset;
        Json_dom *dom = Json_dom::parse(EMPTY_JSON_STR, EMPTY_JSON_STRLEN,
                                        &parse_err, &err_offset);
        DBUG_ASSERT(dom);
        json_binary::serialize(dom, &json_bin);
        delete dom;
      }

      builder.appendBinData(sdb_field_name(field), json_bin.length(),
                            bson::BinDataGeneral, json_bin.ptr());
      break;
    }
#endif
    default: { DBUG_ASSERT(false); }
  }
}

bool get_cast_rule(bson::BSONObjBuilder &builder, Field *old_field,
                   Field *new_field) {
  int old_idx = get_type_idx(old_field->real_type());
  int new_idx = get_type_idx(new_field->real_type());
  I_build_cast_rule &func = *build_cast_funcs[old_idx][new_idx];
  return func(builder, old_field, new_field);
}

bool is_type_diff(Field *old_field, Field *new_field) {
  bool rs = true;
  if (old_field->real_type() != new_field->real_type()) {
    goto done;
  }
  /*
    Check the definition difference.
    Some types are not suitable to be checked by Field::eq_def.
    Reasons:
    1. No need to check ZEROFILL for Field_num.
    2. No need to check CHARACTER SET and COLLATE for Field_str.
    3. It doesn't check Field::binary().
    3. It doesn't check the M for Field_bit.
    4. It doesn't check the fsp for time-like types.
   */
  switch (old_field->real_type()) {
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_LONGLONG: {
      if (((Field_num *)old_field)->unsigned_flag !=
          ((Field_num *)new_field)->unsigned_flag) {
        goto done;
      }
      break;
    }
    case MYSQL_TYPE_FLOAT:
    case MYSQL_TYPE_DOUBLE: {
      Field_real *old_float = (Field_real *)old_field;
      Field_real *new_float = (Field_real *)new_field;
      if (old_float->unsigned_flag != new_float->unsigned_flag ||
          old_float->field_length != new_float->field_length ||
          old_float->decimals() != new_float->decimals()) {
        goto done;
      }
      break;
    }
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_NEWDECIMAL: {
      Field_new_decimal *old_dec = (Field_new_decimal *)old_field;
      Field_new_decimal *new_dec = (Field_new_decimal *)new_field;
      if (old_dec->unsigned_flag != new_dec->unsigned_flag ||
          old_dec->precision != new_dec->precision ||
          old_dec->decimals() != new_dec->decimals()) {
        goto done;
      }
      break;
    }
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_VAR_STRING: {
      if (old_field->char_length() != new_field->char_length() ||
          old_field->binary() != new_field->binary()) {
        goto done;
      }
      break;
    }
    case MYSQL_TYPE_BIT: {
      if (old_field->field_length != new_field->field_length) {
        goto done;
      }
      break;
    }
    case MYSQL_TYPE_TIME:
    case MYSQL_TYPE_TIME2:
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_DATETIME2:
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_TIMESTAMP2: {
      if (old_field->decimals() != new_field->decimals()) {
        goto done;
      }
      break;
    }
    default: {
      if (!old_field->eq_def(new_field)) {
        goto done;
      }
      break;
    }
  }
  rs = false;
done:
  return rs;
}

int update_null_to_notnull(Sdb_cl &cl, Field *field, longlong &modified_num) {
  int rc = 0;
  bson::BSONObj result;
  bson::BSONElement be_modified_num;

  bson::BSONObjBuilder rule_builder;
  bson::BSONObjBuilder sub_rule(rule_builder.subobjStart("$set"));
  append_zero_value(sub_rule, field);
  sub_rule.done();

  bson::BSONObjBuilder cond_builder;
  bson::BSONObjBuilder sub_cond(
      cond_builder.subobjStart(sdb_field_name(field)));
  sub_cond.append("$isnull", 1);
  sub_cond.done();

  rc = cl.update(rule_builder.obj(), cond_builder.obj(), SDB_EMPTY_BSON,
                 UPDATE_KEEP_SHARDINGKEY | UPDATE_RETURNNUM, &result);
  be_modified_num = result.getField(SDB_FIELD_MODIFIED_NUM);
  if (be_modified_num.isNumber()) {
    modified_num = be_modified_num.numberLong();
  }
  return rc;
}

int ha_sdb::alter_column(TABLE *altered_table,
                         Alter_inplace_info *ha_alter_info, Sdb_conn *conn,
                         Sdb_cl &cl) {
  static const int EMPTY_BUILDER_LEN = 8;
  static const char *EXCEED_THRESHOLD_MSG =
      "Table is too big to be altered. The records count exceeds the "
      "sequoiadb_alter_table_overhead_threshold.";
  static const char *DROP_AUTOINC_MSG =
      "Failed to drop the auto-increment field of cl[%s.%s]";
  static const char *ADD_AUTOINC_MSG =
      "Failed to add the auto-increment field of cl[%s.%s]";

  int rc = 0;
  int tmp_rc = 0;
  THD *thd = ha_thd();
  const HA_CREATE_INFO *create_info = ha_alter_info->create_info;
  Sdb_alter_ctx *ctx = (Sdb_alter_ctx *)ha_alter_info->handler_ctx;
  List<Col_alter_info> &changed_columns = ctx->changed_columns;
  longlong count = 0;

  bson::BSONObjBuilder unset_builder;
  List_iterator_fast<Field> dropped_it;
  Field *field = NULL;

  bson::BSONObjBuilder set_builder;
  // bson::BSONObjBuilder inc_builder;
  List_iterator_fast<Field> added_it;

  bson::BSONObjBuilder cast_builder;
  List_iterator_fast<Col_alter_info> changed_it;
  Col_alter_info *info = NULL;

  bson::BSONObjBuilder builder;

  rc = cl.get_count(count);
  if (0 != rc) {
    my_error(ER_GET_ERRNO, MYF(0), rc);
    goto error;
  }

  // 1.Handle the dropped_columns
  dropped_it.init(ctx->dropped_columns);
  while ((field = dropped_it++)) {
    unset_builder.append(sdb_field_name(field), "");
  }

  // 2.Handle the added_columns
  added_it.init(ctx->added_columns);
  while ((field = added_it++)) {
    my_ptrdiff_t offset = field->table->default_values_offset();
    if (!field->is_real_null(offset) &&
        !(field->flags & NO_DEFAULT_VALUE_FLAG)) {
      rc = append_default_value(set_builder, field);
      if (rc != 0) {
        rc = ER_WRONG_ARGUMENTS;
        my_printf_error(rc, ER(rc), MYF(0), sdb_field_name(field));
        goto error;
      }
    } else if (!field->maybe_null()) {
      if (!(field->flags & AUTO_INCREMENT_FLAG)) {
        append_zero_value(set_builder, field);
      } else {
        // inc_builder.append(sdb_field_name(field), get_inc_option(option));
      }
    }
  }

  // 3.Handle the changed_columns
  changed_it.init(changed_columns);
  while ((info = changed_it++)) {
    if (info->op_flag & Col_alter_info::CHANGE_DATA_TYPE &&
        !info->cast_rule.isEmpty()) {
      cast_builder.appendElements(info->cast_rule);
    }

    if (info->op_flag & Col_alter_info::TURN_TO_NOT_NULL) {
      const char *field_name = sdb_field_name(info->after);
      longlong modified_num = 0;
      if (count > sdb_alter_table_overhead_threshold(thd)) {
        rc = HA_ERR_WRONG_COMMAND;
        my_printf_error(rc, "%s", MYF(0), EXCEED_THRESHOLD_MSG);
        goto error;
      }
      if (!conn->is_transaction_on()) {
        rc = conn->begin_transaction();
        if (rc != 0) {
          my_printf_error(rc, "Failed to begin transaction", MYF(0));
        }
      }
      rc = update_null_to_notnull(cl, info->before, modified_num);
      if (rc != 0) {
        my_printf_error(rc, "Failed to update column[%s] on cl[%s.%s]", MYF(0),
                        field_name, db_name, table_name);
        goto error;
      }
      if (modified_num > 0) {
        if (is_strict_mode(ha_thd()->variables.sql_mode)) {
          my_error(ER_INVALID_USE_OF_NULL, MYF(0));
          rc = ER_INVALID_USE_OF_NULL;
          goto error;
        } else {
          for (longlong i = 1; i <= modified_num; ++i) {
            push_warning_printf(thd, Sql_condition::SL_WARNING,
                                ER_WARN_NULL_TO_NOTNULL,
                                ER(ER_WARN_NULL_TO_NOTNULL), field_name, i);
          }
        }
      }
    }
  }

  // 4.Full table update
  if (unset_builder.len() > EMPTY_BUILDER_LEN) {
    builder.append("$unset", unset_builder.obj());
  }
  if (set_builder.len() > EMPTY_BUILDER_LEN) {
    builder.append("$set", set_builder.obj());
  }
  /*if (inc_builder.len() > EMPTY_BUILDER_LEN) {
    builder.append("$inc", inc_builder.obj());
  }
  if (cast_builder.len() > EMPTY_BUILDER_LEN) {
    builder.append("$cast", cast_builder.obj());
  }*/

  if (builder.len() > EMPTY_BUILDER_LEN) {
    if (count > sdb_alter_table_overhead_threshold(thd)) {
      rc = HA_ERR_WRONG_COMMAND;
      my_printf_error(rc, "%s", MYF(0), EXCEED_THRESHOLD_MSG);
      goto error;
    }
    if (!conn->is_transaction_on()) {
      rc = conn->begin_transaction();
      if (rc != 0) {
        my_printf_error(rc, "Failed to begin transaction", MYF(0));
      }
    }
    rc = cl.update(builder.obj(), SDB_EMPTY_BSON, SDB_EMPTY_BSON,
                   UPDATE_KEEP_SHARDINGKEY);
    if (rc != 0) {
      my_printf_error(rc, "Failed to update table[%s.%s]", MYF(0), db_name,
                      table_name);
      goto error;
    }
  }

  if (conn->is_transaction_on()) {
    rc = conn->commit_transaction();
    if (rc != 0) {
      my_printf_error(rc, "Failed to commit update transaction.", MYF(0));
      goto error;
    }
  }

  /*if (ha_alter_info->handler_flags &
       ALTER_STORED_COLUMN_TYPE) {
    bson::BSONObj result;
    rc = conn->get_last_result_obj(result);
    if (rc != 0) {
      goto error;
    }
    if (has_warnings) {
      if (is_strict_mode(ha_thd()->variables.sql_mode)) {
        //push_warnings...
      } else {
        //print and return error
      }
    }
  }*/

  // 5.Create and drop auto-increment.
  dropped_it.rewind();
  while ((field = dropped_it++)) {
    if (field->flags & AUTO_INCREMENT_FLAG) {
      rc = cl.drop_auto_increment(sdb_field_name(field));
      if (0 != rc) {
        my_printf_error(rc, DROP_AUTOINC_MSG, MYF(0), db_name, table_name);
        goto error;
      }
    }
  }

  added_it.rewind();
  while ((field = added_it++)) {
    if (field->flags & AUTO_INCREMENT_FLAG) {
      bson::BSONObj option;
      build_auto_inc_option(field, create_info, option);
      rc = cl.create_auto_increment(option);
      if (0 != rc) {
        my_printf_error(rc, ADD_AUTOINC_MSG, MYF(0), db_name, table_name);
        goto error;
      }
    }
  }

  changed_it.rewind();
  while ((info = changed_it++)) {
    if (info->op_flag & Col_alter_info::DROP_AUTO_INC) {
      rc = cl.drop_auto_increment(sdb_field_name(info->before));
      if (0 != rc) {
        my_printf_error(rc, DROP_AUTOINC_MSG, MYF(0), db_name, table_name);
        goto error;
      }
    }

    if (info->op_flag & Col_alter_info::ADD_AUTO_INC) {
      bson::BSONObj option;
      build_auto_inc_option(info->after, create_info, option);
      rc = cl.create_auto_increment(option);
      if (0 != rc) {
        my_printf_error(rc, ADD_AUTOINC_MSG, MYF(0), db_name, table_name);
        goto error;
      }
    }
  }

done:
  return rc;
error:
  if (conn->is_transaction_on()) {
    tmp_rc = conn->rollback_transaction();
    if (tmp_rc != 0) {
      SDB_LOG_WARNING(
          "Failed to rollback the transaction of inplace alteration of "
          "table[%s.%s], rc: %d",
          db_name, table_name, tmp_rc);
    }
  }
  goto done;
}

int create_index(Sdb_cl &cl, Alter_inplace_info *ha_alter_info,
                 Bitmap<MAX_INDEXES> &ignored_keys) {
  int rc = 0;
  const KEY *key_info = NULL;
  uint key_nr = 0;

  for (uint i = 0; i < ha_alter_info->index_add_count; i++) {
    if (ignored_keys.is_set(i)) {
      continue;
    }

    key_nr = ha_alter_info->index_add_buffer[i];
    key_info = &ha_alter_info->key_info_buffer[key_nr];
    rc = sdb_create_index(key_info, cl);
    if (rc) {
      goto error;
    }
  }
done:
  return rc;
error:
  goto done;
}

int drop_index(Sdb_cl &cl, Alter_inplace_info *ha_alter_info,
               Bitmap<MAX_INDEXES> &ignored_keys) {
  int rc = 0;

  if (NULL == ha_alter_info->index_drop_buffer) {
    goto done;
  }

  for (uint i = 0; i < ha_alter_info->index_drop_count; i++) {
    if (ignored_keys.is_set(i)) {
      continue;
    }

    KEY *key_info = ha_alter_info->index_drop_buffer[i];
    rc = cl.drop_index(sdb_key_name(key_info));
    if (rc) {
      goto error;
    }
  }
done:
  return rc;
error:
  goto done;
}

enum_alter_inplace_result ha_sdb::check_if_supported_inplace_alter(
    TABLE *altered_table, Alter_inplace_info *ha_alter_info) {
  enum_alter_inplace_result rs;
  List_iterator_fast<Create_field> cf_it;
  Bitmap<MAX_FIELDS> matched_map;
  Sdb_alter_ctx *ctx = NULL;
  KEY *new_key = NULL;
  KEY_PART_INFO *key_part = NULL;
  sql_mode_t sql_mode = ha_thd()->variables.sql_mode;

  DBUG_ASSERT(!ha_alter_info->handler_ctx);

  SDB_EXECUTE_ONLY_IN_MYSQL_RETURN(ha_thd(), rs,
                                   HA_ALTER_INPLACE_NOCOPY_NO_LOCK);

  if (ha_alter_info->handler_flags & ~INPLACE_ONLINE_OPERATIONS) {
    rs = HA_ALTER_INPLACE_NOT_SUPPORTED;
    goto error;
  }

  ctx = new Sdb_alter_ctx();
  if (!ctx) {
    rs = HA_ALTER_INPLACE_NOT_SUPPORTED;
    goto error;
  }
  ha_alter_info->handler_ctx = ctx;

  // Filter added_columns, dropped_columns and changed_columns
  for (uint i = 0; table->field[i]; i++) {
    Field *old_field = table->field[i];
    bool found_col = false;
    for (uint j = 0; altered_table->field[j]; j++) {
      bson::BSONObjBuilder cast_builder;
      Field *new_field = altered_table->field[j];
      if (!matched_map.is_set(j) &&
          strcmp(sdb_field_name(old_field), sdb_field_name(new_field)) == 0) {
        matched_map.set_bit(j);
        found_col = true;

        int op_flag = 0;
        if (is_type_diff(old_field, new_field)) {
          if (get_cast_rule(cast_builder, old_field, new_field)) {
            ha_alter_info->unsupported_reason =
                "Can't do such type conversion.";
            rs = HA_ALTER_INPLACE_NOT_SUPPORTED;
            goto error;
          }
          op_flag |= Col_alter_info::CHANGE_DATA_TYPE;
        }

        bool old_is_auto_inc = (old_field->flags & AUTO_INCREMENT_FLAG);
        bool new_is_auto_inc = (new_field->flags & AUTO_INCREMENT_FLAG);
        if (!old_is_auto_inc && new_is_auto_inc) {
          op_flag |= Col_alter_info::ADD_AUTO_INC;
        } else if (old_is_auto_inc && !new_is_auto_inc) {
          op_flag |= Col_alter_info::DROP_AUTO_INC;
        }
        // Temporarily unsupported for SEQUOIADBMAINSTREAM-4889.
        if (op_flag & Col_alter_info::ADD_AUTO_INC) {
          rs = HA_ALTER_INPLACE_NOT_SUPPORTED;
          goto error;
        }

        if (!new_field->maybe_null() && old_field->maybe_null()) {
          // Avoid ZERO DATE when sql_mode doesn't allow
          if (is_temporal_type_with_date(new_field->type()) &&
              sql_mode & MODE_NO_ZERO_DATE) {
            rs = HA_ALTER_INPLACE_NOT_SUPPORTED;
            goto error;
          }
          op_flag |= Col_alter_info::TURN_TO_NOT_NULL;
        }

        if (op_flag) {
          Col_alter_info *info = new Col_alter_info();
          info->before = old_field;
          info->after = new_field;
          info->op_flag = op_flag;
          info->cast_rule = cast_builder.obj();
          ctx->changed_columns.push_back(info);
        }
        break;
      }
    }
    if (!found_col) {
      ctx->dropped_columns.push_back(old_field);
    }
  }

  for (uint i = 0; altered_table->field[i]; i++) {
    if (!matched_map.is_set(i)) {
      Field *field = altered_table->field[i];
      // Avoid DEFAULT CURRENT_TIMESTAMP
      if (sdb_is_current_timestamp(field)) {
        ha_alter_info->unsupported_reason =
            "DEFAULT CURRENT_TIMESTAMP is unsupported.";
        rs = HA_ALTER_INPLACE_NOT_SUPPORTED;
        goto error;
      }
      // Avoid ZERO DATE when sql_mode doesn't allow
      if (is_temporal_type_with_date(field->type()) &&
          !(field->flags & NO_DEFAULT_VALUE_FLAG)) {
        MYSQL_TIME ltime;
        int warnings = 0;
        date_mode_t flags = TIME_FUZZY_DATES | TIME_INVALID_DATES |
                            sdb_thd_time_round_mode(ha_thd());
        if (sql_mode & MODE_NO_ZERO_DATE) {
          flags |= TIME_NO_ZERO_DATE;
        }
        if (sql_mode & MODE_NO_ZERO_IN_DATE) {
          flags |= TIME_NO_ZERO_IN_DATE;
        }

        if (field->get_date(&ltime, flags) ||
            check_date(&ltime, non_zero_date(&ltime), (ulonglong)flags,
                       &warnings) ||
            warnings) {
          rs = HA_ALTER_INPLACE_NOT_SUPPORTED;
          goto error;
        }
      }
      // Temporarily unsupported for SEQUOIADBMAINSTREAM-4889.
      if (field->flags & AUTO_INCREMENT_FLAG) {
        rs = HA_ALTER_INPLACE_NOT_SUPPORTED;
        goto error;
      }
      ctx->added_columns.push_back(field);
    }
  }

  cf_it.init(ha_alter_info->alter_info->create_list);
  for (new_key = ha_alter_info->key_info_buffer;
       new_key < ha_alter_info->key_info_buffer + ha_alter_info->key_count;
       new_key++) {
    /* Fix the key parts. */
    for (key_part = new_key->key_part;
         key_part < new_key->key_part + new_key->user_defined_key_parts;
         key_part++) {
      const Create_field *new_field;
      DBUG_ASSERT(key_part->fieldnr < altered_table->s->fields);
      cf_it.rewind();
      for (uint fieldnr = 0; (new_field = cf_it++); fieldnr++) {
        if (fieldnr == key_part->fieldnr) {
          break;
        }
      }
      DBUG_ASSERT(new_field);
      key_part->field = altered_table->field[key_part->fieldnr];

      key_part->null_offset = key_part->field->null_offset();
      key_part->null_bit = key_part->field->null_bit;

      if (new_field->field) {
        continue;
      }
    }
  }

  rs = HA_ALTER_INPLACE_NOCOPY_NO_LOCK;
done:
  return rs;
error:
  if (ctx) {
    ctx->changed_columns.delete_elements();
    delete ctx;
    ha_alter_info->handler_ctx = NULL;
  }
  goto done;
}

bool ha_sdb::prepare_inplace_alter_table(TABLE *altered_table,
                                         Alter_inplace_info *ha_alter_info) {
  return false;
}

bool ha_sdb::inplace_alter_table(TABLE *altered_table,
                                 Alter_inplace_info *ha_alter_info) {
  DBUG_ENTER("sdb_inplace_alter_table()");
  bool rs = true;
  int rc = 0;
  THD *thd = current_thd;
  Sdb_conn *conn = NULL;
  Sdb_cl cl;
  Bitmap<MAX_INDEXES> ignored_drop_keys;
  Bitmap<MAX_INDEXES> ignored_add_keys;
  Sdb_alter_ctx *ctx = (Sdb_alter_ctx *)ha_alter_info->handler_ctx;

  SDB_EXECUTE_ONLY_IN_MYSQL_DBUG_RETURN(ha_thd(), rs, false);

  const HA_CREATE_INFO *create_info = ha_alter_info->create_info;
  const alter_table_operations alter_flags = ha_alter_info->handler_flags;

  DBUG_ASSERT(ha_alter_info->handler_flags | INPLACE_ONLINE_OPERATIONS);

  conn = check_sdb_in_thd(thd, true);
  if (NULL == conn) {
    rc = HA_ERR_NO_CONNECTION;
    goto error;
  }

  rc = conn->get_cl(db_name, table_name, cl);
  if (0 != rc) {
    SDB_LOG_ERROR("Collection[%s.%s] is not available. rc: %d", db_name,
                  table_name, rc);
    goto error;
  }

  if (alter_flags & ALTER_CHANGE_CREATE_OPTION) {
    const char *old_comment = table->s->comment.str;
    const char *new_comment = create_info->comment.str;
    bson::BSONObj option;
    if (!(old_comment == new_comment ||
          strcmp(old_comment, new_comment) == 0)) {
      my_error(HA_ERR_WRONG_COMMAND, MYF(0));
      goto error;
    }

    if (create_info->used_fields & HA_CREATE_USED_AUTO &&
        table->found_next_number_field &&
        (create_info->auto_increment_value >
         table->file->stats.auto_increment_value)) {
      bson::BSONObjBuilder builder;
      bson::BSONObjBuilder sub_builder(
          builder.subobjStart(SDB_FIELD_NAME_AUTOINCREMENT));
      sub_builder.append(SDB_FIELD_NAME_FIELD,
                         sdb_field_name(table->found_next_number_field));
      longlong current_value = create_info->auto_increment_value -
                               thd->variables.auto_increment_increment;
      if (current_value < 1) {
        current_value = 1;
      }
      sub_builder.append(SDB_FIELD_CURRENT_VALUE, current_value);
      sub_builder.done();

      rc = cl.set_attributes(builder.obj());
      if (0 != rc) {
        my_printf_error(rc,
                        "Failed to alter auto_increment option "
                        "on cl[%s.%s]",
                        MYF(0), db_name, table_name);
        goto error;
      }
    }
  }

  // If it's a redefinition of the secondary attributes, such as btree/hash
  // and comment, don't recreate the index.
  if (alter_flags & INPLACE_ONLINE_DROPIDX &&
      alter_flags & INPLACE_ONLINE_ADDIDX) {
    for (uint i = 0; i < ha_alter_info->index_drop_count; i++) {
      KEY *drop_key = ha_alter_info->index_drop_buffer[i];
      for (uint j = 0; j < ha_alter_info->index_add_count; j++) {
        uint key_nr = ha_alter_info->index_add_buffer[j];
        KEY *add_key = &ha_alter_info->key_info_buffer[key_nr];
        if (sdb_is_same_index(drop_key, add_key)) {
          ignored_drop_keys.set_bit(i);
          ignored_add_keys.set_bit(j);
        }
      }
    }
  }

  if (alter_flags & ALTER_RENAME_INDEX) {
    my_error(HA_ERR_UNSUPPORTED, MYF(0), cl.get_cl_name());
    goto error;
  }

  if (alter_flags & INPLACE_ONLINE_DROPIDX) {
    rc = drop_index(cl, ha_alter_info, ignored_drop_keys);
    if (0 != rc) {
      my_error(ER_GET_ERRNO, MYF(0), rc);
      goto error;
    }
  }

  if (alter_flags & (ALTER_DROP_STORED_COLUMN | ALTER_ADD_STORED_BASE_COLUMN |
                     ALTER_STORED_COLUMN_TYPE | ALTER_COLUMN_DEFAULT)) {
    rc = alter_column(altered_table, ha_alter_info, conn, cl);
    if (0 != rc) {
      goto error;
    }
  }

  if (alter_flags & INPLACE_ONLINE_ADDIDX) {
    rc = create_index(cl, ha_alter_info, ignored_add_keys);
    if (0 != rc) {
      print_error(rc, MYF(0));
      goto error;
    }
  }

  rs = false;

done:
  if (ctx) {
    ctx->changed_columns.delete_elements();
    delete ctx;
    ha_alter_info->handler_ctx = NULL;
  }
  DBUG_RETURN(rs);
error:
  goto done;
}
