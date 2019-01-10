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

#include <sql_time.h>
#include <my_dbug.h>
#include "sdb_item.h"
#include "sdb_errcode.h"
#include "sdb_util.h"
#include "sdb_def.h"

#define BSON_APPEND(field_name, value, obj, arr_builder) \
  do {                                                   \
    if (NULL == (arr_builder)) {                         \
      (obj) = BSON((field_name) << (value));             \
    } else {                                             \
      (arr_builder)->append(value);                      \
    }                                                    \
  } while (0)

static const uint MAX_TIME_DEC = 6;
static const uint POWER_10[7] = {1, 10, 100, 1000, 10000, 100000, 1000000};

// This function is similar to Item::get_date_from_string() but without warning.
static bool get_date_from_item_string(Item *item, MYSQL_TIME *ltime,
                                      my_time_flags_t flags) {
  char buff[MAX_DATE_STRING_REP_LENGTH];
  MYSQL_TIME_STATUS status;
  THD *thd = current_thd;
  String tmp(buff, sizeof(buff), &my_charset_bin), *res;
  Dummy_error_handler error_handler;  // ignore all error and warning states
  bool ret = true;

  DBUG_ASSERT(NULL != item);
  DBUG_ASSERT(NULL != ltime);

  thd->push_internal_handler(&error_handler);

  if (!(res = item->val_str(&tmp))) {
    set_zero_time(ltime, MYSQL_TIMESTAMP_DATETIME);
    ret = true;
    goto done;
  }

  if (thd->variables.sql_mode & MODE_NO_ZERO_DATE) {
    flags |= TIME_NO_ZERO_DATE;
  }
  if (thd->variables.sql_mode & MODE_INVALID_DATES) {
    flags |= TIME_INVALID_DATES;
  }

  ret = str_to_datetime(res, ltime, flags, &status);

done:
  thd->pop_internal_handler();
  return ret;
}

// This function is similar to Item::get_time_from_string() but without warning.
static bool get_time_from_item_string(Item *item, MYSQL_TIME *ltime) {
  char buff[MAX_DATE_STRING_REP_LENGTH];
  MYSQL_TIME_STATUS status;
  THD *thd = current_thd;
  String tmp(buff, sizeof(buff), &my_charset_bin), *res;
  Dummy_error_handler error_handler;  // ignore all error and warning states
  bool ret = true;

  DBUG_ASSERT(NULL != item);
  DBUG_ASSERT(NULL != ltime);

  thd->push_internal_handler(&error_handler);

  if (!(res = item->val_str(&tmp))) {
    set_zero_time(ltime, MYSQL_TIMESTAMP_TIME);
    ret = true;
    goto done;
  }

  ret = str_to_time(res, ltime, 0, &status);

done:
  thd->pop_internal_handler();
  return ret;
}

// This function is similar to Item::get_timeval() but without warning.
static bool get_timeval(Item *item, struct timeval *tm) {
  MYSQL_TIME ltime;
  int warnings = 0;

  if (get_date_from_item_string(item, &ltime, TIME_FUZZY_DATE)) {
    goto error; /* Could not extract date from the value */
  }

  if (datetime_to_timeval(current_thd, &ltime, tm, &warnings)) {
    goto error; /* Value is out of the supported range */
  }

  return false; /* Value is a good Unix timestamp */

error:
  tm->tv_sec = tm->tv_usec = 0;
  return true;
}

int Sdb_logic_item::push(Sdb_item *cond_item) {
  int rc = 0;
  bson::BSONObj obj_tmp;

  if (is_finished) {
    // there must be something wrong,
    // skip all condition
    rc = SDB_ERR_COND_UNEXPECTED_ITEM;
    is_ok = FALSE;
    goto error;
  }

  rc = cond_item->to_bson(obj_tmp);
  if (rc != 0) {
    // skip the error and go on to parse the condition-item
    // the error will return in to_bson() ;
    rc = SDB_ERR_COND_PART_UNSUPPORTED;
    is_ok = FALSE;
    goto error;
  }
  delete cond_item;
  children.append(obj_tmp);

done:
  return rc;
error:
  goto done;
}

int Sdb_logic_item::push(Item *cond_item) {
  if (NULL != cond_item) {
    return SDB_ERR_COND_UNEXPECTED_ITEM;
  }
  is_finished = TRUE;
  return SDB_ERR_OK;
}

int Sdb_logic_item::to_bson(bson::BSONObj &obj) {
  int rc = SDB_ERR_OK;
  if (is_ok) {
    obj = BSON(this->name() << children.arr());
  } else {
    rc = SDB_ERR_COND_INCOMPLETED;
  }
  return rc;
}

int Sdb_and_item::to_bson(bson::BSONObj &obj) {
  obj = BSON(this->name() << children.arr());
  return SDB_ERR_OK;
}

Sdb_func_item::Sdb_func_item() : para_num_cur(0), para_num_max(1) {
  l_child = NULL;
  r_child = NULL;
}

Sdb_func_item::~Sdb_func_item() {
  para_list.pop();
  if (l_child != NULL) {
    delete l_child;
    l_child = NULL;
  }
  if (r_child != NULL) {
    delete r_child;
    r_child = NULL;
  }
}

void Sdb_func_item::update_stat() {
  if (++para_num_cur >= para_num_max) {
    is_finished = TRUE;
  }
}

int Sdb_func_item::push(Sdb_item *cond_item) {
  int rc = SDB_ERR_OK;
  if (cond_item->type() != Item_func::UNKNOWN_FUNC) {
    rc = SDB_ERR_COND_UNEXPECTED_ITEM;
    goto error;
  }

  if (((Sdb_func_unkown *)cond_item)->get_func_item()->const_item()) {
    rc = push(((Sdb_func_unkown *)cond_item)->get_func_item());
    if (rc != SDB_ERR_OK) {
      goto error;
    }
    delete cond_item;
  } else {
    if (l_child != NULL || r_child != NULL) {
      rc = SDB_ERR_COND_UNEXPECTED_ITEM;
      goto error;
    }
    if (para_num_cur != 0) {
      r_child = cond_item;
    } else {
      l_child = cond_item;
    }
    update_stat();
  }
done:
  return rc;
error:
  goto done;
}

int Sdb_func_item::push(Item *cond_item) {
  int rc = SDB_ERR_OK;

  if (is_finished) {
    goto error;
  }
  para_list.push_back(cond_item);
  update_stat();
done:
  return rc;
error:
  rc = SDB_ERR_COND_UNSUPPORTED;
  goto done;
}

int Sdb_func_item::pop(Item *&para_item) {
  if (para_list.is_empty()) {
    return SDB_ERR_EOF;
  }
  para_item = para_list.pop();
  return SDB_ERR_OK;
}

int Sdb_func_item::get_item_val(const char *field_name, Item *item_val,
                                Field *field, bson::BSONObj &obj,
                                bson::BSONArrayBuilder *arr_builder) {
  int rc = SDB_ERR_OK;

  if (NULL == item_val || !item_val->const_item() ||
      (Item::FUNC_ITEM == item_val->type() &&
       (((Item_func *)item_val)->functype() == Item_func::FUNC_SP ||
        ((Item_func *)item_val)->functype() == Item_func::TRIG_COND_FUNC))) {
    // don't push down the triggered conditions or the func will be
    // triggered in push down one more time
    rc = SDB_ERR_COND_UNEXPECTED_ITEM;
    goto error;
  }

  if (Item::NULL_ITEM == item_val->type()) {
    // "$exists" appear in array is not support now
    if (NULL == arr_builder) {
      if (type() == Item_func::EQ_FUNC) {
        obj = BSON("$exists" << 0);
        goto done;
      } else if (type() == Item_func::NE_FUNC) {
        obj = BSON("$exists" << 1);
        goto done;
      } else if (type() == Item_func::EQUAL_FUNC) {
        obj = BSON("$isnull" << 1);
        goto done;
      }
    }
    rc = SDB_ERR_OVF;
    goto error;
  }

  switch (field->type()) {
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_LONGLONG:
    case MYSQL_TYPE_FLOAT:
    case MYSQL_TYPE_DOUBLE:
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_NEWDECIMAL: {
      switch (item_val->result_type()) {
        case INT_RESULT: {
          longlong val_tmp = item_val->val_int();
          if (val_tmp < 0 && item_val->unsigned_flag) {
            bson::bsonDecimal decimal;
            my_decimal dec_tmp;
            char buff[MAX_FIELD_WIDTH] = {0};
            String str(buff, sizeof(buff), item_val->charset_for_protocol());
            item_val->val_decimal(&dec_tmp);
            my_decimal2string(E_DEC_FATAL_ERROR, &dec_tmp, 0, 0, 0, &str);

            rc = decimal.fromString(str.c_ptr());
            if (0 != rc) {
              rc = SDB_ERR_INVALID_ARG;
              goto error;
            }

            BSON_APPEND(field_name, decimal, obj, arr_builder);
          } else {
            BSON_APPEND(field_name, val_tmp, obj, arr_builder);
          }
          break;
        }
        case REAL_RESULT: {
          BSON_APPEND(field_name, item_val->val_real(), obj, arr_builder);
          break;
        }
        case STRING_RESULT: {
          if (NULL != arr_builder) {
            // SEQUOIADBMAINSTREAM-3365
            // the string value is not support for "in"
            rc = SDB_ERR_TYPE_UNSUPPORTED;
            goto error;
          }
          // pass through
        }
        case DECIMAL_RESULT: {
          // ignore all error and warning states
          Dummy_error_handler error_handler;

          if (MYSQL_TYPE_FLOAT == field->type()) {
            THD *thd = current_thd;
            thd->push_internal_handler(&error_handler);
            float value = (float)item_val->val_real();
            thd->pop_internal_handler();
            BSON_APPEND(field_name, value, obj, arr_builder);
          } else if (MYSQL_TYPE_DOUBLE == field->type()) {
            THD *thd = current_thd;
            thd->push_internal_handler(&error_handler);
            double value = item_val->val_real();
            thd->pop_internal_handler();
            BSON_APPEND(field_name, value, obj, arr_builder);
          } else {
            bson::bsonDecimal decimal;
            char buff[MAX_FIELD_WIDTH] = {0};
            String str(buff, sizeof(buff), item_val->charset_for_protocol());
            String conv_str;
            String *pStr;
            pStr = item_val->val_str(&str);
            if (NULL == pStr) {
              rc = SDB_ERR_INVALID_ARG;
              goto error;
            }
            if (!my_charset_same(pStr->charset(), &SDB_CHARSET)) {
              rc = sdb_convert_charset(*pStr, conv_str, &SDB_CHARSET);
              if (rc) {
                goto error;
              }
              pStr = &conv_str;
            }

            rc = decimal.fromString(pStr->c_ptr());
            if (0 != rc) {
              rc = SDB_ERR_INVALID_ARG;
              goto error;
            }

            BSON_APPEND(field_name, decimal, obj, arr_builder);
          }
          break;
        }
        default: {
          rc = SDB_ERR_COND_UNEXPECTED_ITEM;
          goto error;
        }
      }
      break;
    }

    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_BLOB: {
      if (item_val->result_type() == STRING_RESULT && !field->binary()) {
        String *pStr = NULL;
        String conv_str;
        char buff[MAX_FIELD_WIDTH] = {0};
        String str(buff, sizeof(buff), item_val->charset_for_protocol());

        pStr = item_val->val_str(&str);
        if (NULL == pStr) {
          rc = SDB_ERR_INVALID_ARG;
          break;
        }

        if (!my_charset_same(pStr->charset(), &my_charset_bin)) {
          if (!my_charset_same(pStr->charset(), &SDB_CHARSET)) {
            rc = sdb_convert_charset(*pStr, conv_str, &SDB_CHARSET);
            if (rc) {
              break;
            }
            pStr = &conv_str;
          }

          if (MYSQL_TYPE_STRING == field->type() ||
              MYSQL_TYPE_VAR_STRING == field->type()) {
            // Trailing space of CHAR/ENUM/SET condition should be stripped.
            pStr->strip_sp();
          }
        }

        if (NULL == arr_builder) {
          bson::BSONObjBuilder obj_builder;
          obj_builder.appendStrWithNoTerminating(field_name, pStr->ptr(),
                                                 pStr->length());
          obj = obj_builder.obj();
        } else {
          arr_builder->append(pStr->c_ptr());
        }
      } else {
        rc = SDB_ERR_COND_UNEXPECTED_ITEM;
        goto error;
      }
      break;
    }

    case MYSQL_TYPE_DATE: {
      MYSQL_TIME ltime;
      if (STRING_RESULT == item_val->result_type() &&
          !get_date_from_item_string(item_val, &ltime, TIME_FUZZY_DATE)) {
        struct tm tm_val;
        tm_val.tm_sec = ltime.second;
        tm_val.tm_min = ltime.minute;
        tm_val.tm_hour = ltime.hour;
        tm_val.tm_mday = ltime.day;
        tm_val.tm_mon = ltime.month - 1;
        tm_val.tm_year = ltime.year - 1900;
        tm_val.tm_wday = 0;
        tm_val.tm_yday = 0;
        tm_val.tm_isdst = 0;
        time_t time_tmp = mktime(&tm_val);
        bson::Date_t dt((longlong)(time_tmp * 1000));
        BSON_APPEND(field_name, dt, obj, arr_builder);
      } else {
        rc = SDB_ERR_COND_UNEXPECTED_ITEM;
        goto error;
      }
      break;
    }

    case MYSQL_TYPE_TIMESTAMP: {
      struct timeval tm;
      if (item_val->result_type() != STRING_RESULT ||
          get_timeval(item_val, &tm)) {
        rc = SDB_ERR_COND_UNEXPECTED_ITEM;
        goto error;
      } else {
        uint dec = field->decimals();
        if (dec < 6) {
          uint power = POWER_10[MAX_TIME_DEC - dec];
          tm.tv_usec = (tm.tv_usec / power) * power;
        }
        bson::OpTime t(tm.tv_sec, tm.tv_usec);
        long long time_val = t.asDate();
        if (NULL == arr_builder) {
          bson::BSONObjBuilder obj_builder;
          obj_builder.appendTimestamp(field_name, time_val);
          obj = obj_builder.obj();
        } else {
          arr_builder->appendTimestamp(time_val);
        }
      }
      break;
    }

    case MYSQL_TYPE_DATETIME: {
      MYSQL_TIME ltime;
      if (item_val->result_type() != STRING_RESULT ||
          get_date_from_item_string(item_val, &ltime, TIME_FUZZY_DATE) ||
          ltime.year > 9999 || ltime.year < 1000) {
        rc = SDB_ERR_COND_UNEXPECTED_ITEM;
        goto error;
      } else {
        uint dec = field->decimals();
        char buff[MAX_FIELD_WIDTH];
        int len = sprintf(buff, "%04u-%02u-%02u %s%02u:%02u:%02u", ltime.year,
                          ltime.month, ltime.day, (ltime.neg ? "-" : ""),
                          ltime.hour, ltime.minute, ltime.second);
        if (dec) {
          len += sprintf(buff + len, ".%0*lu", (int)dec, ltime.second_part);
        }

        BSON_APPEND(field_name, buff, obj, arr_builder);
      }
      break;
    }

    case MYSQL_TYPE_TIME: {
      MYSQL_TIME ltime;
      if (STRING_RESULT == item_val->result_type() &&
          !get_time_from_item_string(item_val, &ltime)) {
        uint dec = field->decimals();
        double time = ltime.hour;
        time = time * 100 + ltime.minute;
        time = time * 100 + ltime.second;
        if (ltime.second_part && dec > 0) {
          ulong second_part = ltime.second_part;
          if (dec < 6) {
            uint power = POWER_10[MAX_TIME_DEC - dec];
            second_part = (second_part / power) * power;
          }
          double ms = second_part / (double)1000000;
          time += ms;
        }
        if (ltime.neg) {
          time = -time;
        }

        BSON_APPEND(field_name, time, obj, arr_builder);
      } else {
        rc = SDB_ERR_COND_UNEXPECTED_ITEM;
        goto error;
      }
      break;
    }

    case MYSQL_TYPE_YEAR: {
      if (INT_RESULT == item_val->result_type()) {
        longlong value = item_val->val_int();
        if (value > 0) {
          if (value < YY_PART_YEAR) {
            value += 2000;  // 2000 - 2069
          } else if (value < 100) {
            value += 1900;  // 1970 - 2000
          }
        }
        BSON_APPEND(field_name, value, obj, arr_builder);
      } else {
        rc = SDB_ERR_COND_UNEXPECTED_ITEM;
        goto error;
      }
      break;
    }

    case MYSQL_TYPE_BIT: {
      if (INT_RESULT == item_val->result_type()) {
        longlong value = item_val->val_int();
        BSON_APPEND(field_name, value, obj, arr_builder);
      } else {
        rc = SDB_ERR_COND_UNEXPECTED_ITEM;
        goto error;
      }
      break;
    }

    case MYSQL_TYPE_NULL:
    case MYSQL_TYPE_JSON:
    case MYSQL_TYPE_GEOMETRY:
    default: {
      rc = SDB_ERR_TYPE_UNSUPPORTED;
      goto error;
    }
  }

done:
  return rc;
error:
  goto done;
}

Sdb_func_unkown::Sdb_func_unkown(Item_func *item) {
  func_item = item;
  para_num_max = item->argument_count();
  if (0 == para_num_max) {
    is_finished = TRUE;
  }
}

Sdb_func_unkown::~Sdb_func_unkown() {}

Sdb_func_unary_op::Sdb_func_unary_op() {
  para_num_max = 1;
}

Sdb_func_unary_op::~Sdb_func_unary_op() {}

Sdb_func_isnull::Sdb_func_isnull() {}

Sdb_func_isnull::~Sdb_func_isnull() {}

int Sdb_func_isnull::to_bson(bson::BSONObj &obj) {
  int rc = SDB_ERR_OK;
  Item *item_tmp = NULL;

  if (!is_finished || para_list.elements != para_num_max) {
    rc = SDB_ERR_COND_INCOMPLETED;
    goto error;
  }

  if (l_child != NULL || r_child != NULL) {
    rc = SDB_ERR_COND_UNKOWN_ITEM;
    goto error;
  }

  item_tmp = para_list.pop();
  if (Item::FIELD_ITEM != item_tmp->type()) {
    rc = SDB_ERR_COND_UNKOWN_ITEM;
    goto error;
  }
  obj = BSON(((Item_field *)item_tmp)->field_name << BSON(this->name() << 1));

done:
  return rc;
error:
  goto done;
}

Sdb_func_isnotnull::Sdb_func_isnotnull() {}

Sdb_func_isnotnull::~Sdb_func_isnotnull() {}

int Sdb_func_isnotnull::to_bson(bson::BSONObj &obj) {
  int rc = SDB_ERR_OK;
  Item *item_tmp = NULL;

  if (!is_finished || para_list.elements != para_num_max) {
    rc = SDB_ERR_COND_INCOMPLETED;
    goto error;
  }

  item_tmp = para_list.pop();
  if (Item::FIELD_ITEM != item_tmp->type()) {
    rc = SDB_ERR_COND_UNKOWN_ITEM;
    goto error;
  }
  obj = BSON(((Item_field *)item_tmp)->field_name << BSON(this->name() << 0));

done:
  return rc;
error:
  goto done;
}

Sdb_func_bin_op::Sdb_func_bin_op() {
  para_num_max = 2;
}

Sdb_func_bin_op::~Sdb_func_bin_op() {}

Sdb_func_cmp::Sdb_func_cmp() {}

Sdb_func_cmp::~Sdb_func_cmp() {}

int Sdb_func_cmp::to_bson_with_child(bson::BSONObj &obj) {
  int rc = SDB_ERR_OK;
  Sdb_item *child = NULL;
  Item *field1 = NULL, *field2 = NULL, *field3 = NULL, *item_tmp;
  Item_func *func = NULL;
  Sdb_func_unkown *sdb_func = NULL;
  bool cmp_inverse = FALSE;
  bson::BSONObj obj_tmp;
  bson::BSONObjBuilder builder_tmp;

  if (r_child != NULL) {
    child = r_child;
    cmp_inverse = TRUE;
  } else {
    child = l_child;
  }

  if (child->type() != Item_func::UNKNOWN_FUNC || !child->finished() ||
      ((Sdb_func_item *)child)->get_para_num() != 2 ||
      this->get_para_num() != 2) {
    rc = SDB_ERR_COND_UNEXPECTED_ITEM;
    goto error;
  }
  sdb_func = (Sdb_func_unkown *)child;
  item_tmp = sdb_func->get_func_item();
  if (item_tmp->type() != Item::FUNC_ITEM) {
    rc = SDB_ERR_COND_UNEXPECTED_ITEM;
    goto error;
  }
  func = (Item_func *)item_tmp;
  if (func->functype() != Item_func::UNKNOWN_FUNC) {
    rc = SDB_ERR_COND_UNEXPECTED_ITEM;
    goto error;
  }

  if (sdb_func->pop(field1) || sdb_func->pop(field2)) {
    rc = SDB_ERR_COND_UNEXPECTED_ITEM;
    goto error;
  }
  field3 = para_list.pop();

  if (Item::FIELD_ITEM == field1->type()) {
    if (Item::FIELD_ITEM == field2->type()) {
      if (!(field3->const_item()) || (0 != strcmp(func->func_name(), "-") &&
                                      0 != strcmp(func->func_name(), "/"))) {
        rc = SDB_ERR_COND_UNEXPECTED_ITEM;
        goto error;
      }

      if (0 == strcmp(func->func_name(), "-")) {
        // field1 - field2 < num
        rc = get_item_val("$add", field3, ((Item_field *)field2)->field,
                          obj_tmp);
      } else {
        // field1 / field2 < num
        rc = get_item_val("$multiply", field3, ((Item_field *)field2)->field,
                          obj_tmp);
      }
      if (rc != SDB_ERR_OK) {
        goto error;
      }
      builder_tmp.appendElements(obj_tmp);
      obj_tmp =
          BSON((cmp_inverse ? this->name() : this->inverse_name())
               << BSON("$field" << ((Item_field *)field1)->field->field_name));
      builder_tmp.appendElements(obj_tmp);
      obj_tmp = builder_tmp.obj();
      obj = BSON(((Item_field *)field2)->field_name << obj_tmp);
    } else {
      if (!field2->const_item()) {
        rc = SDB_ERR_COND_UNEXPECTED_ITEM;
        goto error;
      }
      if (0 == strcmp(func->func_name(), "+")) {
        rc = get_item_val("$add", field2, ((Item_field *)field1)->field,
                          obj_tmp);
      } else if (0 == strcmp(func->func_name(), "-")) {
        rc = get_item_val("$subtract", field2, ((Item_field *)field1)->field,
                          obj_tmp);
      } else if (0 == strcmp(func->func_name(), "*")) {
        rc = get_item_val("$multiply", field2, ((Item_field *)field1)->field,
                          obj_tmp);
      } else if (0 == strcmp(func->func_name(), "/")) {
        rc = get_item_val("$divide", field2, ((Item_field *)field1)->field,
                          obj_tmp);
      } else {
        rc = SDB_ERR_COND_UNEXPECTED_ITEM;
      }
      if (rc != SDB_ERR_OK) {
        goto error;
      }
      builder_tmp.appendElements(obj_tmp);
      if (Item::FIELD_ITEM == field3->type()) {
        // field1 - num < field3
        obj_tmp = BSON(
            (cmp_inverse ? this->inverse_name() : this->name())
            << BSON("$field" << ((Item_field *)field3)->field->field_name));
      } else {
        // field1 - num1 < num3
        rc = get_item_val((cmp_inverse ? this->inverse_name() : this->name()),
                          field3, ((Item_field *)field1)->field, obj_tmp);
        if (rc != SDB_ERR_OK) {
          goto error;
        }
      }
      builder_tmp.appendElements(obj_tmp);
      obj_tmp = builder_tmp.obj();
      obj = BSON(((Item_field *)field1)->field->field_name << obj_tmp);
    }
  } else {
    if (!field1->const_item()) {
      rc = SDB_ERR_COND_UNEXPECTED_ITEM;
      goto error;
    }
    if (Item::FIELD_ITEM == field2->type()) {
      if (Item::FIELD_ITEM == field3->type()) {
        // num + field2 < field3
        if (0 == strcmp(func->func_name(), "+")) {
          rc = get_item_val("$add", field1, ((Item_field *)field2)->field,
                            obj_tmp);
        } else if (0 == strcmp(func->func_name(), "*")) {
          rc = get_item_val("$multiply", field1, ((Item_field *)field2)->field,
                            obj_tmp);
        } else {
          rc = SDB_ERR_COND_UNEXPECTED_ITEM;
        }
        if (rc != SDB_ERR_OK) {
          goto error;
        }
        builder_tmp.appendElements(obj_tmp);
        obj_tmp = BSON(
            (cmp_inverse ? this->inverse_name() : this->name())
            << BSON("$field" << ((Item_field *)field3)->field->field_name));
        builder_tmp.appendElements(obj_tmp);
        obj = BSON(((Item_field *)field2)->field->field_name
                   << builder_tmp.obj());
      } else {
        if (!field3->const_item()) {
          rc = SDB_ERR_COND_UNEXPECTED_ITEM;
          goto error;
        }
        if (0 == strcmp(func->func_name(), "+")) {
          // num1 + field2 < num3
          rc = get_item_val("$add", field1, ((Item_field *)field2)->field,
                            obj_tmp);
          if (rc != SDB_ERR_OK) {
            goto error;
          }
          builder_tmp.appendElements(obj_tmp);
          rc = get_item_val((cmp_inverse ? this->inverse_name() : this->name()),
                            field3, ((Item_field *)field2)->field, obj_tmp);
          if (rc != SDB_ERR_OK) {
            goto error;
          }
          builder_tmp.appendElements(obj_tmp);
          obj = BSON(((Item_field *)field2)->field->field_name
                     << builder_tmp.obj());
        } else if (0 == strcmp(func->func_name(), "-")) {
          // num1 - field2 < num3   =>   num1 < num3 + field2
          rc = get_item_val("$add", field3, ((Item_field *)field2)->field,
                            obj_tmp);
          if (rc != SDB_ERR_OK) {
            goto error;
          }
          builder_tmp.appendElements(obj_tmp);
          rc = get_item_val((cmp_inverse ? this->name() : this->inverse_name()),
                            field1, ((Item_field *)field2)->field, obj_tmp);
          if (rc != SDB_ERR_OK) {
            goto error;
          }
          builder_tmp.appendElements(obj_tmp);
          obj = BSON(((Item_field *)field2)->field->field_name
                     << builder_tmp.obj());
        } else if (0 == strcmp(func->func_name(), "*")) {
          // num1 * field2 < num3
          rc = get_item_val("$multiply", field1, ((Item_field *)field2)->field,
                            obj_tmp);
          if (rc != SDB_ERR_OK) {
            goto error;
          }
          builder_tmp.appendElements(obj_tmp);
          rc = get_item_val((cmp_inverse ? this->inverse_name() : this->name()),
                            field3, ((Item_field *)field2)->field, obj_tmp);
          if (rc != SDB_ERR_OK) {
            goto error;
          }
          builder_tmp.appendElements(obj_tmp);
          obj = BSON(((Item_field *)field2)->field->field_name
                     << builder_tmp.obj());
        } else if (0 == strcmp(func->func_name(), "/")) {
          // num1 / field2 < num3   =>   num1 < num3 + field2
          rc = get_item_val("$multiply", field3, ((Item_field *)field2)->field,
                            obj_tmp);
          if (rc != SDB_ERR_OK) {
            goto error;
          }
          builder_tmp.appendElements(obj_tmp);
          rc = get_item_val((cmp_inverse ? this->name() : this->inverse_name()),
                            field1, ((Item_field *)field2)->field, obj_tmp);
          if (rc != SDB_ERR_OK) {
            goto error;
          }
          builder_tmp.appendElements(obj_tmp);
          obj = BSON(((Item_field *)field2)->field->field_name
                     << builder_tmp.obj());
        } else {
          rc = SDB_ERR_COND_UNEXPECTED_ITEM;
          goto error;
        }
      }
    } else {
      rc = SDB_ERR_COND_UNEXPECTED_ITEM;
      goto error;
    }
  }

done:
  return rc;
error:
  goto done;
}

int Sdb_func_cmp::to_bson(bson::BSONObj &obj) {
  int rc = SDB_ERR_OK;
  bool inverse = FALSE;
  bool cmp_with_field = FALSE;
  Item *item_tmp = NULL, *item_val = NULL;
  Item_field *item_field = NULL;
  const char *name_tmp = NULL;
  bson::BSONObj obj_tmp;

  if (!is_finished || para_list.elements != para_num_max) {
    rc = SDB_ERR_COND_INCOMPLETED;
    goto error;
  }

  if (l_child != NULL || r_child != NULL) {
    rc = to_bson_with_child(obj);
    if (rc != SDB_ERR_OK) {
      goto error;
    }
    goto done;
  }

  while (!para_list.is_empty()) {
    item_tmp = para_list.pop();
    if (Item::FIELD_ITEM != item_tmp->type() || item_tmp->const_item()) {
      if (NULL == item_field) {
        inverse = TRUE;
      }
      if (item_val != NULL) {
        rc = SDB_ERR_COND_UNEXPECTED_ITEM;
        goto error;
      }
      item_val = item_tmp;
    } else {
      if (item_field != NULL) {
        if (item_val != NULL) {
          rc = SDB_ERR_COND_PART_UNSUPPORTED;
          goto error;
        }

        if (NULL == item_field->db_name ||
            NULL == ((Item_field *)item_tmp)->db_name ||
            NULL == item_field->table_name ||
            NULL == ((Item_field *)item_tmp)->table_name ||
            0 != strcmp(item_field->db_name,
                        ((Item_field *)item_tmp)->db_name) ||
            0 != strcmp(item_field->table_name,
                        ((Item_field *)item_tmp)->table_name)) {
          rc = SDB_ERR_COND_PART_UNSUPPORTED;
          goto error;
        }
        item_val = item_tmp;
        cmp_with_field = TRUE;
      } else {
        item_field = (Item_field *)item_tmp;
      }
    }
  }

  if (inverse) {
    name_tmp = this->inverse_name();
  } else {
    name_tmp = this->name();
  }

  if (cmp_with_field) {
    enum_field_types l_type = item_field->field->type();
    enum_field_types r_type = ((Item_field *)item_val)->field->type();

    if (MYSQL_TYPE_JSON == l_type || MYSQL_TYPE_JSON == r_type) {
      rc = SDB_ERR_COND_PART_UNSUPPORTED;
      goto error;
    }

    if (l_type != r_type) {
      // floating-point values in different types can't compare
      if (sdb_field_is_floating(l_type) && sdb_field_is_floating(r_type)) {
        rc = SDB_ERR_COND_PART_UNSUPPORTED;
        goto error;
      }

      // date and time types can't compare
      if (sdb_field_is_date_time(l_type) || sdb_field_is_date_time(r_type)) {
        rc = SDB_ERR_COND_PART_UNSUPPORTED;
        goto error;
      }
    }

    obj = BSON(item_field->field_name
               << BSON(name_tmp << BSON(
                           "$field" << ((Item_field *)item_val)->field_name)));
    goto done;
  }

  rc = get_item_val(name_tmp, item_val, item_field->field, obj_tmp);
  if (rc) {
    goto error;
  }
  obj = BSON(item_field->field_name << obj_tmp);

done:
  return rc;
error:
  if (SDB_ERR_OVF == rc) {
    rc = SDB_ERR_COND_PART_UNSUPPORTED;
  }
  goto done;
}

Sdb_func_between::Sdb_func_between(bool has_not) : Sdb_func_neg(has_not) {
  para_num_max = 3;
}

Sdb_func_between::~Sdb_func_between() {}

int Sdb_func_between::to_bson(bson::BSONObj &obj) {
  int rc = SDB_ERR_OK;
  Item_field *item_field = NULL;
  Item *item_start = NULL, *item_end = NULL, *item_tmp = NULL;
  bson::BSONObj obj_start, obj_end, obj_tmp;
  bson::BSONArrayBuilder arr_builder;

  if (!is_finished || para_list.elements != para_num_max) {
    rc = SDB_ERR_COND_INCOMPLETED;
    goto error;
  }

  if (l_child != NULL || r_child != NULL) {
    rc = SDB_ERR_COND_UNKOWN_ITEM;
    goto error;
  }

  item_tmp = para_list.pop();
  if (Item::FIELD_ITEM != item_tmp->type()) {
    rc = SDB_ERR_COND_UNEXPECTED_ITEM;
    goto error;
  }
  item_field = (Item_field *)item_tmp;

  item_start = para_list.pop();

  item_end = para_list.pop();

  if (negated) {
    rc = get_item_val("$lt", item_start, item_field->field, obj_tmp);
    if (rc) {
      goto error;
    }
    obj_start = BSON(item_field->field_name << obj_tmp);

    rc = get_item_val("$gt", item_end, item_field->field, obj_tmp);
    if (rc) {
      goto error;
    }
    obj_end = BSON(item_field->field_name << obj_tmp);

    arr_builder.append(obj_start);
    arr_builder.append(obj_end);
    obj = BSON("$or" << arr_builder.arr());
  } else {
    rc = get_item_val("$gte", item_start, item_field->field, obj_tmp);
    if (rc) {
      goto error;
    }
    obj_start = BSON(item_field->field_name << obj_tmp);

    rc = get_item_val("$lte", item_end, item_field->field, obj_tmp);
    if (rc) {
      goto error;
    }
    obj_end = BSON(item_field->field_name << obj_tmp);

    arr_builder.append(obj_start);
    arr_builder.append(obj_end);
    obj = BSON("$and" << arr_builder.arr());
  }

done:
  return rc;
error:
  if (SDB_ERR_OVF == rc) {
    rc = SDB_ERR_COND_PART_UNSUPPORTED;
  }
  goto done;
}

Sdb_func_in::Sdb_func_in(bool has_not, uint args_num) : Sdb_func_neg(has_not) {
  para_num_max = args_num;
}

Sdb_func_in::~Sdb_func_in() {}

int Sdb_func_in::to_bson(bson::BSONObj &obj) {
  int rc = SDB_ERR_OK;
  Item_field *item_field = NULL;
  Item *item_tmp = NULL;
  bson::BSONArrayBuilder arr_builder;
  bson::BSONObj obj_tmp;

  if (!is_finished || para_list.elements != para_num_max) {
    rc = SDB_ERR_COND_INCOMPLETED;
    goto error;
  }

  if (l_child != NULL || r_child != NULL) {
    rc = SDB_ERR_COND_UNKOWN_ITEM;
    goto error;
  }

  item_tmp = para_list.pop();
  if (Item::FIELD_ITEM != item_tmp->type()) {
    rc = SDB_ERR_COND_UNEXPECTED_ITEM;
    goto error;
  }
  item_field = (Item_field *)item_tmp;

  while (!para_list.is_empty()) {
    item_tmp = para_list.pop();
    rc = get_item_val("", item_tmp, item_field->field, obj_tmp, &arr_builder);
    if (rc) {
      goto error;
    }
  }

  if (negated) {
    obj = BSON(item_field->field_name << BSON("$nin" << arr_builder.arr()));
  } else {
    obj = BSON(item_field->field_name << BSON("$in" << arr_builder.arr()));
  }

done:
  return rc;
error:
  if (SDB_ERR_OVF == rc) {
    rc = SDB_ERR_COND_PART_UNSUPPORTED;
  }
  goto done;
}

Sdb_func_like::Sdb_func_like(Item_func_like *item) : like_item(item) {}

Sdb_func_like::~Sdb_func_like() {}

int Sdb_func_like::to_bson(bson::BSONObj &obj) {
  int rc = SDB_ERR_OK;
  Item_field *item_field = NULL;
  Item *item_tmp = NULL;
  Item_string *item_val = NULL;
  String *str_val_org;
  String str_val_conv;
  std::string regex_val;

  if (!is_finished || para_list.elements != para_num_max) {
    rc = SDB_ERR_COND_INCOMPLETED;
    goto error;
  }

  if (!like_item->escape_is_evaluated() || !my_isascii(like_item->escape)) {
    rc = SDB_ERR_COND_UNSUPPORTED;
    goto error;
  }

  if (l_child != NULL || r_child != NULL) {
    rc = SDB_ERR_COND_UNKOWN_ITEM;
    goto error;
  }

  while (!para_list.is_empty()) {
    item_tmp = para_list.pop();
    if (Item::FIELD_ITEM != item_tmp->type()) {
      if (item_tmp->type() != Item::STRING_ITEM  // only support string
          || item_val != NULL) {
        rc = SDB_ERR_COND_UNEXPECTED_ITEM;
        goto error;
      }

      item_val = (Item_string *)item_tmp;
    } else {
      if (item_field != NULL) {
        // not support: field1 like field2
        rc = SDB_ERR_COND_UNEXPECTED_ITEM;
        goto error;
      }
      item_field = (Item_field *)item_tmp;

      // only support the string-field
      if ((item_field->field_type() != MYSQL_TYPE_VARCHAR &&
           item_field->field_type() != MYSQL_TYPE_VAR_STRING &&
           item_field->field_type() != MYSQL_TYPE_STRING &&
           item_field->field_type() != MYSQL_TYPE_TINY_BLOB &&
           item_field->field_type() != MYSQL_TYPE_MEDIUM_BLOB &&
           item_field->field_type() != MYSQL_TYPE_LONG_BLOB &&
           item_field->field_type() != MYSQL_TYPE_BLOB) ||
          item_field->field->binary()) {
        rc = SDB_ERR_COND_UNEXPECTED_ITEM;
        goto error;
      }
    }
  }

  str_val_org = item_val->val_str(NULL);
  rc = sdb_convert_charset(*str_val_org, str_val_conv, &SDB_CHARSET);
  if (rc) {
    goto error;
  }
  rc = get_regex_str(str_val_conv.ptr(), str_val_conv.length(), regex_val);
  if (rc) {
    goto error;
  }

  if (regex_val.empty()) {
    // select * from t1 where a like "";
    // => {a:""}
    obj = BSON(item_field->field_name << regex_val);
  } else {
    obj = BSON(item_field->field_name << BSON("$regex" << regex_val));
  }

done:
  return rc;
error:
  goto done;
}

int Sdb_func_like::get_regex_str(const char *like_str, size_t len,
                                 std::string &regex_str) {
  int rc = SDB_ERR_OK;
  const char *p_prev, *p_cur, *p_begin, *p_end, *p_last;
  char str_buf[SDB_MATCH_FIELD_SIZE_MAX + 2] = {0};  // reserve one byte for '\'
  int buf_pos = 0;
  regex_str = "";
  int escape_char = like_item->escape;

  if (0 == len) {
    // select * from t1 where field like "" ;
    // => {field: ""}
    goto done;
  }

  p_begin = like_str;
  p_end = p_begin + len - 1;
  p_prev = NULL;
  p_cur = p_begin;
  p_last = p_begin;
  while (p_cur <= p_end) {
    if (buf_pos >= SDB_MATCH_FIELD_SIZE_MAX) {
      // reserve 2 byte for character and '\'
      rc = SDB_ERR_SIZE_OVF;
    }

    if ('%' == *p_cur || '_' == *p_cur) {
      // '%' and '_' are treated as normal character
      if (p_prev != NULL && escape_char == *p_prev) {
        // skip the escape
        str_buf[buf_pos - 1] = *p_cur;
        p_prev = NULL;
      } else {
        // begin with the string:
        //     select * from t1 where field like "abc%"
        //     => (^abc.*)
        if (p_begin == p_last) {
          regex_str = "^";
        }

        if (buf_pos > 0) {
          regex_str.append(str_buf, buf_pos);
          buf_pos = 0;
        }

        if ('%' == *p_cur) {
          regex_str.append(".*");
        } else {
          regex_str.append(".");
        }
        p_last = p_cur + 1;
        ++p_cur;
        continue;
      }
    } else {
      if (p_prev != NULL && escape_char == *p_prev) {
        if (buf_pos > 0) {
          // skip the escape.
          --buf_pos;
        }
        if ('(' == *p_cur || ')' == *p_cur || '[' == *p_cur || ']' == *p_cur ||
            '{' == *p_cur || '}' == *p_cur || '\\' == *p_cur || '^' == *p_cur ||
            '$' == *p_cur || '.' == *p_cur || '|' == *p_cur || '*' == *p_cur ||
            '+' == *p_cur || '?' == *p_cur || '-' == *p_cur) {
          // process perl regexp special characters: {}[]()^$.|*+?-\
          // add '\' before the special character
          str_buf[buf_pos++] = '\\';
        }
        str_buf[buf_pos++] = *p_cur;
        p_prev = NULL;
      } else {
        if (('(' == *p_cur || ')' == *p_cur || '[' == *p_cur || ']' == *p_cur ||
             '{' == *p_cur || '}' == *p_cur || '^' == *p_cur || '$' == *p_cur ||
             '.' == *p_cur || '|' == *p_cur || '*' == *p_cur || '+' == *p_cur ||
             '?' == *p_cur || '-' == *p_cur || '\\' == *p_cur) &&
            (escape_char != *p_cur)) {
          // process perl regexp special characters: {}[]()^$.|*+?-\
          // add '\' before the special character
          str_buf[buf_pos++] = '\\';
          str_buf[buf_pos++] = *p_cur;
        } else {
          str_buf[buf_pos++] = *p_cur;
        }
        p_prev = p_cur;
      }
    }
    ++p_cur;
  }

  if (p_last == p_begin) {
    regex_str = "^";
  }
  if (buf_pos > 0) {
    regex_str.append(str_buf, buf_pos);
    buf_pos = 0;
  }
  regex_str.append("$");

done:
  return rc;
}
