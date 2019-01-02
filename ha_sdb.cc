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

#include "ha_sdb.h"
#include <sql_class.h>
#include <sql_table.h>
#include <mysql/plugin.h>
#include <mysql/psi/mysql_file.h>
#include <json_dom.h>
#include <time.h>
#include <client.hpp>
#include "sdb_log.h"
#include "sdb_conf.h"
#include "sdb_cl.h"
#include "sdb_conn.h"
#include "sdb_thd.h"
#include "sdb_util.h"
#include "sdb_condition.h"
#include "sdb_errcode.h"
#include "sdb_idx.h"

using namespace sdbclient;

#ifndef SDB_DRIVER_VERSION
#define SDB_DRIVER_VERSION "UNKNOWN"
#endif

#ifndef SDB_PLUGIN_VERSION
#define SDB_PLUGIN_VERSION "UNKNOWN"
#endif

#ifdef DEBUG
#ifdef SDB_ENTERPRISE
#define SDB_ENGINE_EDITION "Enterprise-Debug"
#else /* SDB_ENTERPRISE */
#define SDB_ENGINE_EDITION "Community-Debug"
#endif /* SDB_ENTERPRISE */
#else  /* DEBUG */
#ifdef SDB_ENTERPRISE
#define SDB_ENGINE_EDITION "Enterprise"
#else /* SDB_ENTERPRISE */
#define SDB_ENGINE_EDITION "Community"
#endif /* SDB_ENTERPRISE */
#endif /* DEBUG */

#define SDB_ENGINE_INFO "SequoiaDB storage engine(" SDB_ENGINE_EDITION ")"
#define SDB_VERSION_INFO                                        \
  "Plugin: " SDB_PLUGIN_VERSION ", Driver: " SDB_DRIVER_VERSION \
  ", BuildTime: " __DATE__ " " __TIME__

#define SDB_OID_LEN 12
#define SDB_OID_FIELD "_id"
#define SDB_FIELD_MAX_LEN (16 * 1024 * 1024)

const static char *sdb_plugin_info = SDB_ENGINE_INFO ". " SDB_VERSION_INFO ".";

handlerton *sdb_hton = NULL;

mysql_mutex_t sdb_mutex;
static PSI_mutex_key key_mutex_sdb, key_mutex_SDB_SHARE_mutex;
static HASH sdb_open_tables;
static PSI_memory_key key_memory_sdb_share;
static PSI_memory_key sdb_key_memory_blobroot;

static uchar *sdb_get_key(Sdb_share *share, size_t *length,
                          my_bool not_used MY_ATTRIBUTE((unused))) {
  *length = share->table_name_length;
  return (uchar *)share->table_name;
}

static Sdb_share *get_sdb_share(const char *table_name, TABLE *table) {
  Sdb_share *share = NULL;
  char *tmp_name = NULL;
  uint length;

  mysql_mutex_lock(&sdb_mutex);
  length = (uint)strlen(table_name);

  /*
   If share is not present in the hash, create a new share and
   initialize its members.
  */

  if (!(share = (Sdb_share *)my_hash_search(&sdb_open_tables,
                                            (uchar *)table_name, length))) {
    if (!my_multi_malloc(key_memory_sdb_share, MYF(MY_WME | MY_ZEROFILL),
                         &share, sizeof(*share), &tmp_name, length + 1,
                         NullS)) {
      goto error;
    }

    share->use_count = 0;
    share->table_name_length = length;
    share->table_name = tmp_name;
    strncpy(share->table_name, table_name, length);

    if (my_hash_insert(&sdb_open_tables, (uchar *)share)) {
      goto error;
    }
    thr_lock_init(&share->lock);
    mysql_mutex_init(key_mutex_SDB_SHARE_mutex, &share->mutex,
                     MY_MUTEX_INIT_FAST);
  }

  share->use_count++;

done:
  mysql_mutex_unlock(&sdb_mutex);
  return share;
error:
  if (share) {
    my_free(share);
    share = NULL;
  }
  goto done;
}

static int free_sdb_share(Sdb_share *share) {
  mysql_mutex_lock(&sdb_mutex);
  if (!--share->use_count) {
    my_hash_delete(&sdb_open_tables, (uchar *)share);
    thr_lock_delete(&share->lock);
    mysql_mutex_destroy(&share->mutex);
    my_free(share);
  }
  mysql_mutex_unlock(&sdb_mutex);

  return 0;
}

ha_sdb::ha_sdb(handlerton *hton, TABLE_SHARE *table_arg)
    : handler(hton, table_arg) {
  active_index = MAX_KEY;
  share = NULL;
  m_lock_type = TL_IGNORE;
  collection = NULL;
  first_read = true;
  count_times = 0;
  last_count_time = time(NULL);
  m_use_bulk_insert = false;
  stats.records = 0;
  memset(db_name, 0, SDB_CS_NAME_MAX_SIZE + 1);
  memset(table_name, 0, SDB_CL_NAME_MAX_SIZE + 1);
  init_alloc_root(sdb_key_memory_blobroot, &blobroot, 8 * 1024, 0);
}

ha_sdb::~ha_sdb() {
  free_root(&blobroot, MYF(0));
  if (NULL != collection) {
    delete collection;
    collection = NULL;
  }
}

const char **ha_sdb::bas_ext() const {
  /*
    If frm_error() is called then we will use this to find out
    what file extensions exist for the storage engine. This is
    also used by the default rename_table and delete_table method
    in handler.cc.
    SequoiaDB is a distributed database, and we have implemented delete_table,
    so it's no need to fill this array.
  */
  static const char *ext[] = {NullS};
  return ext;
}

ulonglong ha_sdb::table_flags() const {
  return (HA_REC_NOT_IN_SEQ | HA_NO_AUTO_INCREMENT | HA_NO_READ_LOCAL_LOCK |
          HA_BINLOG_ROW_CAPABLE | HA_BINLOG_STMT_CAPABLE |
          HA_TABLE_SCAN_ON_INDEX | HA_NULL_IN_KEY | HA_CAN_INDEX_BLOBS);
}

ulong ha_sdb::index_flags(uint inx, uint part, bool all_parts) const {
  // TODO: SUPPORT SORT
  // HA_READ_NEXT | HA_KEYREAD_ONLY ;
  return (HA_READ_RANGE | HA_DO_INDEX_COND_PUSHDOWN | HA_READ_NEXT |
          HA_READ_ORDER);
}

uint ha_sdb::max_supported_record_length() const {
  return HA_MAX_REC_LENGTH;
}

uint ha_sdb::max_supported_keys() const {
  return MAX_KEY;
}

uint ha_sdb::max_supported_key_length() const {
  return 1000;
}

#if MYSQL_VERSION_ID >= 50723
uint ha_sdb::max_supported_key_part_length(
    HA_CREATE_INFO *create_info MY_ATTRIBUTE((unused))) const {
  return 1000;
}
#else
uint ha_sdb::max_supported_key_part_length() const {
  return 1000;
}
#endif

int ha_sdb::open(const char *name, int mode, uint test_if_locked) {
  int rc = 0;
  ref_length = SDB_OID_LEN;  // length of _id
  Sdb_conn *connection = NULL;
  Sdb_cl cl;

  if (!(share = get_sdb_share(name, table))) {
    rc = HA_ERR_OUT_OF_MEM;
    goto error;
  }

  rc = sdb_parse_table_name(name, db_name, SDB_CS_NAME_MAX_SIZE, table_name,
                            SDB_CL_NAME_MAX_SIZE);
  if (rc != 0) {
    SDB_LOG_ERROR("Table name[%s] can't be parsed. rc: %d", name, rc);
    goto error;
  }

  if (sdb_is_tmp_table(name, table_name)) {
    DBUG_ASSERT(table->s->tmp_table);
    if (0 != sdb_rebuild_db_name_of_temp_table(db_name, SDB_CS_NAME_MAX_SIZE)) {
      rc = HA_ERR_GENERIC;
      goto error;
    }
  }

  connection = check_sdb_in_thd(ha_thd(), true);
  if (NULL == connection) {
    rc = HA_ERR_NO_CONNECTION;
    goto error;
  }
  DBUG_ASSERT(connection->thread_id() == ha_thd()->thread_id());

  // Get collection to check if the collection is available.
  rc = connection->get_cl(db_name, table_name, cl);
  if (0 != rc) {
    SDB_LOG_ERROR("Collection[%s.%s] is not available. rc: %d", db_name,
                  table_name, rc);
    goto error;
  }

  thr_lock_data_init(&share->lock, &lock_data, (void *)this);

done:
  return rc;
error:
  if (share) {
    free_sdb_share(share);
    share = NULL;
  }
  goto done;
}

int ha_sdb::close(void) {
  if (NULL != collection) {
    delete collection;
    collection = NULL;
  }
  if (share) {
    free_sdb_share(share);
    share = NULL;
  }
  m_bulk_insert_rows.clear();
  m_bson_element_cache.release();
  return 0;
}

int ha_sdb::reset() {
  if (NULL != collection) {
    delete collection;
    collection = NULL;
  }
  // don't release bson element cache, so that we can reuse it
  m_bulk_insert_rows.clear();
  free_root(&blobroot, MYF(0));
  m_lock_type = TL_IGNORE;
  pushed_condition = SDB_EMPTY_BSON;
  return 0;
}

int ha_sdb::row_to_obj(uchar *buf, bson::BSONObj &obj, bool gen_oid,
                       bool output_null, bson::BSONObj &null_obj) {
  int rc = 0;
  bson::BSONObjBuilder obj_builder;
  bson::BSONObjBuilder null_obj_builder;

  my_bitmap_map *org_bitmap = dbug_tmp_use_all_columns(table, table->read_set);
  if (buf != table->record[0]) {
    repoint_field_to_record(table, table->record[0], buf);
  }

  if (gen_oid) {
    // Generate and assign an OID for the _id field.
    // _id should be the first element for good performance.
    obj_builder.genOID();
  }

  for (Field **field = table->field; *field; field++) {
    if ((*field)->is_null()) {
      if (output_null) {
        null_obj_builder.append((*field)->field_name, "");
      }
    } else {
      rc = field_to_obj(*field, obj_builder);
      if (0 != rc) {
        goto error;
      }
    }
  }
  obj = obj_builder.obj();
  null_obj = null_obj_builder.obj();

done:
  if (buf != table->record[0]) {
    repoint_field_to_record(table, buf, table->record[0]);
  }
  dbug_tmp_restore_column_map(table->read_set, org_bitmap);
  return rc;
error:
  goto done;
}

int ha_sdb::field_to_obj(Field *field, bson::BSONObjBuilder &obj_builder) {
  int rc = 0;

  DBUG_ASSERT(NULL != field);

  switch (field->type()) {
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_YEAR: {
      // overflow is impossible, store as INT32
      DBUG_ASSERT(field->val_int() <= INT_MAX32 &&
                  field->val_int() >= INT_MIN32);
      obj_builder.append(field->field_name, (int)field->val_int());
      break;
    }
    case MYSQL_TYPE_BIT:
    case MYSQL_TYPE_LONG: {
      longlong value = field->val_int();
      if (value > INT_MAX32 || value < INT_MIN32) {
        // overflow, so store as INT64
        obj_builder.append(field->field_name, (long long)value);
      } else {
        obj_builder.append(field->field_name, (int)value);
      }
      break;
    }
    case MYSQL_TYPE_LONGLONG: {
      longlong value = field->val_int();
      if (value < 0 && ((Field_num *)field)->unsigned_flag) {
        // overflow, so store as DECIMAL
        my_decimal tmp_val;
        char buff[MAX_FIELD_WIDTH];
        String str(buff, sizeof(buff), field->charset());
        ((Field_num *)field)->val_decimal(&tmp_val);
        my_decimal2string(E_DEC_FATAL_ERROR, &tmp_val, 0, 0, 0, &str);
        obj_builder.appendDecimal(field->field_name, str.c_ptr());
      } else {
        obj_builder.append(field->field_name, (long long)value);
      }
      break;
    }
    case MYSQL_TYPE_FLOAT:
    case MYSQL_TYPE_DOUBLE:
    case MYSQL_TYPE_TIME: {
      obj_builder.append(field->field_name, field->val_real());
      break;
    }
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_BLOB: {
      String val_tmp;
      field->val_str(&val_tmp);
      if (((Field_str *)field)->binary()) {
        obj_builder.appendBinData(field->field_name, val_tmp.length(),
                                  bson::BinDataGeneral, val_tmp.ptr());
      } else {
        String conv_str;
        String *str = &val_tmp;
        if (!my_charset_same(str->charset(), &SDB_CHARSET)) {
          rc = sdb_convert_charset(*str, conv_str, &SDB_CHARSET);
          if (rc) {
            goto error;
          }
          str = &conv_str;
        }

        obj_builder.appendStrWithNoTerminating(field->field_name, str->ptr(),
                                               str->length());
      }
      break;
    }
    case MYSQL_TYPE_NEWDECIMAL:
    case MYSQL_TYPE_DECIMAL: {
      Field_decimal *f = (Field_decimal *)field;
      int precision = (int)(f->pack_length());
      int scale = (int)(f->decimals());
      if (precision < 0 || scale < 0) {
        rc = -1;
        goto error;
      }
      char buff[MAX_FIELD_WIDTH];
      String str(buff, sizeof(buff), field->charset());
      String unused;
      f->val_str(&str, &unused);
      obj_builder.appendDecimal(field->field_name, str.c_ptr());
      break;
    }
    case MYSQL_TYPE_DATE: {
      longlong date_val = 0;
      date_val = ((Field_newdate *)field)->val_int();
      struct tm tm_val;
      tm_val.tm_sec = 0;
      tm_val.tm_min = 0;
      tm_val.tm_hour = 0;
      tm_val.tm_mday = date_val % 100;
      date_val = date_val / 100;
      tm_val.tm_mon = date_val % 100 - 1;
      date_val = date_val / 100;
      tm_val.tm_year = date_val - 1900;
      tm_val.tm_wday = 0;
      tm_val.tm_yday = 0;
      tm_val.tm_isdst = 0;
      time_t time_tmp = mktime(&tm_val);
      bson::Date_t dt((longlong)(time_tmp * 1000));
      obj_builder.appendDate(field->field_name, dt);
      break;
    }
    case MYSQL_TYPE_TIMESTAMP2:
    case MYSQL_TYPE_TIMESTAMP: {
      struct timeval tm;
      int warnings = 0;
      field->get_timestamp(&tm, &warnings);
      obj_builder.appendTimestamp(field->field_name, tm.tv_sec * 1000,
                                  tm.tv_usec);
      break;
    }
    case MYSQL_TYPE_NULL:
      // skip the null value
      break;
    case MYSQL_TYPE_DATETIME: {
      char buff[MAX_FIELD_WIDTH];
      String str(buff, sizeof(buff), field->charset());
      field->val_str(&str);
      obj_builder.append(field->field_name, str.c_ptr());
      break;
    }
    case MYSQL_TYPE_JSON: {
      Json_wrapper wr;
      String buf;
      Field_json *field_json = dynamic_cast<Field_json *>(field);

#if MYSQL_VERSION_ID >= 50722
      if (field_json->val_json(&wr) || wr.to_binary(&buf)) {
#else
      if (field_json->val_json(&wr) || wr.to_value().raw_binary(&buf)) {
#endif
        my_error(ER_INVALID_JSON_BINARY_DATA, MYF(0));
        rc = ER_INVALID_JSON_BINARY_DATA;
        goto error;
      }

      obj_builder.appendBinData(field->field_name, buf.length(),
                                bson::BinDataGeneral, buf.ptr());
      break;
    }
    default: {
      SDB_PRINT_ERROR(ER_BAD_FIELD_ERROR, ER(ER_BAD_FIELD_ERROR),
                      field->field_name, table_name);
      rc = ER_BAD_FIELD_ERROR;
      goto error;
    }
  }

done:
  return rc;
error:
  goto done;
}

/*
  If table has unique keys, we can match a specific record by the value of
  unique key instead of the whole record.

  @return false if success
*/
my_bool ha_sdb::get_unique_key_cond(const uchar *rec_row, bson::BSONObj &cond) {
  my_bool rc = true;
  // force cast to adapt sql layer unreasonable interface.
  uchar *row = const_cast<uchar *>(rec_row);
  my_bitmap_map *org_bitmap = dbug_tmp_use_all_columns(table, table->read_set);
  if (row != table->record[0]) {
    repoint_field_to_record(table, table->record[0], row);
  }

  // 1. match by primary key
  uint index_no = table->s->primary_key;
  if (index_no < MAX_KEY) {
    const KEY *primary_key = table->s->key_info + index_no;
    rc = get_cond_from_key(primary_key, cond);
    if (!rc) {
      goto done;
    }
  }

  // 2. match by other unique index fields.
  for (uint i = 0; i < table->s->keys; ++i) {
    const KEY *key_info = table->s->key_info + i;
    if (key_info->flags & HA_NOSAME) {
      rc = get_cond_from_key(key_info, cond);
      if (!rc) {
        goto done;
      }
    }
  }

done:
  if (row != table->record[0]) {
    repoint_field_to_record(table, row, table->record[0]);
  }
  dbug_tmp_restore_column_map(table->read_set, org_bitmap);
  return rc;
}

/*
  @return false if success
*/
my_bool ha_sdb::get_cond_from_key(const KEY *unique_key, bson::BSONObj &cond) {
  my_bool rc = true;
  const KEY_PART_INFO *key_part = unique_key->key_part;
  const KEY_PART_INFO *key_end = key_part + unique_key->user_defined_key_parts;
  my_bool all_field_null = true;
  bson::BSONObjBuilder builder;

  for (; key_part != key_end; ++key_part) {
    Field *field = table->field[key_part->fieldnr - 1];

    if (!field->is_null()) {
      if (SDB_ERR_OK != field_to_obj(field, builder)) {
        rc = true;
        goto error;
      }
      all_field_null = false;
    } else {
      bson::BSONObjBuilder sub_builder(builder.subobjStart(field->field_name));
      sub_builder.append("$isnull", 1);
      sub_builder.doneFast();
    }
  }
  // If all fields are NULL, more than one record may be matched!
  if (all_field_null) {
    rc = true;
    goto error;
  }
  cond = builder.obj();
  rc = false;

done:
  return rc;
error:
  goto done;
}

int ha_sdb::get_update_obj(const uchar *old_data, uchar *new_data,
                           bson::BSONObj &obj, bson::BSONObj &null_obj) {
  int rc = 0;
  uint row_offset = (uint)(old_data - new_data);
  bson::BSONObjBuilder obj_builder;
  bson::BSONObjBuilder null_obj_builder;

  my_bitmap_map *org_bitmap = dbug_tmp_use_all_columns(table, table->read_set);
  if (new_data != table->record[0]) {
    repoint_field_to_record(table, table->record[0], new_data);
  }

  for (Field **fields = table->field; *fields; fields++) {
    Field *field = *fields;
    bool is_null = field->is_null();
    if (is_null != field->is_null_in_record(old_data)) {
      if (is_null) {
        null_obj_builder.append(field->field_name, "");
      } else {
        rc = field_to_obj(field, obj_builder);
        if (0 != rc) {
          goto error;
        }
      }
    } else if (!is_null) {
      if (field->cmp_binary_offset(row_offset) != 0) {
        rc = field_to_obj(field, obj_builder);
        if (0 != rc) {
          goto error;
        }
      }
    }
  }
  obj = obj_builder.obj();
  null_obj = null_obj_builder.obj();

done:
  if (new_data != table->record[0]) {
    repoint_field_to_record(table, new_data, table->record[0]);
  }
  dbug_tmp_restore_column_map(table->read_set, org_bitmap);
  return rc;
error:
  goto done;
}

void ha_sdb::start_bulk_insert(ha_rows rows) {
  if (!sdb_use_bulk_insert) {
    m_use_bulk_insert = false;
    return;
  }

  m_bulk_insert_rows.clear();

  /**
    We don't bother with bulk-insert semantics when the estimated rows == 1
    The rows value will be 0 if the server does not know how many rows
    would be inserted. This can occur when performing INSERT...SELECT
  */
  if (rows == 1) {
    m_use_bulk_insert = false;
    return;
  }

  m_use_bulk_insert = true;
}

int ha_sdb::flush_bulk_insert(bool ignore_dup_key) {
  DBUG_ASSERT(m_bulk_insert_rows.size() > 0);
  DBUG_ASSERT(NULL != collection);
  DBUG_ASSERT(collection->thread_id() == ha_thd()->thread_id());

  int flag = ignore_dup_key ? FLG_INSERT_CONTONDUP : 0;

  int rc = collection->bulk_insert(flag, m_bulk_insert_rows);
  if (rc != 0) {
    if (SDB_IXM_DUP_KEY == get_sdb_code(rc)) {
      // convert to MySQL errcode
      rc = HA_ERR_FOUND_DUPP_KEY;
    }
  }
  stats.records += m_bulk_insert_rows.size();
  m_bulk_insert_rows.clear();
  return rc;
}

int ha_sdb::end_bulk_insert() {
  int rc = 0;

  if (m_use_bulk_insert) {
    m_use_bulk_insert = false;
    if (m_bulk_insert_rows.size() > 0) {
      bool ignore_dup_key = ha_thd()->lex && ha_thd()->lex->is_ignore();
      rc = flush_bulk_insert(ignore_dup_key);
    }
  }

  return rc;
}

int ha_sdb::write_row(uchar *buf) {
  int rc = 0;
  bson::BSONObj obj;
  bson::BSONObj tmp_obj;
  bool ignore_dup_key = ha_thd()->lex && ha_thd()->lex->is_ignore();

  ha_statistic_increment(&SSV::ha_write_count);

  DBUG_ASSERT(NULL != collection);
  DBUG_ASSERT(collection->thread_id() == ha_thd()->thread_id());

  rc = row_to_obj(buf, obj, TRUE, FALSE, tmp_obj);
  if (rc != 0) {
    goto error;
  }

  if (m_use_bulk_insert) {
    m_bulk_insert_rows.push_back(obj);
    if ((int)m_bulk_insert_rows.size() >= sdb_bulk_insert_size) {
      rc = flush_bulk_insert(ignore_dup_key);
      if (rc != 0) {
        goto error;
      }
    }
  } else {
    // TODO: SequoiaDB C++ driver currently has no insert() method with a flag,
    // we need send FLG_INSERT_CONTONDUP flag to server to ignore duplicate key
    // error, so that SequoiaDB will not rollback transaction, here we
    // temporarily use bulk_insert() instead, this should be revised when driver
    // add new method in new version.
    std::vector<bson::BSONObj> row(1, obj);
    int flag = ignore_dup_key ? FLG_INSERT_CONTONDUP : 0;
    rc = collection->bulk_insert(flag, row);
    if (rc != 0) {
      if (SDB_IXM_DUP_KEY == get_sdb_code(rc)) {
        // convert to MySQL errcode
        rc = HA_ERR_FOUND_DUPP_KEY;
      }
      goto error;
    }

    stats.records++;
  }

done:
  return rc;
error:
  goto done;
}

int ha_sdb::update_row(const uchar *old_data, uchar *new_data) {
  int rc = 0;
  bson::BSONObj cond;
  bson::BSONObj new_obj;
  bson::BSONObj null_obj;
  bson::BSONObj rule_obj;

  DBUG_ASSERT(NULL != collection);
  DBUG_ASSERT(collection->thread_id() == ha_thd()->thread_id());

  ha_statistic_increment(&SSV::ha_update_count);

  rc = get_update_obj(old_data, new_data, new_obj, null_obj);
  if (rc != 0) {
    if (HA_ERR_UNKNOWN_CHARSET == rc && ha_thd()->lex->is_ignore()) {
      rc = 0;
    } else {
      goto error;
    }
  }

  if (null_obj.isEmpty()) {
    rule_obj = BSON("$set" << new_obj);
  } else {
    rule_obj = BSON("$set" << new_obj << "$unset" << null_obj);
  }

  if (get_unique_key_cond(old_data, cond)) {
    cond = cur_rec;
  }
  rc = collection->update(rule_obj, cond, SDB_EMPTY_BSON,
                          UPDATE_KEEP_SHARDINGKEY);
  if (rc != 0) {
    if (SDB_IXM_DUP_KEY == get_sdb_code(rc)) {
      // convert to MySQL errcode
      rc = HA_ERR_FOUND_DUPP_KEY;
    }
    goto error;
  }

done:
  return rc;
error:
  goto done;
}

int ha_sdb::delete_row(const uchar *buf) {
  int rc = 0;
  bson::BSONObj cond;

  DBUG_ASSERT(NULL != collection);
  DBUG_ASSERT(collection->thread_id() == ha_thd()->thread_id());

  ha_statistic_increment(&SSV::ha_delete_count);

  if (get_unique_key_cond(buf, cond)) {
    cond = cur_rec;
  }
  rc = collection->del(cond);
  if (rc != 0) {
    goto error;
  }

  stats.records--;

done:
  return rc;
error:
  goto done;
}

int ha_sdb::index_next(uchar *buf) {
  int rc = 0;

  DBUG_ASSERT(idx_order_direction == 1);

  ha_statistic_increment(&SSV::ha_read_next_count);

  rc = next_row(cur_rec, buf);
  if (rc != 0) {
    goto error;
  }

done:
  return rc;
error:
  goto done;
}

int ha_sdb::index_prev(uchar *buf) {
  int rc = 0;

  DBUG_ASSERT(idx_order_direction == -1);

  ha_statistic_increment(&SSV::ha_read_prev_count);

  rc = next_row(cur_rec, buf);
  if (rc != 0) {
    goto error;
  }

done:
  return rc;
error:
  goto done;
}

int ha_sdb::index_last(uchar *buf) {
  int rc = 0;
  rc = index_read_one(pushed_condition, -1, buf);
  if (rc) {
    goto error;
  }
done:
  return rc;
error:
  goto done;
}

int ha_sdb::index_first(uchar *buf) {
  int rc = 0;
  rc = index_read_one(pushed_condition, 1, buf);
  if (rc) {
    goto error;
  }
done:
  return rc;
error:
  goto done;
}

int ha_sdb::index_read_map(uchar *buf, const uchar *key_ptr,
                           key_part_map keypart_map,
                           enum ha_rkey_function find_flag) {
  int rc = 0;
  bson::BSONObjBuilder cond_builder;
  bson::BSONObj condition = pushed_condition;
  bson::BSONObj condition_idx;
  int order_direction = 1;

  if (NULL != key_ptr && active_index < MAX_KEY) {
    KEY *key_info = table->key_info + active_index;
    key_range start_key;
    start_key.key = key_ptr;
    start_key.length = calculate_key_len(table, active_index, keypart_map);
    start_key.keypart_map = keypart_map;
    start_key.flag = find_flag;

    rc = sdb_create_condition_from_key(table, key_info, &start_key, end_range,
                                       0, (NULL != end_range) ? eq_range : 0,
                                       condition_idx);
    if (0 != rc) {
      SDB_LOG_ERROR("Fail to build index match object. rc: %d", rc);
      goto error;
    }

    order_direction = sdb_get_key_direction(find_flag);
  }

  if (!condition.isEmpty()) {
    if (!condition_idx.isEmpty()) {
      bson::BSONArrayBuilder arr_builder;
      arr_builder.append(condition);
      arr_builder.append(condition_idx);
      condition = BSON("$and" << arr_builder.arr());
    }
  } else {
    condition = condition_idx;
  }

  rc = index_read_one(condition, order_direction, buf);
  if (rc) {
    goto error;
  }

done:
  return rc;
error:
  goto done;
}

int ha_sdb::index_read_one(bson::BSONObj condition, int order_direction,
                           uchar *buf) {
  int rc = 0;
  bson::BSONObj hint;
  bson::BSONObj order_by;
  int flag = 0;
  KEY *key_info = table->key_info + active_index;

  DBUG_ASSERT(NULL != collection);
  DBUG_ASSERT(collection->thread_id() == ha_thd()->thread_id());
  DBUG_ASSERT(NULL != key_info);
  DBUG_ASSERT(NULL != key_info->name);

  hint = BSON("" << key_info->name);

  idx_order_direction = order_direction;
  rc = sdb_get_idx_order(key_info, order_by, order_direction);
  if (rc) {
    SDB_LOG_ERROR("Fail to get index order. rc: %d", rc);
    goto error;
  }

  flag = get_query_flag(thd_sql_command(ha_thd()), m_lock_type);
  rc =
      collection->query(condition, SDB_EMPTY_BSON, order_by, hint, 0, -1, flag);
  if (rc) {
    SDB_LOG_ERROR(
        "Collection[%s.%s] failed to query with "
        "condition[%s], order[%s], hint[%s]. rc: %d",
        collection->get_cs_name(), collection->get_cl_name(),
        condition.toString().c_str(), order_by.toString().c_str(),
        hint.toString().c_str(), rc);
    goto error;
  }

  rc = (1 == order_direction) ? index_next(buf) : index_prev(buf);
  switch (rc) {
    case SDB_OK: {
      table->status = 0;
      break;
    }

    case SDB_DMS_EOC:
    case HA_ERR_END_OF_FILE: {
      rc = HA_ERR_KEY_NOT_FOUND;
      table->status = STATUS_NOT_FOUND;
      break;
    }

    default: {
      table->status = STATUS_NOT_FOUND;
      break;
    }
  }
done:
  return rc;
error:
  goto done;
}

int ha_sdb::index_init(uint idx, bool sorted) {
  active_index = idx;
  if (!pushed_cond) {
    pushed_condition = SDB_EMPTY_BSON;
  }
  free_root(&blobroot, MYF(0));
  return 0;
}

int ha_sdb::index_end() {
  DBUG_ASSERT(NULL != collection);
  DBUG_ASSERT(collection->thread_id() == ha_thd()->thread_id());
  collection->close();
  active_index = MAX_KEY;
  return 0;
}

double ha_sdb::scan_time() {
  // TODO*********
  return 10;
}

double ha_sdb::read_time(uint index, uint ranges, ha_rows rows) {
  // TODO********
  return rows;
}

int ha_sdb::rnd_init(bool scan) {
  first_read = true;
  if (!pushed_cond) {
    pushed_condition = SDB_EMPTY_BSON;
  }
  free_root(&blobroot, MYF(0));
  return 0;
}

int ha_sdb::rnd_end() {
  DBUG_ASSERT(NULL != collection);
  DBUG_ASSERT(collection->thread_id() == ha_thd()->thread_id());
  collection->close();
  return 0;
}

int ha_sdb::obj_to_row(bson::BSONObj &obj, uchar *buf) {
  int rc = SDB_ERR_OK;
  THD *thd = table->in_use;
  my_bool is_select = (SQLCOM_SELECT == thd_sql_command(thd));
  memset(buf, 0, table->s->null_bytes);

  // allow zero date
  sql_mode_t old_sql_mode = thd->variables.sql_mode;
  thd->variables.sql_mode &= ~(MODE_NO_ZERO_DATE | MODE_NO_ZERO_IN_DATE);

  // ignore field warning
  enum_check_fields old_check_fields = thd->count_cuted_fields;
  thd->count_cuted_fields = CHECK_FIELD_IGNORE;

  /* Avoid asserts in ::store() for columns that are not going to be updated */
  my_bitmap_map *org_bitmap = dbug_tmp_use_all_columns(table, table->write_set);

  bson::BSONObjIterator iter(obj);

  rc = m_bson_element_cache.ensure(table->s->fields);
  if (SDB_ERR_OK != rc) {
    goto error;
  }

  for (Field **fields = table->field; *fields; fields++) {
    Field *field = *fields;
    bson::BSONElement elem;

    // we only skip non included fields when SELECT.
    if (is_select && !bitmap_is_set(table->read_set, field->field_index)) {
      continue;
    }

    if (!m_bson_element_cache[field->field_index].eoo()) {
      elem = m_bson_element_cache[field->field_index];
    } else {
      while (iter.more()) {
        bson::BSONElement elem_tmp = iter.next();
        if (strcmp(elem_tmp.fieldName(), field->field_name) == 0) {
          // current element match the field
          elem = elem_tmp;
          break;
        }

        if (strcmp(elem_tmp.fieldName(), SDB_OID_FIELD) == 0) {
          // ignore _id
          continue;
        }

        // find matched field to store the element
        for (Field **next_fields = fields + 1; *next_fields; next_fields++) {
          Field *next_field = *next_fields;
          if (strcmp(elem_tmp.fieldName(), next_field->field_name) == 0) {
            m_bson_element_cache[next_field->field_index] = elem_tmp;
            break;
          }
        }
      }
    }

    field->reset();

    if (elem.eoo() || elem.isNull() || bson::Undefined == elem.type()) {
      field->set_null();
      continue;
    }

    rc = bson_element_to_field(elem, field);
    if (0 != rc) {
      goto error;
    }
  }

done:
  dbug_tmp_restore_column_map(table->write_set, org_bitmap);
  thd->count_cuted_fields = old_check_fields;
  thd->variables.sql_mode = old_sql_mode;
  return rc;
error:
  goto done;
}

int ha_sdb::bson_element_to_field(const bson::BSONElement elem, Field *field) {
  int rc = SDB_ERR_OK;

  DBUG_ASSERT(0 == strcmp(elem.fieldName(), field->field_name));

  switch (elem.type()) {
    case bson::NumberInt:
    case bson::NumberLong: {
      longlong nr = elem.numberLong();
      field->store(nr, false);
      break;
    }
    case bson::NumberDouble: {
      double nr = elem.numberDouble();
      field->store(nr);
      break;
    }
    case bson::BinData: {
      int lenTmp = 0;
      const char *dataTmp = elem.binData(lenTmp);
      if (MYSQL_TYPE_JSON != field->type()) {
        field->store(dataTmp, lenTmp, &my_charset_bin);
      } else {
        Field_json *field_json = dynamic_cast<Field_json *>(field);
        json_binary::Value v = json_binary::parse_binary(dataTmp, lenTmp);
        Json_wrapper wr(v);
        field_json->store_json(&wr);
      }
      break;
    }
    case bson::String: {
      // datetime is stored as string
      field->store(elem.valuestr(), elem.valuestrsize() - 1, &SDB_CHARSET);
      break;
    }
    case bson::NumberDecimal: {
      bson::bsonDecimal valTmp = elem.numberDecimal();
      string strValTmp = valTmp.toString();
      field->store(strValTmp.c_str(), strValTmp.length(), &my_charset_bin);
      break;
    }
    case bson::Date: {
      MYSQL_TIME time_val;
      struct timeval tv;
      struct tm tm_val;

      longlong millisec = (longlong)(elem.date());
      tv.tv_sec = millisec / 1000;
      tv.tv_usec = millisec % 1000 * 1000;
      localtime_r((const time_t *)(&tv.tv_sec), &tm_val);

      time_val.year = tm_val.tm_year + 1900;
      time_val.month = tm_val.tm_mon + 1;
      time_val.day = tm_val.tm_mday;
      time_val.hour = 0;
      time_val.minute = 0;
      time_val.second = 0;
      time_val.second_part = 0;
      time_val.neg = 0;
      time_val.time_type = MYSQL_TIMESTAMP_DATE;
      if ((time_val.month < 1 || time_val.day < 1) ||
          (time_val.year > 9999 || time_val.month > 12 || time_val.day > 31)) {
        // Invalid date, the field has been reset to zero,
        // so no need to store.
      } else {
        field->store_time(&time_val, 0);
      }
      break;
    }
    case bson::Timestamp: {
      struct timeval tv;
      longlong millisec = (longlong)(elem.timestampTime());
      longlong microsec = elem.timestampInc();
      tv.tv_sec = millisec / 1000;
      tv.tv_usec = millisec % 1000 * 1000 + microsec;
      field->store_timestamp(&tv);
      break;
    }
    case bson::Object:
    case bson::Bool:
    default:
      rc = SDB_ERR_TYPE_UNSUPPORTED;
      goto error;
  }
  if (field->flags & BLOB_FLAG) {
    Field_blob *blob = (Field_blob *)field;
    uchar *src, *dst;
    uint length, packlength;

    packlength = blob->pack_length_no_ptr();
    length = blob->get_length(blob->ptr);
    memcpy(&src, blob->ptr + packlength, sizeof(char *));
    if (src) {
      dst = (uchar *)alloc_root(&blobroot, length);
      memmove(dst, src, length);
      memcpy(blob->ptr + packlength, &dst, sizeof(char *));
    }
  }

done:
  return rc;
error:
  goto done;
}

int ha_sdb::cur_row(uchar *buf) {
  int rc = 0;

  DBUG_ASSERT(NULL != collection);
  DBUG_ASSERT(collection->thread_id() == ha_thd()->thread_id());

  rc = collection->current(cur_rec);
  if (rc != 0) {
    goto error;
  }

  rc = obj_to_row(cur_rec, buf);
  if (rc != 0) {
    goto error;
  }

done:
  return rc;
error:
  goto done;
}

int ha_sdb::next_row(bson::BSONObj &obj, uchar *buf) {
  int rc = 0;

  DBUG_ASSERT(NULL != collection);
  DBUG_ASSERT(collection->thread_id() == ha_thd()->thread_id());

  rc = collection->next(obj);
  if (rc != 0) {
    if (HA_ERR_END_OF_FILE == rc) {
      table->status = STATUS_NOT_FOUND;
    }
    goto error;
  }

  rc = obj_to_row(obj, buf);
  if (rc != 0) {
    goto error;
  }

  table->status = 0;

done:
  return rc;
error:
  goto done;
}

int ha_sdb::rnd_next(uchar *buf) {
  int rc = 0;

  DBUG_ASSERT(NULL != collection);
  DBUG_ASSERT(collection->thread_id() == ha_thd()->thread_id());

  if (first_read) {
    int flag = get_query_flag(thd_sql_command(ha_thd()), m_lock_type);
    rc = collection->query(pushed_condition, SDB_EMPTY_BSON, SDB_EMPTY_BSON,
                           SDB_EMPTY_BSON, 0, -1, flag);
    if (rc != 0) {
      goto error;
    }
    first_read = false;
  }

  ha_statistic_increment(&SSV::ha_read_rnd_next_count);

  rc = next_row(cur_rec, buf);
  if (rc != 0) {
    goto error;
  }

done:
  return rc;
error:
  goto done;
}

int ha_sdb::rnd_pos(uchar *buf, uchar *pos) {
  int rc = 0;
  bson::BSONObjBuilder objBuilder;
  bson::OID oid;

  DBUG_ASSERT(NULL != collection);
  DBUG_ASSERT(collection->thread_id() == ha_thd()->thread_id());

  memcpy((void *)oid.getData(), pos, SDB_OID_LEN);
  objBuilder.appendOID(SDB_OID_FIELD, &oid);
  bson::BSONObj oidObj = objBuilder.obj();

  rc = collection->query_one(cur_rec, oidObj);
  if (rc) {
    goto error;
  }

  ha_statistic_increment(&SSV::ha_read_rnd_count);
  rc = obj_to_row(cur_rec, buf);
  if (rc != 0) {
    goto error;
  }

done:
  return rc;
error:
  goto done;
}

void ha_sdb::position(const uchar *record) {
  bson::BSONElement beField;
  if (cur_rec.getObjectID(beField)) {
    bson::OID oid = beField.__oid();
    memcpy(ref, oid.getData(), SDB_OID_LEN);
    if (beField.type() != bson::jstOID) {
      SDB_LOG_ERROR("Unexpected _id's type: %d ", beField.type());
    }
  }
  return;
}

int ha_sdb::info(uint flag) {
  int rc = 0;
  long long rec_num = 0;

  rc = ensure_collection(ha_thd());
  if (0 != rc) {
    goto error;
  }

  if (count_times++ % 100 == 0) {
    rc = collection->get_count(rec_num);
    if (rc != 0) {
      goto error;
    }
    last_count_time = time(NULL);
    count_times = 1;
  } else if (count_times % 10 == 0) {
    time_t cur_time = time(NULL);
    // get record count every 5 minutes
    if (difftime(cur_time, last_count_time) > 5 * 60) {
      rc = collection->get_count(rec_num);
      if (rc != 0) {
        goto error;
      }
      last_count_time = cur_time;
      count_times = 1;
    }
  }
  if (count_times != 1) {
    goto done;
  }

  // TODO: fill the stats with actual info.
  stats.data_file_length = (rec_num * 1024) + 32 * 1024 * 1024;
  stats.max_data_file_length = 1099511627776LL;  // 1*1024*1024*1024*1024
  stats.index_file_length = (rec_num * 100) + 32 * 1024 * 1024;
  stats.max_index_file_length = 10737418240LL;  // 10*1024*1024*1024
  stats.delete_length = 0;
  stats.auto_increment_value = 0;
  stats.records = rec_num;
  stats.deleted = 0;
  stats.mean_rec_length = 1024;
  stats.create_time = 0;
  stats.check_time = 0;
  stats.update_time = 0;
  stats.block_size = 0;
  stats.mrr_length_per_rec = 0;
  stats.table_in_mem_estimate = -1;

done:
  return rc;
error:
  goto done;
}

int ha_sdb::extra(enum ha_extra_function operation) {
  // TODO: extra hints
  return 0;
}

int ha_sdb::ensure_collection(THD *thd) {
  int rc = 0;
  DBUG_ASSERT(NULL != thd);

  if (NULL != collection && collection->thread_id() != thd->thread_id()) {
    delete collection;
    collection = NULL;
  }

  if (NULL == collection) {
    Sdb_conn *conn = check_sdb_in_thd(thd, true);
    if (NULL == conn) {
      rc = HA_ERR_NO_CONNECTION;
      goto error;
    }
    DBUG_ASSERT(conn->thread_id() == thd->thread_id());

    collection = new (std::nothrow) Sdb_cl();
    if (NULL == collection) {
      rc = HA_ERR_OUT_OF_MEM;
      goto error;
    }

    conn->get_cl(db_name, table_name, *collection);
    if (0 != rc) {
      delete collection;
      collection = NULL;
      SDB_LOG_ERROR("Collection[%s.%s] is not available. rc: %d", db_name,
                    table_name, rc);
      goto error;
    }
  }

done:
  return rc;
error:
  goto done;
}

int ha_sdb::start_statement(THD *thd, uint table_count) {
  int rc = 0;

  rc = ensure_collection(thd);
  if (0 != rc) {
    goto error;
  }

  if (0 == table_count) {
    Sdb_conn *conn = check_sdb_in_thd(thd, true);
    if (NULL == conn) {
      rc = HA_ERR_NO_CONNECTION;
      goto error;
    }
    DBUG_ASSERT(conn->thread_id() == thd->thread_id());

    if (thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)) {
      if (!conn->is_transaction_on()) {
        rc = conn->begin_transaction();
        if (rc != 0) {
          goto error;
        }
        trans_register_ha(thd, TRUE, ht, NULL);
      }
    } else {
      // autocommit
      if (sdb_use_autocommit && !conn->is_transaction_on()) {
        rc = conn->begin_transaction();
        if (rc != 0) {
          goto error;
        }
        trans_register_ha(thd, FALSE, ht, NULL);
      }
    }
  } else {
    // there is more than one handler involved
  }

done:
  return rc;
error:
  goto done;
}

int ha_sdb::external_lock(THD *thd, int lock_type) {
  int rc = 0;
  Thd_sdb *thd_sdb = NULL;

  if (NULL == check_sdb_in_thd(thd)) {
    rc = HA_ERR_NO_CONNECTION;
    goto error;
  }

  thd_sdb = thd_get_thd_sdb(thd);

  if (F_UNLCK != lock_type) {
    rc = start_statement(thd, thd_sdb->lock_count++);
    if (0 != rc) {
      thd_sdb->lock_count--;
      goto error;
    }
  } else {
    if (!--thd_sdb->lock_count) {
      if (!(thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)) &&
          thd_sdb->get_conn()->is_transaction_on()) {
        /*
          Unlock is done without a transaction commit / rollback.
          This happens if the thread didn't update any rows
          We must in this case close the transaction to release resources
        */
        if (thd->is_error()) {
          rc = thd_sdb->get_conn()->rollback_transaction();
        } else {
          rc = thd_sdb->get_conn()->commit_transaction();
        }
        if (0 != rc) {
          goto error;
        }
      }
    }
  }

done:
  return rc;
error:
  goto done;
}

int ha_sdb::start_stmt(THD *thd, thr_lock_type lock_type) {
  int rc = 0;
  Thd_sdb *thd_sdb = thd_get_thd_sdb(thd);

  m_lock_type = lock_type;
  rc = start_statement(thd, thd_sdb->start_stmt_count++);
  if (0 != rc) {
    thd_sdb->start_stmt_count--;
  }

  return rc;
}

enum_alter_inplace_result ha_sdb::check_if_supported_inplace_alter(
    TABLE *altered_table, Alter_inplace_info *ha_alter_info) {
  enum_alter_inplace_result rs;
  KEY *keyInfo;

  Alter_inplace_info::HA_ALTER_FLAGS inplace_online_operations =
      Alter_inplace_info::ADD_INDEX | Alter_inplace_info::DROP_INDEX |
      Alter_inplace_info::ADD_UNIQUE_INDEX |
      Alter_inplace_info::DROP_UNIQUE_INDEX | Alter_inplace_info::ADD_PK_INDEX |
      Alter_inplace_info::DROP_PK_INDEX |
      Alter_inplace_info::ALTER_COLUMN_NOT_NULLABLE |
      Alter_inplace_info::ALTER_COLUMN_NULLABLE |
      Alter_inplace_info::ADD_COLUMN | Alter_inplace_info::DROP_COLUMN |
      Alter_inplace_info::ALTER_STORED_COLUMN_ORDER |
      Alter_inplace_info::ALTER_STORED_COLUMN_TYPE |
      Alter_inplace_info::ALTER_COLUMN_DEFAULT |
      Alter_inplace_info::ALTER_COLUMN_EQUAL_PACK_LENGTH;

  if (ha_alter_info->handler_flags & ~inplace_online_operations) {
    // include offline-operations
    // rs = handler::check_if_supported_inplace_alter(
    //                           altered_table, ha_alter_info ) ;
    rs = HA_ALTER_INPLACE_NOT_SUPPORTED;
    goto done;
  }

  keyInfo = ha_alter_info->key_info_buffer;
  for (; keyInfo < ha_alter_info->key_info_buffer + ha_alter_info->key_count;
       keyInfo++) {
    KEY_PART_INFO *keyPart;
    KEY_PART_INFO *keyEnd;
    /*if ( ( keyInfo->flags & HA_FULLTEXT )
       || ( keyInfo->flags & HA_PACK_KEY )
       || ( keyInfo->flags & HA_BINARY_PACK_KEY ))
    {
       rs = HA_ALTER_INPLACE_NOT_SUPPORTED ;
       goto done ;
    }*/
    keyPart = keyInfo->key_part;
    keyEnd = keyPart + keyInfo->user_defined_key_parts;
    for (; keyPart < keyEnd; keyPart++) {
      keyPart->field = altered_table->field[keyPart->fieldnr];
      keyPart->null_offset = keyPart->field->null_offset();
      keyPart->null_bit = keyPart->field->null_bit;
      if (keyPart->field->flags & AUTO_INCREMENT_FLAG) {
        rs = HA_ALTER_INPLACE_NOT_SUPPORTED;
        goto done;
      }
    }
  }

  rs = HA_ALTER_INPLACE_NO_LOCK;
done:
  return rs;
}

bool ha_sdb::prepare_inplace_alter_table(TABLE *altered_table,
                                         Alter_inplace_info *ha_alter_info) {
  /*THD *thd = current_thd ;
  bool rs = false ;
  switch( thd_sql_command(thd) )
  {
  case SQLCOM_CREATE_INDEX:
  case SQLCOM_DROP_INDEX:
     rs = false ;
     break ;
  default:
     rs = true ;
     goto error ;
  }
done:
  return rs ;
error:
  goto done ;*/
  return false;
}

int ha_sdb::create_index(Sdb_cl &cl, Alter_inplace_info *ha_alter_info) {
  const KEY *keyInfo;
  int rc = 0;

  for (uint i = 0; i < ha_alter_info->index_add_count; i++) {
    keyInfo =
        &ha_alter_info->key_info_buffer[ha_alter_info->index_add_buffer[i]];
    rc = sdb_create_index(keyInfo, cl);
    if (rc) {
      goto error;
    }
  }
done:
  return rc;
error:
  goto done;
}

int ha_sdb::drop_index(Sdb_cl &cl, Alter_inplace_info *ha_alter_info) {
  int rc = 0;

  if (NULL == ha_alter_info->index_drop_buffer) {
    goto done;
  }

  for (uint i = 0; i < ha_alter_info->index_drop_count; i++) {
    KEY *keyInfo = NULL;
    keyInfo = ha_alter_info->index_drop_buffer[i];
    rc = cl.drop_index(keyInfo->name);
    if (rc) {
      goto error;
    }
  }
done:
  return rc;
error:
  goto done;
}

bool ha_sdb::inplace_alter_table(TABLE *altered_table,
                                 Alter_inplace_info *ha_alter_info) {
  bool rs = false;
  int rc = 0;
  THD *thd = current_thd;
  Sdb_conn *conn = NULL;
  Sdb_cl cl;

  Alter_inplace_info::HA_ALTER_FLAGS inplace_online_addidx =
      Alter_inplace_info::ADD_INDEX | Alter_inplace_info::ADD_UNIQUE_INDEX |
      Alter_inplace_info::ADD_PK_INDEX |
      Alter_inplace_info::ALTER_COLUMN_NOT_NULLABLE;

  Alter_inplace_info::HA_ALTER_FLAGS inplace_online_dropidx =
      Alter_inplace_info::DROP_INDEX | Alter_inplace_info::DROP_UNIQUE_INDEX |
      Alter_inplace_info::DROP_PK_INDEX |
      Alter_inplace_info::ALTER_COLUMN_NULLABLE;

  if (ha_alter_info->handler_flags &
      ~(inplace_online_addidx | inplace_online_dropidx |
        Alter_inplace_info::ADD_COLUMN | Alter_inplace_info::DROP_COLUMN |
        Alter_inplace_info::ALTER_STORED_COLUMN_ORDER |
        Alter_inplace_info::ALTER_STORED_COLUMN_TYPE |
        Alter_inplace_info::ALTER_COLUMN_DEFAULT |
        Alter_inplace_info::ALTER_COLUMN_EQUAL_PACK_LENGTH)) {
    SDB_PRINT_ERROR(HA_ERR_UNSUPPORTED,
                    "Storage engine doesn't support the operation.");
    rs = true;
    goto error;
  }

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

  if (ha_alter_info->handler_flags & inplace_online_dropidx) {
    rc = drop_index(cl, ha_alter_info);
    if (0 != rc) {
      SDB_PRINT_ERROR(ER_GET_ERRNO, ER(ER_GET_ERRNO), rc);
      rs = true;
      goto error;
    }
  }

  if (ha_alter_info->handler_flags & inplace_online_addidx) {
    rc = create_index(cl, ha_alter_info);
    if (0 != rc) {
      SDB_PRINT_ERROR(ER_GET_ERRNO, ER(ER_GET_ERRNO), rc);
      rs = true;
      goto error;
    }
  }

done:
  return rs;
error:
  goto done;
}

int ha_sdb::delete_all_rows() {
  int rc = 0;
  DBUG_ASSERT(NULL != collection);
  DBUG_ASSERT(collection->thread_id() == ha_thd()->thread_id());

  if (collection->is_transaction_on()) {
    rc = collection->del();
    if (0 == rc) {
      stats.records = 0;
    }
    return rc;
  }
  return truncate();
}

int ha_sdb::truncate() {
  int rc = 0;
  DBUG_ASSERT(NULL != collection);
  DBUG_ASSERT(collection->thread_id() == ha_thd()->thread_id());

  rc = collection->truncate();
  if (0 == rc) {
    stats.records = 0;
  }
  return rc;
}

ha_rows ha_sdb::records_in_range(uint inx, key_range *min_key,
                                 key_range *max_key) {
  // TODO*********
  return 1;
}

int ha_sdb::delete_table(const char *from) {
  int rc = 0;
  Sdb_conn *conn = NULL;

  rc = sdb_parse_table_name(from, db_name, SDB_CS_NAME_MAX_SIZE, table_name,
                            SDB_CL_NAME_MAX_SIZE);
  if (rc != 0) {
    goto error;
  }

  if (sdb_is_tmp_table(from, table_name)) {
    if (0 != sdb_rebuild_db_name_of_temp_table(db_name, SDB_CS_NAME_MAX_SIZE)) {
      rc = HA_ERR_GENERIC;
      goto error;
    }
  }

  conn = check_sdb_in_thd(ha_thd(), true);
  if (NULL == conn) {
    rc = HA_ERR_NO_CONNECTION;
    goto error;
  }
  DBUG_ASSERT(conn->thread_id() == ha_thd()->thread_id());

  rc = conn->drop_cl(db_name, table_name);
  if (0 != rc) {
    goto error;
  }

done:
  return rc;
error:
  goto done;
}

int ha_sdb::rename_table(const char *from, const char *to) {
  Sdb_conn *conn = NULL;
  int rc = 0;

  char old_db_name[SDB_CS_NAME_MAX_SIZE + 1] = {0};
  char old_table_name[SDB_CL_NAME_MAX_SIZE + 1] = {0};
  char new_db_name[SDB_CS_NAME_MAX_SIZE + 1] = {0};
  char new_table_name[SDB_CL_NAME_MAX_SIZE + 1] = {0};

  rc = sdb_parse_table_name(from, old_db_name, SDB_CS_NAME_MAX_SIZE,
                            old_table_name, SDB_CL_NAME_MAX_SIZE);
  if (0 != rc) {
    goto error;
  }

  rc = sdb_parse_table_name(to, new_db_name, SDB_CS_NAME_MAX_SIZE,
                            new_table_name, SDB_CL_NAME_MAX_SIZE);
  if (0 != rc) {
    goto error;
  }

  if (sdb_is_tmp_table(from, old_table_name)) {
    rc = sdb_rebuild_db_name_of_temp_table(old_db_name, SDB_CS_NAME_MAX_SIZE);
    if (0 != rc) {
      goto error;
    }
  }

  if (sdb_is_tmp_table(to, new_table_name)) {
    rc = sdb_rebuild_db_name_of_temp_table(new_db_name, SDB_CS_NAME_MAX_SIZE);
    if (0 != rc) {
      goto error;
    }
  }

  if (strcmp(old_db_name, new_db_name) != 0) {
    rc = HA_ERR_NOT_ALLOWED_COMMAND;
    goto error;
  }

  conn = check_sdb_in_thd(ha_thd(), true);
  if (NULL == conn) {
    rc = HA_ERR_NO_CONNECTION;
    goto error;
  }
  DBUG_ASSERT(conn->thread_id() == ha_thd()->thread_id());

  rc = conn->rename_cl(old_db_name, old_table_name, new_table_name);
  if (0 != rc) {
    goto error;
  }

done:
  return rc;
error:
  goto done;
}

int ha_sdb::get_sharding_key(TABLE *form, bson::BSONObj &sharding_key) {
  int rc = 0;
  const KEY *shard_idx = NULL;

  for (uint i = 0; i < form->s->keys; i++) {
    const KEY *key_info = form->s->key_info + i;
    if (!strcmp(key_info->name, primary_key_name)) {
      shard_idx = key_info;
      break;
    }
    if (NULL == shard_idx && (key_info->flags & HA_NOSAME)) {
      shard_idx = key_info;
    }
  }
  if (NULL != shard_idx) {
    bson::BSONObjBuilder sharding_key_builder;
    const KEY_PART_INFO *key_part;
    const KEY_PART_INFO *key_end;

    // check unique-idx if include sharding-key
    for (uint i = 0; i < form->s->keys; i++) {
      const KEY *key_info = form->s->key_info + i;
      if ((key_info->flags & HA_NOSAME) && key_info != shard_idx) {
        key_part = shard_idx->key_part;
        key_end = key_part + shard_idx->user_defined_key_parts;
        for (; key_part != key_end; ++key_part) {
          const KEY_PART_INFO *key_part_tmp = key_info->key_part;
          const KEY_PART_INFO *key_end_tmp =
              key_part_tmp + key_info->user_defined_key_parts;
          for (; key_part_tmp != key_end_tmp; ++key_part_tmp) {
            if (0 == strcmp(key_part->field->field_name,
                            key_part_tmp->field->field_name)) {
              break;
            }
          }

          if (key_part_tmp == key_end_tmp) {
            rc = SDB_ERR_INVALID_ARG;
            SDB_PRINT_ERROR(
                rc,
                "The unique index('%-.192s') must include the field: '%-.192s'",
                key_info->name, key_part->field->field_name);
            goto error;
          }
        }
      }
    }

    key_part = shard_idx->key_part;
    key_end = key_part + shard_idx->user_defined_key_parts;
    for (; key_part != key_end; ++key_part) {
      sharding_key_builder.append(key_part->field->field_name, 1);
    }
    sharding_key = sharding_key_builder.obj();
  } else {
    Field **field = form->field;
    if (*field) {
      sharding_key = BSON((*field)->field_name << 1);
    }
  }

done:
  return rc;
error:
  goto done;
}

int ha_sdb::get_cl_options(TABLE *form, HA_CREATE_INFO *create_info,
                           bson::BSONObj &options, my_bool use_partition) {
  int rc = 0;
  bson::BSONObj sharding_key;

  if (create_info && create_info->comment.str) {
    bson::BSONElement beOptions;
    bson::BSONObj comments;

    rc = bson::fromjson(create_info->comment.str, comments);
    if (0 != rc) {
      SDB_PRINT_ERROR(rc, "Failed to parse comment: '%-.192s'",
                      create_info->comment.str);
      goto error;
    }

    beOptions = comments.getField("table_options");
    if (beOptions.type() == bson::Object) {
      options = beOptions.embeddedObject().copy();
      goto done;
    } else if (beOptions.type() != bson::EOO) {
      rc = SDB_ERR_INVALID_ARG;
      SDB_PRINT_ERROR(rc, "Failed to parse cl_options!");
      goto error;
    }
  }

  if (!use_partition) {
    goto done;
  }

  rc = get_sharding_key(form, sharding_key);
  if (rc != 0) {
    goto error;
  }

  if (!sharding_key.isEmpty()) {
    options = BSON("ShardingKey" << sharding_key << "AutoSplit" << true
                                 << "EnsureShardingIndex" << false
                                 << "Compressed" << true << "CompressionType"
                                 << "lzw");
  }

done:
  return rc;
error:
  goto done;
}

int ha_sdb::create(const char *name, TABLE *form, HA_CREATE_INFO *create_info) {
  int rc = 0;
  Sdb_conn *conn = NULL;
  Sdb_cl cl;
  bool create_temporary = (create_info->options & HA_LEX_CREATE_TMP_TABLE);
  bson::BSONObj options;

  for (Field **fields = form->field; *fields; fields++) {
    Field *field = *fields;

    if (field->key_length() >= SDB_FIELD_MAX_LEN) {
      my_error(ER_TOO_BIG_FIELDLENGTH, MYF(0), field->field_name,
               static_cast<ulong>(SDB_FIELD_MAX_LEN));
      rc = HA_WRONG_CREATE_OPTION;
      goto error;
    }

    if (strcasecmp(field->field_name, SDB_OID_FIELD) == 0) {
      my_error(ER_WRONG_COLUMN_NAME, MYF(0), field->field_name);
      rc = HA_WRONG_CREATE_OPTION;
      goto error;
    }

    if (Field::NEXT_NUMBER == field->unireg_check) {
      // TODO: support auto-increment field.
      //      it is auto-increment field if run here.
      //      the start-value is create_info->auto_increment_value
    }
  }

  rc = sdb_parse_table_name(name, db_name, SDB_CS_NAME_MAX_SIZE, table_name,
                            SDB_CL_NAME_MAX_SIZE);
  if (0 != rc) {
    goto error;
  }

  if (create_temporary) {
    if (0 != sdb_rebuild_db_name_of_temp_table(db_name, SDB_CS_NAME_MAX_SIZE)) {
      rc = HA_WRONG_CREATE_OPTION;
      goto error;
    }
  }

  rc = get_cl_options(form, create_info, options, sdb_use_partition);
  if (0 != rc) {
    goto error;
  }

  conn = check_sdb_in_thd(ha_thd(), true);
  if (NULL == conn) {
    rc = HA_ERR_NO_CONNECTION;
    goto error;
  }
  DBUG_ASSERT(conn->thread_id() == ha_thd()->thread_id());

  rc = conn->create_cl(db_name, table_name, options);
  if (0 != rc) {
    goto error;
  }

  rc = conn->get_cl(db_name, table_name, cl);
  if (0 != rc) {
    goto error;
  }

  for (uint i = 0; i < form->s->keys; i++) {
    rc = sdb_create_index(form->s->key_info + i, cl);
    if (0 != rc) {
      // we disabled sharding index,
      // so do not ignore SDB_IXM_EXIST_COVERD_ONE
      goto error;
    }
  }

done:
  return rc;
error:
  convert_sdb_code(rc);
  goto done;
}

THR_LOCK_DATA **ha_sdb::store_lock(THD *thd, THR_LOCK_DATA **to,
                                   enum thr_lock_type lock_type) {
  /**
    In this function, we can get the MySQL lock by parameter lock_type,
    and tell MySQL which lock we can support by return a new THR_LOCK_DATA.
    Then, we can change MySQL behavior of mutexes.
  */
  m_lock_type = lock_type;
  return to;
}

void ha_sdb::unlock_row() {
  // TODO: this operation is not supported in sdb.
  //       unlock by _id or completed-record?
}

int ha_sdb::get_query_flag(const uint sql_command,
                           enum thr_lock_type lock_type) {
  /*
    We always add flag QUERY_WITH_RETURNDATA to improve performance,
    and we need to add the lock related flag QUERY_FOR_UPDATE in the following
    cases:
    1. SELECT ... FOR UPDATE
    2. doing query in UPDATE ... or DELETE ...
    3. SELECT ... LOCK IN SHARE MODE
  */
  int query_flag = QUERY_WITH_RETURNDATA;
  if ((lock_type >= TL_WRITE_CONCURRENT_INSERT &&
       (SQLCOM_UPDATE == sql_command || SQLCOM_DELETE == sql_command ||
        SQLCOM_SELECT == sql_command)) ||
      TL_READ_WITH_SHARED_LOCKS == lock_type) {
    query_flag |= QUERY_FOR_UPDATE;
  }
  return query_flag;
}

const Item *ha_sdb::cond_push(const Item *cond) {
  const Item *remain_cond = cond;
  Sdb_cond_ctx sdb_condition;

  // we can handle the condition which only involved current table,
  // can't handle conditions which involved other tables
  if (cond->used_tables() & ~table->pos_in_table_list->map()) {
    goto done;
  }

  try {
    sdb_parse_condtion(cond, &sdb_condition);
    sdb_condition.to_bson(pushed_condition);
  } catch (bson::assertion e) {
    SDB_LOG_DEBUG("Exception[%s] occurs when build bson obj.", e.full.c_str());
    DBUG_ASSERT(0);
    sdb_condition.status = SDB_COND_UNSUPPORTED;
  }

  if (SDB_COND_SUPPORTED == sdb_condition.status) {
    // TODO: build unanalysable condition
    remain_cond = NULL;
  } else {
    if (NULL != ha_thd()) {
      SDB_LOG_DEBUG(
          "Condition can't be pushed down. db=[%s], table[%s], sql=[%s]",
          db_name, table_name, ha_thd()->query().str);
    } else {
      SDB_LOG_DEBUG(
          "Condition can't be pushed down. "
          "db=[unknown], table[unknown], sql=[unknown]");
    }
    pushed_condition = SDB_EMPTY_BSON;
  }
done:
  return remain_cond;
}

Item *ha_sdb::idx_cond_push(uint keyno, Item *idx_cond) {
  return idx_cond;
}

static handler *sdb_create_handler(handlerton *hton, TABLE_SHARE *table,
                                   MEM_ROOT *mem_root) {
  return new (mem_root) ha_sdb(hton, table);
}

#ifdef HAVE_PSI_INTERFACE

static PSI_memory_info all_sdb_memory[] = {
    {&key_memory_sdb_share, "Sdb_share", PSI_FLAG_GLOBAL},
    {&sdb_key_memory_blobroot, "blobroot", 0}};

static PSI_mutex_info all_sdb_mutexes[] = {
    {&key_mutex_sdb, "sdb", PSI_FLAG_GLOBAL},
    {&key_mutex_SDB_SHARE_mutex, "Sdb_share::mutex", 0}};

static void init_sdb_psi_keys(void) {
  const char *category = "sequoiadb";
  int count;

  count = array_elements(all_sdb_mutexes);
  mysql_mutex_register(category, all_sdb_mutexes, count);

  count = array_elements(all_sdb_memory);
  mysql_memory_register(category, all_sdb_memory, count);
}
#endif

// Commit a transaction started in SequoiaDB.
static int sdb_commit(handlerton *hton, THD *thd, bool all) {
  int rc = 0;
  Thd_sdb *thd_sdb = thd_get_thd_sdb(thd);
  Sdb_conn *connection;

  thd_sdb->start_stmt_count = 0;

  connection = check_sdb_in_thd(thd, true);
  if (NULL == connection) {
    rc = HA_ERR_NO_CONNECTION;
    goto error;
  }
  DBUG_ASSERT(connection->thread_id() == thd->thread_id());

  if (!connection->is_transaction_on()) {
    goto done;
  }

  if (!all && thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)) {
    /*
      An odditity in the handler interface is that commit on handlerton
      is called to indicate end of statement only in cases where
      autocommit isn't used and the all flag isn't set.

      We also leave quickly when a transaction haven't even been started,
      in this case we are safe that no clean up is needed. In this case
      the MySQL Server could handle the query without contacting the
      SequoiaDB.
    */
    thd_sdb->save_point_count++;
    goto done;
  }
  thd_sdb->save_point_count = 0;

  rc = connection->commit_transaction();
  if (0 != rc) {
    goto error;
  }

done:
  return rc;
error:
  goto done;
}

// Rollback a transaction started in SequoiaDB.
static int sdb_rollback(handlerton *hton, THD *thd, bool all) {
  int rc = 0;
  Thd_sdb *thd_sdb = thd_get_thd_sdb(thd);
  Sdb_conn *connection;

  thd_sdb->start_stmt_count = 0;

  connection = check_sdb_in_thd(thd, true);
  if (NULL == connection) {
    rc = HA_ERR_NO_CONNECTION;
    goto error;
  }
  DBUG_ASSERT(connection->thread_id() == thd->thread_id());

  if (!connection->is_transaction_on()) {
    goto done;
  }

  if (!all && thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN) &&
      (thd_sdb->save_point_count > 0)) {
    /*
      Ignore end-of-statement until real rollback or commit is called
      as SequoiaDB does not support rollback statement
      - mark that rollback was unsuccessful, this will cause full rollback
      of the transaction
    */
    thd_mark_transaction_to_rollback(thd, 1);
    my_error(ER_WARN_ENGINE_TRANSACTION_ROLLBACK, MYF(0), "SequoiaDB");
    goto done;
  }
  thd_sdb->save_point_count = 0;

  rc = connection->rollback_transaction();
  if (0 != rc) {
    goto error;
  }

done:
  return rc;
error:
  goto done;
}

static void sdb_drop_database(handlerton *hton, char *path) {
  int rc = 0;
  char db_name[SDB_CS_NAME_MAX_SIZE + 1] = {0};
  Sdb_conn *connection = NULL;
  THD *thd = current_thd;
  if (NULL == thd) {
    goto error;
  }

  connection = check_sdb_in_thd(thd, true);
  if (NULL == connection) {
    rc = HA_ERR_NO_CONNECTION;
    goto error;
  }
  DBUG_ASSERT(connection->thread_id() == thd->thread_id());

  rc = sdb_get_db_name_from_path(path, db_name, SDB_CS_NAME_MAX_SIZE);
  if (rc != 0) {
    goto error;
  }

  rc = connection->drop_cs(db_name);
  if (rc != 0) {
    goto error;
  }

done:
  return;
error:
  goto done;
}

static int sdb_close_connection(handlerton *hton, THD *thd) {
  Thd_sdb *thd_sdb = thd_get_thd_sdb(thd);
  if (NULL != thd_sdb) {
    Thd_sdb::release(thd_sdb);
    thd_set_thd_sdb(thd, NULL);
  }
  return 0;
}

static int sdb_init_func(void *p) {
  int rc = SDB_ERR_OK;
  Sdb_conn_addrs conn_addrs;
#ifdef HAVE_PSI_INTERFACE
  init_sdb_psi_keys();
#endif
  sdb_hton = (handlerton *)p;
  mysql_mutex_init(key_mutex_sdb, &sdb_mutex, MY_MUTEX_INIT_FAST);
  (void)my_hash_init(&sdb_open_tables, system_charset_info, 32, 0, 0,
                     (my_hash_get_key)sdb_get_key, 0, 0, key_memory_sdb_share);
  sdb_hton->state = SHOW_OPTION_YES;
  sdb_hton->db_type = DB_TYPE_UNKNOWN;
  sdb_hton->create = sdb_create_handler;
  sdb_hton->flags = (HTON_SUPPORT_LOG_TABLES | HTON_NO_PARTITION);
  sdb_hton->commit = sdb_commit;
  sdb_hton->rollback = sdb_rollback;
  sdb_hton->drop_database = sdb_drop_database;
  sdb_hton->close_connection = sdb_close_connection;
  if (conn_addrs.parse_conn_addrs(sdb_conn_str)) {
    SDB_LOG_ERROR("Invalid value sequoiadb_conn_addr=%s", sdb_conn_str);
    return 1;
  }

  rc = sdb_encrypt_password();
  if (SDB_ERR_OK != rc) {
    SDB_LOG_ERROR("Failed to encrypt password, rc=%d", rc);
    return 1;
  }

  return 0;
}

static int sdb_done_func(void *p) {
  // TODO************
  // SHOW_COMP_OPTION state;
  my_hash_free(&sdb_open_tables);
  mysql_mutex_destroy(&sdb_mutex);
  return 0;
}

static struct st_mysql_storage_engine sdb_storage_engine = {
    MYSQL_HANDLERTON_INTERFACE_VERSION};

mysql_declare_plugin(sequoiadb){
    MYSQL_STORAGE_ENGINE_PLUGIN,
    &sdb_storage_engine,
    "SequoiaDB",
    "SequoiaDB Inc.",
    sdb_plugin_info,
    PLUGIN_LICENSE_GPL,
    sdb_init_func, /* Plugin Init */
    sdb_done_func, /* Plugin Deinit */
    0x0300,        /* version */
    NULL,          /* status variables */
    sdb_sys_vars,  /* system variables */
    NULL,          /* config options */
    0,             /* flags */
} mysql_declare_plugin_end;
