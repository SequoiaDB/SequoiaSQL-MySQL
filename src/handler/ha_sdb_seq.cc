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

#ifdef IS_MARIADB

#ifndef MYSQL_SERVER
#define MYSQL_SERVER
#endif

#include "ha_sdb_sql.h"
#include "ha_sdb_seq.h"
#include "ha_sdb_thd.h"
#include "ha_sdb_util.h"
#include "ha_sdb_log.h"

ha_sdb_seq::ha_sdb_seq(handlerton *hton, TABLE_SHARE *table_arg)
    : ha_sdb(hton, table_arg) {
  m_sequence = NULL;
  m_sequence_to_be_set = false;
  m_acquire_low_bound = -1;
  m_acquire_up_bound = -1;
  memset(m_sequence_name, 0, SDB_CL_NAME_MAX_SIZE + 1);
}

int ha_sdb_seq::ensure_sequence(THD *thd) {
  DBUG_ENTER("ha_sdb::ensure_sequence");
  int rc = 0;
  DBUG_ASSERT(NULL != thd);

  if (sdb_execute_only_in_mysql(ha_thd())) {
    goto done;
  }

  if (NULL != m_sequence && m_sequence->thread_id() != sdb_thd_id(thd)) {
    delete m_sequence;
    m_sequence = NULL;
  }

  if (NULL == m_sequence) {
    Sdb_conn *conn = NULL;
    rc = check_sdb_in_thd(thd, &conn, true);
    if (0 != rc) {
      goto error;
    }
    DBUG_ASSERT(conn->thread_id() == sdb_thd_id(thd));

    m_sequence = new (std::nothrow) Sdb_seq();
    if (NULL == m_sequence) {
      rc = HA_ERR_OUT_OF_MEM;
      goto error;
    }

    conn->get_seq(db_name, table_name, m_sequence_name, *m_sequence);
    if (0 != rc) {
      delete m_sequence;
      m_sequence = NULL;
      SDB_LOG_ERROR("Sequence[%s.%s] is not available. rc: %d", m_sequence_name,
                    rc);
      goto error;
    }
  }

done:
  DBUG_PRINT("exit",
             ("sequence %s get Sdb_seq %p", m_sequence_name, m_sequence));
  DBUG_RETURN(rc);
error:
  goto done;
}

/*
  The sequence's created in two steps, creating an empty sequence and inserting
  a record. To reduce interaction, both are performed in ha_sdb_seq::write_row.
*/
int ha_sdb_seq::create(const char *name, TABLE *form,
                       HA_CREATE_INFO *create_info) {
  DBUG_ENTER("ha_sdb_seq::create");

  int rc = 0;
  char *key = NULL;
  int key_len = 0;
  sdb_sequence_cache *seq_cache = NULL;
  bool create_temporary = (create_info->options & HA_LEX_CREATE_TMP_TABLE);

  if (sdb_execute_only_in_mysql(ha_thd())) {
    rc = 0;
    goto done;
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
    key_len = strlen(table_name);
    if (NULL == sdb_multi_malloc(key_memory_sequence_cache,
                                 MYF(MY_WME | MY_ZEROFILL), &seq_cache,
                                 sizeof(sdb_sequence_cache), &key, key_len + 1,
                                 NullS)) {
      goto error;
    }
    snprintf(key, key_len + 1, "%s", table_name);
    key[key_len] = '\0';
    seq_cache->sequence_name = key;
    if (my_hash_insert(&sdb_temporary_sequence_cache, (uchar *)seq_cache)) {
      goto error;
    }
  }

done:
  DBUG_RETURN(rc);
error:
  goto done;
}

int ha_sdb_seq::open(const char *name, int mode, uint test_if_locked) {
  DBUG_ENTER("ha_sdb::open");

  int rc = 0;
  Sdb_seq sdb_seq;
  int fetch_num = 0;
  int increment = 0;
  int return_num = 0;
  longlong next_value = 0;
  Sdb_conn *connection = NULL;
  SEQUENCE *seq = table->s->sequence;

  if (sdb_execute_only_in_mysql(ha_thd())) {
    goto done;
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

  rc = check_sdb_in_thd(ha_thd(), &connection, true);
  if (0 != rc) {
    goto error;
  }
  DBUG_ASSERT(connection->thread_id() == sdb_thd_id(ha_thd()));

  // Get sequence to check if the sequence is available.
  rc = connection->get_seq(db_name, table_name, m_sequence_name, sdb_seq);
  if ((SDB_DMS_CS_NOTEXIST == get_sdb_code(rc) ||
       SDB_SEQUENCE_NOT_EXIST == get_sdb_code(rc)) &&
      thd_sql_command(ha_thd()) == SQLCOM_CREATE_SEQUENCE) {
    rc = SDB_ERR_OK;
    goto error;
  }
  if (0 != rc) {
    SDB_LOG_ERROR("Sequence[%s.%s] is not available. rc: %d", db_name,
                  m_sequence_name, rc);
    goto error;
  }

  // The sequence values need to be retrieved once when mysqld restart.
  fetch_num = seq->cache;
  if (thd_sql_command(ha_thd()) == SQLCOM_SHOW_FIELDS) {
    rc = sdb_seq.fetch(fetch_num, next_value, return_num, increment);
    if (rc) {
      goto error;
    }

    m_sequence_to_be_set = true;
    m_acquire_low_bound = next_value;
    m_acquire_up_bound = next_value + return_num * increment;
  }

done:
  DBUG_RETURN(rc);
error:
  goto done;
}

int ha_sdb_seq::build_attribute_of_sequence(bson::BSONObj &options) {
  int rc = SDB_OK;
  SEQUENCE *seq = table->s->sequence;
  bson::BSONObj option;
  bson::BSONObjBuilder builder(96);
  try {
    builder.append(SDB_FIELD_MIN_VALUE, (longlong)seq->min_value);
    builder.append(SDB_FIELD_MAX_VALUE, (longlong)seq->max_value);
    builder.append(SDB_FIELD_START_VALUE, (longlong)seq->start);
    builder.append(SDB_FIELD_INCREMENT, (longlong)seq->increment);
    builder.append(SDB_FIELD_ACQUIRE_SIZE, (longlong)seq->cache);
    builder.append(SDB_FIELD_CACHE_SIZE, (longlong)seq->cache);
    builder.appendBool(SDB_FIELD_CYCLED, seq->cycle);

    options = builder.obj();
  }
  SDB_EXCEPTION_CATCHER(rc,
                        "Failed to build attribute obj of sequence, "
                        "table:%s.%s, exception:%s",
                        db_name, table_name, e.what());

done:
  return rc;
error:
  goto done;
}

int ha_sdb_seq::write_row(uchar *buf) {
  int rc = 0;
  Sdb_seq seq;
  Sdb_conn *conn = NULL;
  bool created_cs = false;
  bool created_seq = false;
  bson::BSONObj options;

  if (sdb_execute_only_in_mysql(ha_thd())) {
    goto done;
  }

  rc = check_sdb_in_thd(ha_thd(), &conn, true);
  if (0 != rc) {
    goto error;
  }
  DBUG_ASSERT(conn->thread_id() == sdb_thd_id(ha_thd()));

  rc = build_attribute_of_sequence(options);
  if (rc) {
    goto error;
  }

  rc = conn->create_seq(db_name, table_name, m_sequence_name, options,
                        &created_cs, &created_seq);
  SDB_LOG_DEBUG("Create sequence: name[%s], options[%s]", m_sequence_name,
                options.toString(false, false).c_str());
  if (rc) {
    goto error;
  }

  rc = conn->get_seq(db_name, table_name, m_sequence_name, seq);
  if (rc) {
    goto error;
  }

  rc = acquire_and_adjust_sequence_value(&seq);
  if (rc) {
    goto error;
  }

done:
  return rc;
error:
  if (created_cs) {
    sdb_drop_empty_cs(*conn, db_name);
  } else if (created_seq) {
    conn->drop_seq(db_name, table_name);
  }
  goto done;
}

int ha_sdb_seq::acquire_and_adjust_sequence_value(Sdb_seq *sdb_seq) {
  int rc = SDB_OK;
  int fetch_num = 0;
  int increment = 0;
  int return_num = 0;
  longlong up_bound = 0;
  longlong next_value = 0;
  SEQUENCE *seq = table->s->sequence;

  fetch_num = seq->cache;
  rc = sdb_seq->fetch(fetch_num, next_value, return_num, increment);
  if (rc) {
    goto error;
  }

  seq->adjust_values(next_value);
  up_bound = next_value + return_num * increment;
  seq->reserved_until = up_bound - seq->increment;

done:
  return rc;
error:
  goto done;
}

int ha_sdb_seq::insert_into_sequence() {
  int rc = SDB_OK;
  longlong nr = -1;
  bson::BSONObj options;
  int fetch_num = 0;
  int increment = 0;
  int return_num = 0;
  longlong next_value = 0;
  bson::BSONObjBuilder builder;
  my_bitmap_map *old_map = NULL;
  SEQUENCE *seq = table->s->sequence;

  old_map = dbug_tmp_use_all_columns(table, table->read_set);
  try {
    nr =
        table->field[SEQUENCE_FIELD_RESERVED_UNTIL]->val_int() - seq->increment;
    builder.append(SDB_FIELD_CURRENT_VALUE, nr);
    builder.append(SDB_FIELD_MIN_VALUE,
                   table->field[SEQUENCE_FIELD_MIN_VALUE]->val_int());
    builder.append(SDB_FIELD_MAX_VALUE,
                   table->field[SEQUENCE_FIELD_MAX_VALUE]->val_int());
    builder.append(SDB_FIELD_START_VALUE,
                   table->field[SEQUENCE_FIELD_START]->val_int());
    builder.append(SDB_FIELD_INCREMENT,
                   table->field[SEQUENCE_FIELD_INCREMENT]->val_int());
    builder.append(SDB_FIELD_ACQUIRE_SIZE,
                   table->field[SEQUENCE_FIELD_CACHE]->val_int());
    builder.append(SDB_FIELD_CACHE_SIZE,
                   table->field[SEQUENCE_FIELD_CACHE]->val_int());
    builder.appendBool(SDB_FIELD_CYCLED,
                       table->field[SEQUENCE_FIELD_CYCLED]->val_int());
    options = builder.obj();
  }
  SDB_EXCEPTION_CATCHER(rc,
                        "Failed to build sequence row, "
                        "table:%s.%s, exception:%s",
                        db_name, table_name, e.what());

  dbug_tmp_restore_column_map(table->read_set, old_map);
  seq->used_fields = ~(uint)0;
  seq->print_dbug();

  rc = m_sequence->set_attributes(options);
  if (rc) {
    goto error;
  }

  fetch_num = seq->cache;
  rc = m_sequence->fetch(fetch_num, next_value, return_num, increment);
  if (rc) {
    goto error;
  }

  m_sequence_to_be_set = true;
  m_acquire_low_bound = next_value;
  m_acquire_up_bound = next_value + return_num * increment;

done:
  return rc;
error:
  goto done;
}

int ha_sdb_seq::alter_sequence() {
  int rc = SDB_OK;
  longlong nr = -1;
  bson::BSONObj options;
  int fetch_num = 0;
  int increment = 0;
  int return_num = 0;
  longlong next_value = 0;
  bson::BSONObjBuilder builder;
  SEQUENCE *seq = table->s->sequence;
  sequence_definition *new_seq = ha_thd()->lex->create_info.seq_create_info;

  try {
    if (seq->reserved_until != new_seq->reserved_until) {
      nr = new_seq->reserved_until - seq->increment;
      builder.append(SDB_FIELD_CURRENT_VALUE, nr);
    }
    if (seq->min_value != new_seq->min_value) {
      builder.append(SDB_FIELD_MIN_VALUE, new_seq->min_value);
    }
    if (seq->max_value != new_seq->max_value) {
      builder.append(SDB_FIELD_MAX_VALUE, new_seq->max_value);
    }
    if (seq->start != new_seq->start) {
      builder.append(SDB_FIELD_START_VALUE, new_seq->start);
    }
    if (seq->increment != new_seq->increment) {
      builder.append(SDB_FIELD_INCREMENT, new_seq->increment);
    }
    if (seq->cache != new_seq->cache) {
      builder.append(SDB_FIELD_ACQUIRE_SIZE, new_seq->cache);
      builder.append(SDB_FIELD_CACHE_SIZE, new_seq->cache);
    }
    if (seq->cycle != new_seq->cycle) {
      builder.appendBool(SDB_FIELD_CYCLED, new_seq->cycle);
    }
    options = builder.obj();
  }
  SDB_EXCEPTION_CATCHER(rc,
                        "Failed to build sequence row, "
                        "table:%s.%s, exception:%s",
                        db_name, table_name, e.what());

  if (!options.isEmpty()) {
    rc = m_sequence->set_attributes(options);
    if (rc) {
      goto error;
    }
  }

  fetch_num = seq->cache;
  rc = m_sequence->fetch(fetch_num, next_value, return_num, increment);
  if (rc) {
    goto error;
  }

  m_sequence_to_be_set = true;
  m_acquire_low_bound = next_value;
  m_acquire_up_bound = next_value + return_num * increment;

done:
  return rc;
error:
  goto done;
}

int ha_sdb_seq::select_sequence() {
  int rc = SDB_OK;
  longlong nr = 0;
  longlong sdb_cur_val = 0;
  bson::BSONObj obj;
  bson::BSONObj condition;
  bson::BSONObjBuilder cond_builder;
  Item *item = NULL;
  Sdb_conn *conn = NULL;
  Item_func_nextval *item_tmp = NULL;
  SEQUENCE *seq = table->s->sequence;
  SELECT_LEX *const select_lex = sdb_lex_first_select(ha_thd());
  List<Item> *const item_list = &select_lex->item_list;
  List_iterator_fast<Item> it(*item_list);

  for (item = it++; item; item = it++) {
    item_tmp = (Item_func_nextval *)item;
    if (0 == strcmp(item_tmp->func_name(), SDB_ITEM_FUN_SET_VAL)) {
      // For SELECT SETVAL().
      rc = check_sdb_in_thd(ha_thd(), &conn, true);
      if (0 != rc) {
        goto error;
      }
      DBUG_ASSERT(conn->thread_id() == sdb_thd_id(ha_thd()));

      cond_builder.append(SDB_FIELD_NAME, m_sequence_name);
      condition = cond_builder.done();

      rc = conn->snapshot(obj, SDB_SNAP_SEQUENCES, condition);
      if (rc) {
        SDB_LOG_ERROR("%s", conn->get_err_msg());
        conn->clear_err_msg();
        SDB_PRINT_ERROR(rc, "Could not get snapshot.");
        goto error;
      }
      sdb_cur_val = obj.getField(SDB_FIELD_CURRENT_VALUE).numberLong();
      nr = seq->increment_value(sdb_cur_val);

      if (seq->reserved_until > nr) {
        nr = seq->reserved_until - seq->increment;
        rc = m_sequence->set_current_value(nr);
        if (rc) {
          goto error;
        }
      }
      rc = acquire_and_adjust_sequence_value(m_sequence);
      if (rc) {
        goto error;
      }
    } else if (0 == strcmp(item_tmp->func_name(), SDB_ITEM_FUN_NEXT_VAL)) {
      // For SELECT NEXTVAL().
      rc = acquire_and_adjust_sequence_value(m_sequence);
      if (rc) {
        goto error;
      }
    }
  }

done:
  return rc;
error:
  goto done;
}

int ha_sdb_seq::update_row(const uchar *old_data, const uchar *new_data) {
  DBUG_ENTER("ha_sdb::update_row");

  int rc = 0;
  TABLE *query_table = ha_thd()->lex->query_tables->table;

  DBUG_ASSERT(NULL != m_sequence);
  DBUG_ASSERT(m_sequence->thread_id() == sdb_thd_id(ha_thd()));
  if (thd_sql_command(ha_thd()) == SQLCOM_INSERT) {
    if (table != query_table) {
      // For NEXTVAL(sequence) when INSERT INTO table.
      rc = acquire_and_adjust_sequence_value(m_sequence);
      if (rc) {
        goto error;
      }
    } else {
      // For INSERT INTO sequence.
      rc = insert_into_sequence();
      if (rc) {
        goto error;
      }
    }
  } else if ((thd_sql_command(ha_thd()) == SQLCOM_ALTER_SEQUENCE)) {
    // For ALTER sequence.
    rc = alter_sequence();
    if (rc) {
      goto error;
    }
  } else if ((thd_sql_command(ha_thd()) == SQLCOM_SELECT)) {
    // For SELECT sequence.
    rc = select_sequence();
    if (rc) {
      goto error;
    }
  }

done:
  DBUG_RETURN(rc);
error:
  goto done;
}

int ha_sdb_seq::rnd_init(bool scan) {
  DBUG_ENTER("ha_sdb::rnd_init()");

  int rc = SDB_ERR_OK;

  if (sdb_execute_only_in_mysql(ha_thd())) {
    goto done;
  }

  rc = ensure_sequence(ha_thd());
  if (rc) {
    goto error;
  }

done:
  DBUG_RETURN(rc);
error:
  goto done;
}

int ha_sdb_seq::external_lock(THD *thd, int lock_type) {
  DBUG_ENTER("ha_sdb::external_lock");

  SEQUENCE *seq = table->s->sequence;

  if (F_UNLCK == lock_type && m_sequence_to_be_set) {
    // update sequence properties when m_sequence_to_be_set.
    m_sequence_to_be_set = false;
    m_acquire_up_bound = m_acquire_up_bound - seq->increment;
    seq->reserved_until = m_acquire_up_bound;
    seq->adjust_values(m_acquire_low_bound);
  }

  DBUG_RETURN(SDB_OK);
}

int ha_sdb_seq::rnd_next(uchar *buf) {
  DBUG_ENTER("ha_sdb::rnd_next()");

  int rc = SDB_OK;
  Sdb_conn *conn = NULL;
  my_bitmap_map *old_map = NULL;
  bson::BSONObj obj;
  bson::BSONObj condition;
  bson::BSONObj selected;
  bson::BSONObjBuilder cond_builder;
  SEQUENCE *seq = table->s->sequence;

  if (sdb_execute_only_in_mysql(ha_thd())) {
    rc = HA_ERR_END_OF_FILE;
    table->status = STATUS_NOT_FOUND;
    goto error;
  }

  DBUG_ASSERT(NULL != m_sequence);
  DBUG_ASSERT(m_sequence->thread_id() == sdb_thd_id(ha_thd()));

  if (buf != table->record[0]) {
    goto done;
  }

  rc = check_sdb_in_thd(ha_thd(), &conn, true);
  if (0 != rc) {
    goto error;
  }
  DBUG_ASSERT(conn->thread_id() == sdb_thd_id(ha_thd()));

  cond_builder.append(SDB_FIELD_NAME, m_sequence_name);
  condition = cond_builder.done();

  rc = conn->snapshot(obj, SDB_SNAP_SEQUENCES, condition);
  if (rc) {
    SDB_LOG_ERROR("%s", conn->get_err_msg());
    conn->clear_err_msg();
    SDB_PRINT_ERROR(rc, "Could not get snapshot.");
    goto error;
  }

  old_map = dbug_tmp_use_all_columns(table, table->write_set);

  /* zero possible delete markers & null bits */
  memcpy(table->record[0], table->s->default_values, table->s->null_bytes);
  {
    bson::BSONObjIterator it(obj);
    while (it.more()) {
      bson::BSONElement elem_tmp = it.next();
      const char *field_name = elem_tmp.fieldName();
      longlong nr = -1;
      if (0 == strcmp(field_name, SDB_FIELD_CURRENT_VALUE)) {
        if (m_sequence_to_be_set &&
            SEQUENCE::SEQ_UNINTIALIZED == seq->initialized) {
          nr = m_acquire_low_bound;
        } else {
          nr = elem_tmp.numberLong();
          nr = seq->increment_value(nr);
        }
        table->field[SEQUENCE_FIELD_RESERVED_UNTIL]->store(nr, false);
      } else if (0 == strcmp(field_name, SDB_FIELD_MIN_VALUE)) {
        longlong nr = elem_tmp.numberLong();
        table->field[SEQUENCE_FIELD_MIN_VALUE]->store(nr, false);
      } else if (0 == strcmp(field_name, SDB_FIELD_MAX_VALUE)) {
        longlong nr = elem_tmp.numberLong();
        table->field[SEQUENCE_FIELD_MAX_VALUE]->store(nr, false);
      } else if (0 == strcmp(field_name, SDB_FIELD_START_VALUE)) {
        longlong nr = elem_tmp.numberLong();
        table->field[SEQUENCE_FIELD_START]->store(nr, false);
      } else if (0 == strcmp(field_name, SDB_FIELD_INCREMENT)) {
        longlong nr = elem_tmp.numberLong();
        table->field[SEQUENCE_FIELD_INCREMENT]->store(nr, false);
      } else if (0 == strcmp(field_name, SDB_FIELD_ACQUIRE_SIZE)) {
        longlong nr = elem_tmp.numberLong();
        table->field[SEQUENCE_FIELD_CACHE]->store(nr, false);
      } else if (0 == strcmp(field_name, SDB_FIELD_CYCLED)) {
        bool val = elem_tmp.boolean();
        table->field[SEQUENCE_FIELD_CYCLED]->store(val ? 1 : 0, true);
      } else if (0 == strcmp(field_name, SDB_FIELD_CYCLED_COUNT)) {
        longlong nr = elem_tmp.numberLong();
        table->field[SEQUENCE_FIELD_CYCLED_ROUND]->store(nr, false);
      }
    }
  }
  dbug_tmp_restore_column_map(table->write_set, old_map);

done:
  DBUG_RETURN(rc);
error:
  goto done;
}

int ha_sdb_seq::reset() {
  DBUG_ENTER("ha_sdb_seq::reset");
  if (m_sequence) {
    delete m_sequence;
    m_sequence = NULL;
  }
  m_acquire_up_bound = -1;
  m_acquire_low_bound = -1;
  m_sequence_to_be_set = false;
  DBUG_RETURN(ha_sdb::close());
}

int ha_sdb_seq::close() {
  DBUG_ENTER("ha_sdb_seq::close");

  if (NULL != collection) {
    delete collection;
    collection = NULL;
  }
  DBUG_RETURN(0);
}

#endif  // IS_MARIADB
