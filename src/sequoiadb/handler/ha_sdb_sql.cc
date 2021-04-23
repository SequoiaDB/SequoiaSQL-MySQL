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

#include "ha_sdb_sql.h"
#include <sql_class.h>
#include <sql_select.h>
#include <sql_time.h>
#include <sql_update.h>

#ifdef IS_MYSQL
#include <my_thread_local.h>
#include <table_trigger_dispatcher.h>
#endif

#ifdef IS_MARIADB
void repoint_field_to_record(TABLE *table, uchar *old_rec, uchar *new_rec) {
  Field **fields = table->field;
  my_ptrdiff_t ptrdiff = new_rec - old_rec;
  for (uint i = 0; i < table->s->fields; i++)
    fields[i]->move_field_offset(ptrdiff);
}

int my_decimal2string(uint mask, const my_decimal *d, uint fixed_prec,
                      uint fixed_dec, char filler, String *str) {
  return d->to_string_native(str, fixed_prec, fixed_dec, filler, mask);
}

uint calculate_key_len(TABLE *table, uint key, key_part_map keypart_map) {
  return calculate_key_len(table, key, NULL, keypart_map);
}

void trans_register_ha(THD *thd, bool all, handlerton *ht_arg,
                       const ulonglong *trxid) {
  trans_register_ha(thd, all, ht_arg);
}
#endif

#if defined IS_MYSQL
void sdb_init_alloc_root(MEM_ROOT *mem_root, PSI_memory_key key,
                         const char *name, size_t block_size,
                         size_t pre_alloc_size MY_ATTRIBUTE((unused))) {
  init_alloc_root(key, mem_root, block_size, pre_alloc_size);
}

my_thread_id sdb_thd_id(THD *thd) {
  return thd->thread_id();
}

void sdb_mark_transaction_to_rollback(MYSQL_THD thd, int all) {
  thd_mark_transaction_to_rollback(thd, all);
}

const char *sdb_field_name(const Field *f) {
  return f->field_name;
}

const char *sdb_item_field_name(const Item_field *f) {
  return f->field_name;
}

const char *sdb_key_name(const KEY *key) {
  return key->name;
}

void sdb_field_get_timestamp(Field *f, struct timeval *tv) {
  int warnings = 0;
  f->get_timestamp(tv, &warnings);
}

void sdb_field_store_time(Field *f, MYSQL_TIME *ltime) {
  f->store_time(ltime, 0);
}

void sdb_field_store_timestamp(Field *f, const struct timeval *tv) {
  f->store_timestamp(tv);
}

table_map sdb_table_map(TABLE *table) {
  return table->pos_in_table_list->map();
}

const char *sdb_thd_query(THD *thd) {
  return thd->query().str;
}

uint sdb_item_arg_count(Item_func_in *item_func) {
  return item_func->arg_count;
}

bool sdb_item_get_date(THD *thd, Item *item, MYSQL_TIME *ltime,
                       date_mode_t flags) {
  return item->get_date(ltime, flags);
}

int sdb_aes_encrypt(enum my_aes_mode mode, const uchar *key, uint klen,
                    const String &src, String &dst, const uchar *iv,
                    uint ivlen) {
  int rc = 0;
  int real_enc_len = 0;
  int dst_len = sdb_aes_get_size(mode, src.length());

  if (dst.alloc(dst_len)) {
    rc = HA_ERR_OUT_OF_MEM;
    goto error;
  }

  dst.set_charset(&my_charset_bin);
  real_enc_len = my_aes_encrypt((uchar *)src.ptr(), src.length(),
                                (uchar *)dst.c_ptr(), key, klen, mode, iv);
  dst.length(real_enc_len);

  if (real_enc_len != dst_len) {
    // Bad parameters.
    rc = ER_WRONG_ARGUMENTS;
    goto error;
  }

done:
  return rc;
error:
  goto done;
}

int sdb_aes_decrypt(enum my_aes_mode mode, const uchar *key, uint klen,
                    const String &src, String &dst, const uchar *iv,
                    uint ivlen) {
  int rc = 0;
  int real_dec_len = 0;

  if (dst.alloc(src.length() + 1)) {
    rc = HA_ERR_OUT_OF_MEM;
    goto error;
  }

  dst.set_charset(&my_charset_bin);
  real_dec_len = my_aes_decrypt((uchar *)src.ptr(), src.length(),
                                (uchar *)dst.c_ptr(), key, klen, mode, iv);
  if (real_dec_len < 0) {
    // Bad parameters.
    rc = ER_WRONG_ARGUMENTS;
    goto error;
  }
  dst.length(real_dec_len);
  dst[real_dec_len] = 0;

done:
  return rc;
error:
  goto done;
}

uint sdb_aes_get_size(enum my_aes_opmode AES_OPMODE, uint slen) {
  return my_aes_get_size(slen, AES_OPMODE);
}

bool sdb_datetime_to_timeval(THD *thd, const MYSQL_TIME *ltime,
                             struct timeval *tm, int *error_code) {
  return datetime_to_timeval(thd, ltime, tm, error_code);
}

void sdb_decimal_to_string(uint mask, const my_decimal *d, uint fixed_prec,
                           uint fixed_dec, char filler, String *str) {
  my_decimal2string(E_DEC_FATAL_ERROR, d, 0, 0, 0, str);
}

ulong sdb_thd_current_row(THD *thd) {
  return thd->get_stmt_da()->current_row_for_condition();
}

SELECT_LEX *sdb_lex_current_select(THD *thd) {
  return thd->lex->current_select();
}

List_iterator<Item> sdb_lex_all_fields(LEX *const lex) {
  return lex->current_select()->all_fields;
}

bool sdb_is_insert_single_value(THD *thd) {
  class Sql_cmd_insert_base *sql_cmd_insert_base = NULL;
  sql_cmd_insert_base =
      dynamic_cast<Sql_cmd_insert_base *>(thd->lex->m_sql_cmd);
  return (sql_cmd_insert_base != NULL &&
          sql_cmd_insert_base->insert_many_values.elements <= 1);
}

SELECT_LEX *sdb_lex_first_select(THD *thd) {
  return thd->lex->select_lex;
}

List<Item> *sdb_update_values_list(THD *thd) {
  Sql_cmd_update *sql_cmd_update = (Sql_cmd_update *)(thd->lex->m_sql_cmd);
  return &sql_cmd_update->update_value_list;
}

SELECT_LEX_UNIT *sdb_lex_unit(THD *thd) {
  return thd->lex->unit;
}

bool sdb_has_update_triggers(TABLE *table) {
  return table->triggers && table->triggers->has_update_triggers();
}

bool sdb_lex_ignore(THD *thd) {
  return thd->lex->is_ignore();
}

bool sdb_is_view(struct TABLE_LIST *table_list) {
  return table_list->is_view();
}

Item *sdb_where_condition(THD *thd) {
  return sdb_lex_first_select(thd)->where_cond();
}

Item *sdb_having_condition(THD *thd) {
  return sdb_lex_first_select(thd)->having_cond();
}

bool sdb_use_distinct(THD *thd) {
  return sdb_lex_first_select(thd)->is_distinct();
}

bool sdb_calc_found_rows(THD *thd) {
  return sdb_lex_first_select(thd)->join->calc_found_rows;
}

bool sdb_use_filesort(THD *thd) {
  JOIN *const join = sdb_lex_first_select(thd)->join;
  QEP_TAB *tab = NULL;
  if (!join->qep_tab) {
    return false;
  }
  if (join->need_tmp) {
    tab = &join->qep_tab[join->primary_tables];
  } else {
    tab = join->qep_tab + join->const_tables;
  }
  return tab->filesort;
}

bool sdb_use_JT_REF_OR_NULL(THD *thd, const TABLE *table) {
  JOIN *const join = sdb_lex_first_select(thd)->join;
  if (!join) {
    return false;
  }
  JOIN_TAB *tab = join->map2table
                      ? join->map2table[table->pos_in_table_list->tableno()]
                      : NULL;
  return JT_REF_OR_NULL == tab->type();
}

sdb_join_type sdb_get_join_type(THD *thd, range_seq_t rseq) {
  sdb_join_type type = SDB_JOIN_UNKNOWN;
  int count = 0;
  QEP_TAB *tab = NULL;
  QUICK_SELECT_I *quick = NULL;
  JOIN *const join = sdb_lex_first_select(thd)->join;
  if (!rseq || !join->qep_tab) {
    goto error;
  }
  if (join->need_tmp) {
    tab = &join->qep_tab[join->primary_tables];
  } else {
    tab = join->qep_tab + join->const_tables;
  }

  // Use JT_REF_OR_NULL.
  if (JT_REF_OR_NULL == tab->type()) {
    type = SDB_JOIN_REF_OR_NULL;
    goto done;
  }
  quick = tab->quick();
  if (quick) {
    // Use INDEX_MERGE.
    if (QUICK_SELECT_I::QS_TYPE_INDEX_MERGE == quick->get_type()) {
      type = SDB_JOIN_INDEX_MERGE_SORT_UNION;
    } else if (QUICK_SELECT_I::QS_TYPE_ROR_INTERSECT == quick->get_type()) {
      type = SDB_JOIN_INDEX_MERGE_ROR_INTERSECT;
    } else if (QUICK_SELECT_I::QS_TYPE_ROR_UNION == quick->get_type()) {
      type = SDB_JOIN_INDEX_MERGE_ROR_UNION;
    }
    if (SDB_JOIN_UNKNOWN != type) {
      goto done;
    }

    // Use MULTI_RANGE.
    if (QUICK_SELECT_I::QS_TYPE_RANGE == quick->get_type()) {
      QUICK_RANGE_SEQ_CTX *ctx = (QUICK_RANGE_SEQ_CTX *)rseq;
      QUICK_RANGE *const *tmp_it = ctx->first;
      while (ctx->first != ctx->last) {
        count++;
        ctx->first++;
      }
      ctx->first = tmp_it;
      if (count > 1) {
        type = SDB_JOIN_MULTI_RANGE;
      }
    }
  }

done:
  return type;
error:
  goto done;
}

bool sdb_judge_index_cover(THD *thd, TABLE *table, uint active_index) {
  bool index_cover = false;
  uint i = 0;
  QEP_TAB *tab = NULL;
  SELECT_LEX *cur_select = sdb_lex_current_select(thd);
  JOIN *join = cur_select->join;

  // not in excute proccess,may be in optimize proccess(Explain type:const)
  if (NULL == join || NULL == join->qep_tab) {
    goto done;
  }

  // distinct not support index_cover
  if (cur_select->is_distinct()) {
    goto done;
  }

  // group by not support index_cover
  if (cur_select->is_grouped()) {
    goto done;
  }

  // is not index query
  if (MAX_KEY == active_index) {
    goto done;
  }

  // not only read key
  if (!table->key_read) {
    goto done;
  }

  // index_merge and filesort not support index_cover
  for (i = 0; i < join->primary_tables; i++) {
    // get current table QEP_TAB
    if (table->pos_in_table_list == join->qep_tab[i].table_ref) {
      tab = join->qep_tab + i;
      if (tab->type() == JT_INDEX_MERGE || tab->keep_current_rowid ||
          tab->filesort) {
        goto done;
      }
      // found table and is not index merge
      index_cover = true;
      break;
    }
  }

done:
  return index_cover;
}

void sdb_clear_const_keys(THD *thd) {
  JOIN *const join = sdb_lex_first_select(thd)->join;
  if (join && join->join_tab) {
    join->join_tab->const_keys.clear_all();
  }
}

st_order *sdb_get_join_order(THD *thd) {
  JOIN *join = sdb_lex_first_select(thd)->join;
  if (!join) {
    return NULL;
  }
  return join->order.order;
}

void sdb_set_join_order(THD *thd, st_order *order) {
  JOIN *join = sdb_lex_first_select(thd)->join;
  if (!join) {
    return;
  }
  join->order.order = order;
}

st_order *sdb_get_join_group_list(THD *thd) {
  JOIN *join = sdb_lex_first_select(thd)->join;
  if (!join) {
    return NULL;
  }
  return join->group_list;
}

void sdb_set_join_group_list(THD *thd, st_order *group_list, bool grouped) {
  JOIN *join = sdb_lex_first_select(thd)->join;
  if (!join) {
    return;
  }
  join->group_list.order = group_list;
  join->grouped = grouped;
}

bool sdb_optimizer_switch_flag(THD *thd, ulonglong flag) {
  return thd->optimizer_switch_flag(flag);
}

const char *sdb_item_name(const Item *cond_item) {
  return cond_item->item_name.ptr();
}

time_round_mode_t sdb_thd_time_round_mode(THD *thd) {
  // mariadb use it control sql_mode, mysql don't have it
  return 0;
}

bool sdb_get_item_time(Item *item_val, THD *thd, MYSQL_TIME *ltime) {
  // For datetime/timestamp, get_time() will truncate the date info.
  // But the day may be useful. So get_date() instead.
  if (MYSQL_TYPE_DATETIME == item_val->field_type() ||
      MYSQL_TYPE_TIMESTAMP == item_val->field_type()) {
    return item_val->get_date(ltime, TIME_FUZZY_DATE);
  }
  return item_val->get_time(ltime);
}

bool sdb_is_current_timestamp(Field *field) {
  return real_type_with_now_as_default(field->real_type()) &&
         field->has_insert_default_function();
}

bool sdb_field_is_gcol(const Field *field) {
  return field->is_gcol();
}

bool sdb_field_is_virtual_gcol(const Field *field) {
  return field->is_virtual_gcol();
}

bool sdb_field_is_stored_gcol(const Field *field) {
  return field->is_gcol() && field->stored_in_db;
}

bool sdb_field_has_insert_def_func(const Field *field) {
  return field->has_insert_default_function();
}

bool sdb_field_has_update_def_func(const Field *field) {
  return field->has_update_default_function();
}

Item *sdb_get_gcol_item(const Field *field) {
  DBUG_ASSERT(field->gcol_info && field->gcol_info->expr_item);
  return field->gcol_info->expr_item;
}

MY_BITMAP *sdb_get_base_columns_map(const Field *field) {
  return &field->gcol_info->base_columns_map;
}

bool sdb_stored_gcol_expr_is_equal(const Field *old_field,
                                   const Field *new_field) {
  LEX_STRING old_expr = old_field->gcol_info->expr_str;
  LEX_STRING new_expr = new_field->gcol_info->expr_str;
  return (old_expr.length == new_expr.length) &&
         0 == strncmp(old_expr.str, new_expr.str, old_expr.length);
}

bool sdb_item_like_escape_is_evaluated(Item *item) {
  return ((Item_func_like *)item)->escape_is_evaluated();
}

uint sdb_filename_to_tablename(const char *from, char *to, size_t to_length,
                               bool stay_quiet) {
  return filename_to_tablename(from, to, to_length
#ifndef DBUG_OFF
                               ,
                               stay_quiet
#endif
  );
}

bool sdb_is_string_item(Item *item) {
  return item->type() == Item::STRING_ITEM;
}

my_bool sdb_hash_init(HASH *hash, CHARSET_INFO *charset,
                      ulong default_array_elements, size_t key_offset,
                      size_t key_length, my_hash_get_key get_key,
                      void (*free_element)(void *), uint flags,
                      PSI_memory_key psi_key) {
  return my_hash_init(hash, charset, default_array_elements, key_offset,
                      key_length, get_key, free_element, flags, psi_key);
}

void sdb_string_free(String *str) {
  str->mem_free();
}

void *sdb_trans_alloc(THD *thd, size_t size) {
  return thd->get_transaction()->allocate_memory(size);
}

const char *sdb_da_message_text(Diagnostics_area *da) {
  return da->message_text();
}

ulong sdb_da_current_statement_cond_count(Diagnostics_area *da) {
  return da->current_statement_cond_count();
}

bool sdb_thd_has_client_capability(THD *thd, ulonglong flag) {
  return thd->get_protocol()->has_client_capability(flag);
}

void sdb_thd_set_not_killed(THD *thd) {
  thd->killed = THD::NOT_KILLED;
}

void sdb_thd_reset_condition_info(THD *thd) {
  thd->get_stmt_da()->reset_condition_info(thd);
}

bool sdb_create_table_like(THD *thd) {
  return (thd->lex->create_info.options & HA_LEX_CREATE_TABLE_LIKE);
}

bool sdb_is_transaction_stmt(THD *thd, bool all) {
  if (all) {
    return thd->get_transaction()->is_active(Transaction_ctx::SESSION);
  } else {
    return thd->get_transaction()->is_active(Transaction_ctx::STMT);
  }
}

void sdb_query_cache_invalidate(THD *thd, bool all) {
  TABLE_LIST *table_list = NULL;
  if (thd_sql_command(thd) == SQLCOM_UPDATE ||
      thd_sql_command(thd) == SQLCOM_DELETE) {
    table_list = thd->lex->select_lex->get_table_list()->updatable_base_table();
  } else {
    table_list = thd->lex->insert_table_leaf;
  }
  query_cache.invalidate_single(thd, table_list,
                                sdb_is_transaction_stmt(thd, all));
}

bool sdb_table_has_gcol(TABLE *table) {
  return table->has_gcol();
}

const char *sdb_table_alias(TABLE *table) {
  return table->alias;
}

uint sdb_tables_in_join(JOIN *join) {
  return join->tables;
}

void sdb_set_timespec(struct timespec &abstime, ulonglong sec) {
  set_timespec(&abstime, sec);
}

bool sdb_has_sql_condition(THD *thd, uint sql_errno) {
  return thd->get_stmt_da()->has_sql_condition(sql_errno);
}

const char *sdb_thd_db(THD *thd) {
  return thd->db().str;
}

Protocol *sdb_thd_protocal(THD *thd) {
  return thd->get_protocol();
}

void sdb_protocal_start_row(Protocol *protocol) {
  return protocol->start_row();
}

bool sdb_protocal_end_row(Protocol *protocol) {
  return protocol->end_row();
}

uint sdb_sql_errno(THD *thd) {
  return thd->get_stmt_da()->mysql_errno();
}

const char *sdb_errno_message(THD *thd) {
  return thd->get_stmt_da()->message_text();
}
#elif defined IS_MARIADB
void sdb_init_alloc_root(MEM_ROOT *mem_root, PSI_memory_key key,
                         const char *name, size_t block_size,
                         size_t pre_alloc_size MY_ATTRIBUTE((unused))) {
  init_alloc_root(mem_root, name, block_size, pre_alloc_size, MYF(0));
}

my_thread_id sdb_thd_id(THD *thd) {
  return thd->thread_id;
}

void sdb_mark_transaction_to_rollback(MYSQL_THD thd, int all) {
  thd_mark_transaction_to_rollback(thd, (bool)all);
}

const char *sdb_field_name(const Field *f) {
  return f->field_name.str;
}

const char *sdb_item_field_name(const Item_field *f) {
  return f->field_name.str;
}

const char *sdb_key_name(const KEY *key) {
  return key->name.str;
}

void sdb_field_get_timestamp(Field *f, struct timeval *tv) {
  tv->tv_sec = f->get_timestamp(f->ptr, (ulong *)&tv->tv_usec);
}

void sdb_field_store_time(Field *f, MYSQL_TIME *ltime) {
  f->store_time(ltime);
}

void sdb_field_store_timestamp(Field *f, const struct timeval *tv) {
  f->store_timestamp(tv->tv_sec, tv->tv_usec);
}

table_map sdb_table_map(TABLE *table) {
  return table->map;
}

const char *sdb_thd_query(THD *thd) {
  return thd->query();
}

uint sdb_item_arg_count(Item_func_in *item_func) {
  return item_func->argument_count();
}

bool sdb_item_get_date(THD *thd, Item *item, MYSQL_TIME *ltime,
                       date_mode_t flags) {
  return item->get_date(thd, ltime, flags);
}

int sdb_aes_encrypt(enum my_aes_mode mode, const uchar *key, uint klen,
                    const String &src, String &dst, const uchar *iv,
                    uint ivlen) {
  int rc = 0;
  uint real_enc_len = 0;
  int dst_len = sdb_aes_get_size(mode, src.length());

  if (dst.alloc(dst_len)) {
    rc = HA_ERR_OUT_OF_MEM;
    goto error;
  }
  dst.set_charset(&my_charset_bin);

  rc = my_aes_crypt(mode, ENCRYPTION_FLAG_ENCRYPT, (uchar *)src.ptr(),
                    (uint)src.length(), (uchar *)dst.c_ptr(), &real_enc_len,
                    key, klen, iv, ivlen);
  dst.length(real_enc_len);

done:
  return rc;
error:
  goto done;
}

int sdb_aes_decrypt(enum my_aes_mode mode, const uchar *key, uint klen,
                    const String &src, String &dst, const uchar *iv,
                    uint ivlen) {
  int rc = 0;
  uint real_dec_len = 0;

  if (dst.alloc(src.length() + 1)) {
    rc = HA_ERR_OUT_OF_MEM;
    goto error;
  }
  dst.set_charset(&my_charset_bin);

  rc = my_aes_crypt(mode, ENCRYPTION_FLAG_DECRYPT, (uchar *)src.ptr(),
                    (uint)src.length(), (uchar *)dst.c_ptr(), &real_dec_len,
                    key, klen, iv, ivlen);
  dst.length(real_dec_len);
  dst[real_dec_len] = 0;

done:
  return rc;
error:
  goto done;
}

uint sdb_aes_get_size(enum my_aes_mode mode, uint slen) {
  return my_aes_get_size(mode, slen);
}

bool sdb_datetime_to_timeval(THD *thd, const MYSQL_TIME *ltime,
                             struct timeval *tm, int *error_code) {
  check_date_with_warn(
      thd, ltime,
      TIME_FUZZY_DATES | TIME_INVALID_DATES | thd->temporal_round_mode(),
      MYSQL_TIMESTAMP_ERROR);
  tm->tv_usec = ltime->second_part;
  return !(tm->tv_sec = TIME_to_timestamp(thd, ltime, (uint *)error_code));
}

void sdb_decimal_to_string(uint mask, const my_decimal *d, uint fixed_prec,
                           uint fixed_dec, char filler, String *str) {
  d->to_string_native(str, 0, 0, 0, E_DEC_FATAL_ERROR);
}

ulong sdb_thd_current_row(THD *thd) {
  return thd->get_stmt_da()->current_row_for_warning();
}

SELECT_LEX *sdb_lex_current_select(THD *thd) {
  return thd->lex->current_select;
}

List_iterator<Item> sdb_lex_all_fields(LEX *const lex) {
  return lex->current_select->item_list;
}

bool sdb_is_insert_single_value(THD *thd) {
  return (thd->lex->many_values.elements <= 1);
}

SELECT_LEX *sdb_lex_first_select(THD *thd) {
  return thd->lex->first_select_lex();
}

List<Item> *sdb_update_values_list(THD *thd) {
  return &thd->lex->value_list;
}

SELECT_LEX_UNIT *sdb_lex_unit(THD *thd) {
  return &thd->lex->unit;
}

bool sdb_has_update_triggers(TABLE *table) {
  return (table->triggers &&
          (table->triggers->has_triggers(TRG_EVENT_UPDATE, TRG_ACTION_BEFORE) ||
           table->triggers->has_triggers(TRG_EVENT_UPDATE, TRG_ACTION_AFTER)));
}

bool sdb_lex_ignore(THD *thd) {
  return thd->lex->ignore;
}

bool sdb_is_view(struct TABLE_LIST *table_list) {
  return table_list->view == NULL ? false : true;
}

Item *sdb_where_condition(THD *thd) {
  return sdb_lex_first_select(thd)->where;
}

Item *sdb_having_condition(THD *thd) {
  return sdb_lex_first_select(thd)->having;
}

bool sdb_use_distinct(THD *thd) {
  return sdb_lex_first_select(thd)->options & SELECT_DISTINCT;
}

bool sdb_calc_found_rows(THD *thd) {
  return sdb_lex_first_select(thd)->join->select_options & OPTION_FOUND_ROWS;
}

bool sdb_use_filesort(THD *thd) {
  JOIN *const join = sdb_lex_first_select(thd)->join;
  if (!join->join_tab) {
    return false;
  }
  JOIN_TAB *tab = join->join_tab + (join->tables_list ? join->const_tables : 0);
  if (join->need_tmp) {
    tab = tab + 1;
  }
  return tab->filesort;
}

bool sdb_use_JT_REF_OR_NULL(THD *thd, const TABLE *table) {
  JOIN_TAB *tab = NULL;
  const JOIN *join = sdb_lex_first_select(thd)->join;
  bool join_two_phase_optimization = true;
  if (!join) {
    return false;
  }
  if (!sdb_lex_first_select(thd)->pushdown_select &&
      join->optimization_state == JOIN::OPTIMIZATION_PHASE_1_DONE) {
    join_two_phase_optimization = true;
  }

  tab = (join_two_phase_optimization ? join->map2table[table->tablenr]
                                     : &join->join_tab[table->tablenr]);
  return JT_REF_OR_NULL == tab->type;
}

sdb_join_type sdb_get_join_type(THD *thd, range_seq_t rseq) {
  sdb_join_type type = SDB_JOIN_UNKNOWN;
  int count = 0;
  SQL_SELECT *select = NULL;
  QUICK_SELECT_I *quick = NULL;
  JOIN *const join = sdb_lex_first_select(thd)->join;
  JOIN_TAB *tab = NULL;
  if (!rseq || !join->join_tab) {
    goto error;
  }
  tab = join->join_tab + (join->tables_list ? join->const_tables : 0);
  if (join->need_tmp) {
    tab = tab + 1;
  }

  // Use JT_REF_OR_NULL.
  if (JT_REF_OR_NULL == tab->type) {
    type = SDB_JOIN_REF_OR_NULL;
    goto done;
  }
  if ((select = tab->select) && (quick = select->quick)) {
    // Use INDEX_MERGE.
    if (QUICK_SELECT_I::QS_TYPE_INDEX_MERGE == quick->get_type()) {
      type = SDB_JOIN_INDEX_MERGE_SORT_UNION;
    } else if (QUICK_SELECT_I::QS_TYPE_INDEX_INTERSECT == quick->get_type()) {
      type = SDB_JOIN_INDEX_MERGE_SORT_INTERSECT;
    } else if (QUICK_SELECT_I::QS_TYPE_ROR_INTERSECT == quick->get_type()) {
      type = SDB_JOIN_INDEX_MERGE_ROR_INTERSECT;
    } else if (QUICK_SELECT_I::QS_TYPE_ROR_UNION == quick->get_type()) {
      type = SDB_JOIN_INDEX_MERGE_ROR_UNION;
    }
    if (SDB_JOIN_UNKNOWN != type) {
      goto done;
    }

    // Use MULTI_RANGE.
    if (QUICK_SELECT_I::QS_TYPE_RANGE == quick->get_type()) {
      QUICK_RANGE_SEQ_CTX *ctx = (QUICK_RANGE_SEQ_CTX *)rseq;
      QUICK_RANGE **tmp_it = ctx->first;
      while (ctx->first != ctx->last) {
        count++;
        ctx->first++;
      }
      ctx->first = tmp_it;
      if (count > 1) {
        type = SDB_JOIN_MULTI_RANGE;
      }
    }
  }

done:
  return type;
error:
  goto done;
}

bool sdb_judge_index_cover(THD *thd, TABLE *table, uint active_index) {
  bool index_cover = false;
  uint i = 0;
  SQL_SELECT *select = NULL;
  QUICK_SELECT_I *quick = NULL;
  JOIN_TAB *tab = NULL;
  JOIN *join = sdb_lex_current_select(thd)->join;

  // not in excute proccess,may be in optimize proccess(Explain type:const)
  if (NULL == join || NULL == join->join_tab) {
    goto done;
  }

  // distinct not support index_cover
  if (sdb_lex_current_select(thd)->options & SELECT_DISTINCT) {
    goto done;
  }

  // group by and aggregated not support index_cover
  if (sdb_lex_current_select(thd)->group_list.elements > 0 ||
      sdb_lex_current_select(thd)->agg_func_used()) {
    goto done;
  }

  // is not index query
  if (MAX_KEY == active_index) {
    goto done;
  }

  // not only read key
  if (!table->file->keyread_enabled()) {
    goto done;
  }

  for (i = 0; i < join->table_count; i++) {
    // get current table JOIN_TAB
    tab = join->map2table[i];
    if (NULL == tab) {
      continue;
    }

    if (table == tab->table) {
      if (tab->keep_current_rowid || tab->filesort) {
        goto done;
      }

      select = tab->select;
      if (NULL != select) {
        quick = select->quick;
      }

      if (NULL != quick) {
        if (QUICK_SELECT_I::QS_TYPE_INDEX_MERGE == quick->get_type() ||
            QUICK_SELECT_I::QS_TYPE_INDEX_INTERSECT == quick->get_type() ||
            QUICK_SELECT_I::QS_TYPE_ROR_INTERSECT == quick->get_type() ||
            QUICK_SELECT_I::QS_TYPE_ROR_UNION == quick->get_type()) {
          goto done;
        }
      }
      // found table and is not index merge
      index_cover = true;
      break;
    }
  }

done:
  return index_cover;
}

void sdb_clear_const_keys(THD *thd) {
  JOIN *const join = sdb_lex_first_select(thd)->join;
  if (join && join->best_ref[0]) {
    join->best_ref[0]->const_keys.clear_all();
  }
}

st_order *sdb_get_join_order(THD *thd) {
  JOIN *join = sdb_lex_first_select(thd)->join;
  if (!join) {
    return NULL;
  }
  return join->order;
}

void sdb_set_join_order(THD *thd, st_order *order) {
  JOIN *join = sdb_lex_first_select(thd)->join;
  if (!join) {
    return;
  }
  join->order = order;
}

st_order *sdb_get_join_group_list(THD *thd) {
  JOIN *join = sdb_lex_first_select(thd)->join;
  if (!join) {
    return NULL;
  }
  return join->group_list;
}

void sdb_set_join_group_list(THD *thd, st_order *group_list, bool grouped) {
  JOIN *join = sdb_lex_first_select(thd)->join;
  if (!join) {
    return;
  }
  join->group_list = group_list;
  join->group = grouped;
}

bool sdb_optimizer_switch_flag(THD *thd, ulonglong flag) {
  return optimizer_flag(thd, flag);
}

const char *sdb_item_name(const Item *cond_item) {
  return cond_item->name.str;
}

time_round_mode_t sdb_thd_time_round_mode(THD *thd) {
  return thd->temporal_round_mode();
}

bool sdb_get_item_time(Item *item_val, THD *thd, MYSQL_TIME *ltime) {
  return item_val->get_time(thd, ltime);
}

bool sdb_is_current_timestamp(Field *field) {
  return (MYSQL_TYPE_DATETIME == field->type() ||
          MYSQL_TYPE_TIMESTAMP == field->type()) &&
         field->has_default_now_unireg_check();
}

bool sdb_field_is_gcol(const Field *field) {
  return field->vcol_info;
}

bool sdb_field_is_virtual_gcol(const Field *field) {
  return !field->stored_in_db();
}

bool sdb_field_is_stored_gcol(const Field *field) {
  return field->vcol_info && field->stored_in_db();
}

bool sdb_field_has_insert_def_func(const Field *field) {
  return field->has_default_now_unireg_check();
}

bool sdb_field_has_update_def_func(const Field *field) {
  return field->unireg_check == Field::TIMESTAMP_UN_FIELD ||
         field->unireg_check == Field::TIMESTAMP_DNUN_FIELD;
}

Item *sdb_get_gcol_item(const Field *field) {
  DBUG_ASSERT(field->vcol_info && field->vcol_info->expr);
  return field->vcol_info->expr;
}

MY_BITMAP *sdb_get_base_columns_map(const Field *field) {
  MY_BITMAP *old_read_set = field->table->read_set;
  field->table->read_set = &field->table->tmp_set;
  bitmap_clear_all(&field->table->tmp_set);
  field->vcol_info->expr->walk(&Item::register_field_in_read_map, 1,
                               field->table);
  field->table->read_set = old_read_set;
  return &field->table->tmp_set;
}

StringBuffer<MAX_FIELD_WIDTH> sdb_parse_stored_gcol_expr(const Field *field) {
  const TABLE *table = field->table;
  const uchar *pos = table->s->vcol_defs.str;
  const uchar *end = pos + table->s->vcol_defs.length;
  Field **field_ptr = table->field - 1;
  StringBuffer<MAX_FIELD_WIDTH> expr_str;
  expr_str.append(PARSE_GCOL_KEYWORD);
  uint expr_length = 0;
  uint field_nr = 0;
  uint name_length = 0;
  while (pos < end) {
    if (table->s->frm_version >= FRM_VER_EXPRESSSIONS) {
      field_nr = uint2korr(pos + 1);
      expr_length = uint2korr(pos + 3);
      name_length = pos[5];
      pos += FRM_VCOL_NEW_HEADER_SIZE + name_length;
      field_ptr = table->field + field_nr;
    }
    expr_str.length(strlen(PARSE_GCOL_KEYWORD));
    expr_str.append((char *)pos, expr_length);
    if (0 == strcmp(sdb_field_name(field), sdb_field_name(*field_ptr))) {
      goto done;
    }
    pos += expr_length;
  }

done:
  return expr_str;
}

bool sdb_stored_gcol_expr_is_equal(const Field *old_field,
                                   const Field *new_field) {
  return 0 == strcmp(sdb_parse_stored_gcol_expr(old_field).c_ptr_safe(),
                     sdb_parse_stored_gcol_expr(new_field).c_ptr_safe());
}

bool sdb_item_like_escape_is_evaluated(Item *item) {
  // mariadb has evaluated escape in sql level
  return true;
}

uint sdb_filename_to_tablename(const char *from, char *to, size_t to_length,
                               bool stay_quiet) {
  return filename_to_tablename(from, to, to_length, stay_quiet);
}

bool sdb_is_string_item(Item *item) {
  return item->type() == Item::CONST_ITEM &&
         (MYSQL_TYPE_STRING == item->field_type() ||
          MYSQL_TYPE_VARCHAR == item->field_type() ||
          MYSQL_TYPE_VAR_STRING == item->field_type());
}

my_bool sdb_hash_init(HASH *hash, CHARSET_INFO *charset,
                      ulong default_array_elements, size_t key_offset,
                      size_t key_length, my_hash_get_key get_key,
                      void (*free_element)(void *), uint flags,
                      PSI_memory_key psi_key) {
  return my_hash_init(hash, charset, default_array_elements, key_offset,
                      key_length, get_key, free_element, flags);
}

void sdb_string_free(String *str) {
  str->free();
}

void *sdb_trans_alloc(THD *thd, size_t size) {
  return thd->trans_alloc(size);
}

const char *sdb_da_message_text(Diagnostics_area *da) {
  return da->message();
}

ulong sdb_da_current_statement_cond_count(Diagnostics_area *da) {
  return da->current_statement_warn_count();
}

bool sdb_thd_has_client_capability(THD *thd, ulonglong flag) {
  return (thd->client_capabilities & flag);
}

void sdb_thd_set_not_killed(THD *thd) {
  thd->killed = NOT_KILLED;
}

void sdb_thd_reset_condition_info(THD *thd) {
  thd->get_stmt_da()->clear_warning_info(thd->query_id);
}

bool sdb_is_transaction_stmt(THD *thd, bool all) {
  return all ? thd->transaction.all.ha_list : thd->transaction.stmt.ha_list;
}

bool sdb_create_table_like(THD *thd) {
  return thd->lex->create_info.like();
}

void sdb_query_cache_invalidate(THD *thd, bool all) {
  query_cache_invalidate3(thd, thd->lex->query_tables,
                          sdb_is_transaction_stmt(thd, all));
}

bool sdb_table_has_gcol(TABLE *table) {
  return table->vfield;
}

const char *sdb_table_alias(TABLE *table) {
  return table->alias.ptr();
}

uint sdb_tables_in_join(JOIN *join) {
  return join->table_count;
}

void sdb_set_timespec(struct timespec &abstime, ulonglong sec) {
  set_timespec(abstime, sec);
}

bool sdb_has_sql_condition(THD *thd, uint sql_errno) {
  Diagnostics_area::Sql_condition_iterator it(
      thd->get_stmt_da()->sql_conditions());
  const Sql_condition *err = NULL;

  while ((err = it++)) {
    if (err->get_sql_errno() == sql_errno)
      return true;
  }
  return false;
}

const char *sdb_thd_db(THD *thd) {
  return thd->db.str;
}

Protocol *sdb_thd_protocal(THD *thd) {
  return thd->protocol;
}

void sdb_protocal_start_row(Protocol *protocol) {
  return protocol->prepare_for_resend();
}

bool sdb_protocal_end_row(Protocol *protocol) {
  return protocol->write();
}

uint sdb_sql_errno(THD *thd) {
  return thd->get_stmt_da()->sql_errno();
}

const char *sdb_errno_message(THD *thd) {
  return thd->get_stmt_da()->message();
}
#endif
