#ifndef MYSQL_SERVER
#define MYSQL_SERVER
#endif

#include "sdb_sql.h"
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

void sdb_aes_crypt(enum my_aes_opmode AES_OPMODE, int flags, const uchar *src,
                   int slen, uchar *dst, int &dlen, const uchar *key,
                   uint klen) {
  if (flags & 1) {
    dlen = my_aes_encrypt(src, slen, dst, key, klen, AES_OPMODE, NULL);
  } else {
    dlen = my_aes_decrypt(src, slen, dst, key, klen, AES_OPMODE, NULL);
  }
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

Item *sdb_where_condition(THD *thd) {
  return sdb_lex_first_select(thd)->where_cond();
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

bool sdb_field_has_insert_def_func(const Field *field) {
  return field->has_insert_default_function();
}

bool sdb_field_has_update_def_func(const Field *field) {
  return field->has_update_default_function();
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

void sdb_aes_crypt(enum my_aes_opmode AES_OPMODE, int flags, const uchar *src,
                   int slen, uchar *dst, int &dlen, const uchar *key,
                   uint klen) {
  uint unsigned_dlen = 0;
  my_aes_crypt(AES_OPMODE, flags, src, (uint)slen, dst, &unsigned_dlen, key,
               klen, NULL, 0);
  dlen = unsigned_dlen;
}

uint sdb_aes_get_size(enum my_aes_opmode AES_OPMODE, uint slen) {
  return my_aes_get_size(AES_OPMODE, slen);
}

bool sdb_datetime_to_timeval(THD *thd, const MYSQL_TIME *ltime,
                             struct timeval *tm, int *error_code) {
  check_date_with_warn(
      thd, ltime,
      TIME_FUZZY_DATES | TIME_INVALID_DATES | thd->temporal_round_mode(),
      MYSQL_TIMESTAMP_ERROR);
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
  return &thd->lex->ignore;
}

Item *sdb_where_condition(THD *thd) {
  return sdb_lex_first_select(thd)->where;
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
  return field->stored_in_db();
}

bool sdb_field_has_insert_def_func(const Field *field) {
  return field->has_default_now_unireg_check();
}

bool sdb_field_has_update_def_func(const Field *field) {
  return field->unireg_check == Field::TIMESTAMP_UN_FIELD ||
         field->unireg_check == Field::TIMESTAMP_DNUN_FIELD;
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

#endif
