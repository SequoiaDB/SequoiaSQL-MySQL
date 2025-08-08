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
#include "ha_sdb_sql.h"
#include <handler.h>
#include <mysql_version.h>
#include <client.hpp>
#include <vector>
#include "ha_sdb_def.h"
#include "sdb_cl.h"
#include "ha_sdb_util.h"
#include "ha_sdb_lock.h"
#include "ha_sdb_conf.h"
#include "ha_sdb_condition.h"

/* Make entry to sql_class method. */
extern "C" {
bool thd_binlog_filter_ok(const MYSQL_THD thd);

int thd_slave_thread(const MYSQL_THD thd);
}

/*
  Stats that can be retrieved from SequoiaDB.
*/
struct Sdb_statistics {
  int32 page_size;
  int32 total_data_pages;
  int32 total_index_pages;
  int64 total_data_free_space;
  int64 total_records;

  Sdb_statistics() { init(); }

  void init() {
    page_size = 0;
    total_data_pages = 0;
    total_index_pages = 0;
    total_data_free_space = 0;
    total_records = ~(int64)0;
  }
};

struct Sdb_share {
  char *table_name;
  uint table_name_length;
  uint use_count;
  THR_LOCK lock;
  Sdb_mutex mutex;
  Sdb_statistics stat;
};

class ha_sdb;

/**
  ALTER TABLE main flow of ALGORITHM COPY:
  1. Copy a new table wtih the same schema in a tmp name.
  2. Copy the old data to new table by INSERT.
  3. Rename the old table as an other tmp name.
  4. Rename the new table as the right name.
  5. Drop the old table.

  It's complicated when the table is with sub-cl, so this class is needed.
*/
class Sdb_cl_copyer : public Sql_alloc {
 public:
  Sdb_cl_copyer(Sdb_conn *conn, const char *src_db_name,
                const char *src_table_name, const char *dst_db_name,
                const char *dst_table_name);

  int copy(ha_sdb *ha);

  int rename(const char *from, const char *to);

  void replace_src_indexes(uint keys, const KEY *key_info);

  void replace_src_auto_inc(const bson::BSONObj &auto_inc_options);

 private:
  int rename_new_cl();

  int rename_old_cl();

 private:
  Sdb_conn *m_conn;
  char *m_mcl_cs;
  char *m_mcl_name;
  char *m_new_cs;
  char *m_new_mcl_tmp_name;
  char *m_old_mcl_tmp_name;
  bson::BSONObj m_old_scl_info;
  List<char> m_new_scl_tmp_fullnames;
  List<char> m_old_scl_tmp_fullnames;

  bool m_replace_index;
  uint m_keys;
  const KEY *m_key_info;
  bool m_replace_autoinc;
  bson::BSONObj m_auto_inc_options;
};

class ha_sdb_alter_ctx;

class ha_sdb : public handler {
 public:
  ha_sdb(handlerton *hton, TABLE_SHARE *table_arg);

  ~ha_sdb();

  /** @brief
     The name that will be used for display purposes.
     */
  const char *table_type() const { return "SequoiaDB"; }

  /** @brief
     The name of the index type that will be used for display.
     Don't implement this method unless you really have indexes.
     */
  const char *index_type(uint key_number) { return ("BTREE"); }

  /** @brief
     The file extensions.
     */
  const char **bas_ext() const;

  /** @brief
     This is a list of flags that indicate what functionality the storage engine
     implements. The current table flags are documented in handler.h
     */
  ulonglong table_flags() const;

  /** @brief
     This is a bitmap of flags that indicates how the storage engine
     implements indexes. The current index flags are documented in
     handler.h. If you do not implement indexes, just return zero here.

     @details
     part is the key part to check. First key part is 0.
     If all_parts is set, MySQL wants to know the flags for the combined
     index, up to and including 'part'.
     */
  ulong index_flags(uint inx, uint part, bool all_parts) const;

  /** @brief
     unireg.cc will call max_supported_record_length(), max_supported_keys(),
     max_supported_key_parts(), uint max_supported_key_length()
     to make sure that the storage engine can handle the data it is about to
     send. Return *real* limits of your storage engine here; MySQL will do
     min(your_limits, MySQL_limits) automatically.
     */
  uint max_supported_record_length() const;

  uint max_key_part_length() const;
  /** @brief
     unireg.cc will call this to make sure that the storage engine can handle
     the data it is about to send. Return *real* limits of your storage engine
     here; MySQL will do min(your_limits, MySQL_limits) automatically.

     @details
     There is no need to implement ..._key_... methods if your engine doesn't
     support indexes.
     */
  uint max_supported_keys() const;

  /** @brief
    unireg.cc will call this to make sure that the storage engine can handle
    the data it is about to send. Return *real* limits of your storage engine
    here; MySQL will do min(your_limits, MySQL_limits) automatically.

      @details
    There is no need to implement ..._key_... methods if your engine doesn't
    support indexes.
   */
  uint max_supported_key_part_length(HA_CREATE_INFO *create_info) const;

  uint max_supported_key_part_length() const;

  /** @brief
    unireg.cc will call this to make sure that the storage engine can handle
    the data it is about to send. Return *real* limits of your storage engine
    here; MySQL will do min(your_limits, MySQL_limits) automatically.

      @details
    There is no need to implement ..._key_... methods if your engine doesn't
    support indexes.
   */
  uint max_supported_key_length() const;

  /*
    Everything below are methods that we implement in ha_example.cc.

    Most of these methods are not obligatory, skip them and
    MySQL will treat them as not implemented
  */
  /** @brief
    We implement this in ha_example.cc; it's a required method.
  */
  int open(const char *name, int mode, uint test_if_locked);

  /** @brief
    We implement this in ha_example.cc; it's a required method.
  */
  int close(void);

  int reset();

  /**
    @brief Prepares the storage engine for bulk inserts.

    @param[in] rows       estimated number of rows in bulk insert
                          or 0 if unknown.
               flags      Flags to control index creation

    @details Initializes memory structures required for bulk insert.
  */
  void start_bulk_insert(ha_rows rows);

  void start_bulk_insert(ha_rows rows, uint flags);

  /**
    @brief End bulk insert.

    @details This method will send any remaining rows to the remote server.
    Finally, it will deinitialize the bulk insert data structure.

    @return Operation status
    @retval       0       No error
    @retval       != 0    Error occured at remote server. Also sets my_errno.
  */
  int end_bulk_insert();

  /** @brief
    We implement this in ha_example.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int write_row(uchar *buf);

  /** @brief
    We implement this in ha_example.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int update_row(const uchar *old_data, uchar *new_data);

  int update_row(const uchar *old_data, const uchar *new_data);

  /** @brief
    We implement this in ha_example.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int delete_row(const uchar *buf);

  int build_selector(bson::BSONObj &selector);

  int append_end_range(bson::BSONObj &condition);

  int build_index_position(const KEY *key_info, const bson::BSONObj &start_cond,
                           bson::BSONObjBuilder &hint_pos_builder);

  int join_cond_idx(const bson::BSONObj &start_cond,
                    const bson::BSONObj &end_cond, bson::BSONObj &cond_idx);

  /** @brief
    We implement this in ha_example.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_read_map(uchar *buf, const uchar *key_ptr, key_part_map keypart_map,
                     enum ha_rkey_function find_flag);

  /**
     @brief
     The following functions works like index_read, but it find the last
     row with the current key value or prefix.
     @returns @see index_read_map().
  */
  int index_read_last_map(uchar *buf, const uchar *key,
                          key_part_map keypart_map) {
    return index_read_map(buf, key, keypart_map, HA_READ_PREFIX_LAST);
  }

  /** @brief
    We implement this in ha_example.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_next(uchar *buf);

  /** @brief
    We implement this in ha_example.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_prev(uchar *buf);

  /** @brief
    We implement this in ha_example.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_first(uchar *buf);

  /** @brief
    We implement this in ha_example.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_last(uchar *buf);

  int multi_range_read_init(RANGE_SEQ_IF *seq, void *seq_init_param,
                            uint n_ranges, uint mode, HANDLER_BUFFER *buf);

  int multi_range_read_next(char **range_info);

  // int index_read(uchar *buf, const uchar *key_ptr, uint key_len,
  //               enum ha_rkey_function find_flage);

  int create_field_rule(const char *field_name, Item_field *value,
                        bson::BSONObjBuilder &builder);

  int create_inc_rule(Field *rfield, Item *value, bool *optimizer_update,
                      bson::BSONObjBuilder &builder);

  int create_set_rule(Field *rfield, Item *value, bool *optimizer_update,
                      bson::BSONObjBuilder &builder);

  int create_modifier_obj(bson::BSONObj &rule, bool *optimizer_update);

  int optimize_count(bson::BSONObj &condition, bool &read_one_record);

  bool optimize_delete(bson::BSONObj &condition);

  int optimize_update(bson::BSONObj &rule, bson::BSONObj &condition,
                      bool &optimizer_update);

  int optimize_proccess(bson::BSONObj &rule, bson::BSONObj &condition,
                        bson::BSONObj &selector, bson::BSONObj &hint,
                        ha_rows &num_to_return, bool &direct_op);

  int index_init(uint idx, bool sorted);

  int index_end();

  uint lock_count(void) const { return 0; }

  /** @brief
    Unlike index_init(), rnd_init() can be called two consecutive times
    without rnd_end() in between (it only makes sense if scan=1). In this
    case, the second call should prepare for the new table scan (e.g if
    rnd_init() allocates the cursor, the second call should position the
    cursor to the start of the table; no need to deallocate and allocate
    it again. This is a required method.
  */
  int rnd_init(bool scan);

  int rnd_end();

  int rnd_next(uchar *buf);

  int rnd_pos(uchar *buf, uchar *pos);

  void position(const uchar *record);

  int info(uint);

  int extra(enum ha_extra_function operation);

  int external_lock(THD *thd, int lock_type);

  bool pushdown_autocommit();

  int autocommit_statement(bool direct_op = false);

  bool convert_autocommit_to_normal_trans();

  int start_statement(THD *thd, uint table_count);

  int delete_all_rows(void);

  int truncate();

  int analyze(THD *thd, HA_CHECK_OPT *check_opt);

  ha_rows records_in_range(uint inx, key_range *min_key, key_range *max_key);

  int delete_table(const char *from);

  int rename_table(const char *from, const char *to);

  int create(const char *name, TABLE *form, HA_CREATE_INFO *create_info);

  void update_create_info(HA_CREATE_INFO *create_info);

  THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to,
                             enum thr_lock_type lock_type);

  void unlock_row();

  int start_stmt(THD *thd, thr_lock_type lock_type);

  bool prepare_inplace_alter_table(TABLE *altered_table,
                                   Alter_inplace_info *ha_alter_info);

  bool inplace_alter_table(TABLE *altered_table,
                           Alter_inplace_info *ha_alter_info);

  enum_alter_inplace_result check_if_supported_inplace_alter(
      TABLE *altered_table, Alter_inplace_info *ha_alter_info);

  const Item *cond_push(const Item *cond);

  Item *idx_cond_push(uint keyno, Item *idx_cond);

  void handle_sdb_error(int error, myf errflag);

  int check(THD *thd, HA_CHECK_OPT *check_opt) { return 0; }

  int repair(THD *thd, HA_CHECK_OPT *repair_opt) { return 0; }

  int optimize(THD *thd, HA_CHECK_OPT *check_opt) { return 0; }

 private:
  int ensure_collection(THD *thd);

  int ensure_cond_ctx(THD *thd);

  int obj_to_row(bson::BSONObj &obj, uchar *buf);

  bool check_element_type_compatible(bson::BSONElement &elem, Field *field);

  int bson_element_to_field(const bson::BSONElement elem, Field *field);

  int row_to_obj(uchar *buf, bson::BSONObj &obj, bool gen_oid, bool output_null,
                 bson::BSONObj &null_obj, bool auto_inc_explicit_used);

  int field_to_strict_obj(Field *field, bson::BSONObjBuilder &obj_builder,
                          bool default_min_value, Item_field *val_field = NULL);

  int field_to_obj(Field *field, bson::BSONObjBuilder &obj_builder,
                   bool auto_inc_explicit_used = false);

  int get_update_obj(const uchar *old_data, const uchar *new_data,
                     bson::BSONObj &obj, bson::BSONObj &null_obj);

  int next_row(bson::BSONObj &obj, uchar *buf);

  int cur_row(uchar *buf);

  int flush_bulk_insert();

  int filter_partition_options(const bson::BSONObj &options,
                               bson::BSONObj &table_options);

  int auto_fill_default_options(enum enum_compress_type sql_compress,
                                const bson::BSONObj &options,
                                const bson::BSONObj &sharding_key,
                                bson::BSONObjBuilder &build);

  int get_cl_options(TABLE *form, HA_CREATE_INFO *create_info,
                     bson::BSONObj &options);

  int get_default_sharding_key(TABLE *form, bson::BSONObj &options);

  inline int get_sharding_key_from_options(TABLE *table,
                                           const bson::BSONObj &options,
                                           bson::BSONObj &sharding_key);

  int get_sharding_key(TABLE *form, bson::BSONObj &options,
                       bson::BSONObj &sharding_key);

  void filter_options(const bson::BSONObj &options, const char **filter_fields,
                      int filter_num, bson::BSONObjBuilder &build,
                      bson::BSONObjBuilder *filter_build = NULL);

  int index_read_one(bson::BSONObj condition, int order_direction, uchar *buf,
                     bson::BSONObjBuilder *hint_builder);

  my_bool get_unique_key_cond(const uchar *rec_row, bson::BSONObj &cond);

  my_bool get_cond_from_key(const KEY *unique_key, bson::BSONObj &cond);

  int get_query_flag(const uint sql_command, enum thr_lock_type lock_type,
                     int &query_flag, bool explicit_lock = false);

  int update_stats(THD *thd, bool do_read_stat);

  int ensure_stats(THD *thd);

  int build_auto_inc_option(const Field *field,
                            const HA_CREATE_INFO *create_info,
                            bson::BSONObj &option);

  void update_last_insert_id();

  int append_default_value(bson::BSONObjBuilder &builder, Field *field);

  int alter_column(TABLE *altered_table, Alter_inplace_info *ha_alter_info,
                   Sdb_conn *conn, Sdb_cl &cl);

  bool get_error_message(int error, String *buf);

  void print_error(int error, myf errflag);

  double scan_time();

  /*add current table share to open_table_share */
  int add_share_to_open_table_shares(THD *thd);

  int get_found_updated_rows(bson::BSONObj &result, ulonglong *found,
                             ulonglong *updated);

  int get_deleted_rows(bson::BSONObj &result, ulonglong *deleted);

  int get_dup_info(bson::BSONObj &result, const char **idx_name);

  int get_dup_key_cond(bson::BSONObj &cond);

  template <class T>
  int insert_row(T &rows, uint row_count);

  int copy_cl_if_alter_table(THD *thd, Sdb_conn *conn, char *db_name,
                             char *table_name, TABLE *form,
                             HA_CREATE_INFO *create_info, bool *has_copy);

  void raw_store_blob(Field_blob *blob, const char *data, uint len);

  int check_and_set_options(const char *old_options_str,
                            const char *new_options_str,
                            enum enum_compress_type old_sql_compress,
                            enum enum_compress_type new_sql_compress,
                            Sdb_cl &cl);

  bool is_field_rule_supported();

  bool is_inc_rule_supported();

  int create_condition_in_for_mrr(bson::BSONObj &condition);

  enum_alter_inplace_result filter_alter_columns(
      TABLE *altered_table, Alter_inplace_info *ha_alter_info,
      ha_sdb_alter_ctx *ctx);

 private:
  THR_LOCK_DATA lock_data;
  enum thr_lock_type m_lock_type;
  Sdb_cl *collection;
  bool first_read;
  Sdb_share *share;
  bool first_info;
  bool direct_sort;
  bool direct_limit;
  bson::BSONObj cur_rec;
  bson::BSONObj pushed_condition;
  bson::BSONObj field_order_condition;
  bson::BSONObj group_list_condition;
  bson::BSONObj last_key_value;
  char db_name[SDB_CS_NAME_MAX_SIZE + 1];
  char table_name[SDB_CL_NAME_MAX_SIZE + 1];
  time_t last_count_time;
  int count_times;
  MEM_ROOT blobroot;
  int idx_order_direction;
  bool m_ignore_dup_key;
  bool m_write_can_replace;
  bool m_insert_with_update;
  bool m_secondary_sort_rowid;
  bool m_use_bulk_insert;
  int m_bulk_insert_total;
  std::vector<bson::BSONObj> m_bulk_insert_rows;
  Sdb_obj_cache<bson::BSONElement> m_bson_element_cache;
  bool m_has_update_insert_id;
  longlong total_count;
  bool count_query;
  ha_sdb_cond_ctx *sdb_condition;
  ulonglong m_table_flags;
  /*incremental stat of current table share in current thd*/
  struct Sdb_local_table_statistics *incr_stat;
  uint m_dup_key_nr;
  bson::OID m_dup_oid;
  bson::BSONObj m_dup_value;
  Item *updated_value;
  Field *updated_field;
  st_order *sdb_order;
  st_order *sdb_group_list;
  bool can_pushdown_cond_in;
  bool m_use_position;
};
