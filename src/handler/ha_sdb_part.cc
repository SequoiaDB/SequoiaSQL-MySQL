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

#ifdef IS_MYSQL

#ifndef MYSQL_SERVER
#define MYSQL_SERVER
#endif

#include "ha_sdb_part.h"
#include "ha_sdb_idx.h"
#include <sql_class.h>
#include <partition_info.h>

static void sdb_traverse_and_append_field(const Item *item, void *arg) {
  if (item && Item::FIELD_ITEM == item->type()) {
    bson::BSONObjBuilder *builder = (bson::BSONObjBuilder *)arg;
    builder->append(sdb_item_name(item), 1);
  }
}

/**
  Check if sharding info of `table_options` is conflicted with `sharding_key`
  and `shard_type`.
*/
int sdb_check_shard_info(const bson::BSONObj &table_options,
                         const bson::BSONObj &sharding_key,
                         const char *shard_type) {
  int rc = 0;
  bson::BSONElement opt_ele;

  opt_ele = table_options.getField(SDB_FIELD_SHARDING_KEY);
  if (opt_ele.type() != bson::EOO) {
    if (opt_ele.type() != bson::Object) {
      rc = ER_WRONG_ARGUMENTS;
      my_printf_error(rc, "Type of option ShardingKey should be 'Object'",
                      MYF(0));
      goto error;
    }
    if (!sharding_key.isEmpty() &&
        !opt_ele.embeddedObject().equal(sharding_key)) {
      rc = ER_WRONG_ARGUMENTS;
      my_printf_error(rc, "Ambiguous option ShardingKey", MYF(0));
      goto error;
    }
  }

  opt_ele = table_options.getField(SDB_FIELD_SHARDING_TYPE);
  if (opt_ele.type() != bson::EOO) {
    if (opt_ele.type() != bson::String) {
      rc = ER_WRONG_ARGUMENTS;
      my_printf_error(rc, "Type of option ShardingType should be 'String'",
                      MYF(0));
      goto error;
    }
    if (strcmp(opt_ele.valuestr(), shard_type) != 0) {
      rc = ER_WRONG_ARGUMENTS;
      my_printf_error(rc, "Ambiguous option ShardingType", MYF(0));
      goto error;
    }
  }
done:
  return rc;
error:
  goto done;
}

int sdb_get_bound(partition_info *part_info, uint curr_part_id,
                  bool is_low_bound, bson::BSONObjBuilder &builder) {
  DBUG_ENTER("sdb_get_bound");

  int rc = 0;
  part_column_list_val *range_col_array = part_info->range_col_array;
  uint field_num = part_info->part_field_list.elements;
  Sdb_func_isnull item_convertor;  // convert Item to BSONObj

  const char *bound_field =
      is_low_bound ? SDB_FIELD_LOW_BOUND : SDB_FIELD_UP_BOUND;
  bson::BSONObjBuilder sub_builder(builder.subobjStart(bound_field));
  uint start = 0;
  if (is_low_bound) {
    start = (curr_part_id - 1) * field_num;
  } else {
    start = curr_part_id * field_num;
  }

  for (uint i = 0; i < field_num; ++i) {
    Field *field = part_info->part_field_array[i];
    const char *field_name = sdb_field_name(field);
    part_column_list_val &col_val = range_col_array[start + i];
    if (col_val.max_value) {
      sub_builder.appendMaxKey(field_name);
    } else if (col_val.null_value) {
      // Do nothing to append $Undefined
    } else {
      bson::BSONObj obj;
      rc = item_convertor.get_item_val(field_name, col_val.item_expression,
                                       field, obj);
      if (rc != 0) {
        goto error;
      }
      sub_builder.appendElements(obj);
    }
  }
  sub_builder.done();

done:
  DBUG_RETURN(rc);
error:
  goto done;
}

ha_sdb_part::ha_sdb_part(handlerton *hton, TABLE_SHARE *table_arg)
    : ha_sdb(hton, table_arg), Partition_helper(this) {}

bool ha_sdb_part::is_sharded_by_part_id(partition_info *part_info) {
  bool is_range_part_with_func =
      (RANGE_PARTITION == part_info->part_type && !part_info->column_list &&
       part_info->part_expr->type() != Item::FIELD_ITEM);
  bool is_list_part = (LIST_PARTITION == part_info->part_type);
  return (is_range_part_with_func || is_list_part);
}

void ha_sdb_part::get_sharding_key(partition_info *part_info,
                                   bson::BSONObj &sharding_key) {
  DBUG_ENTER("ha_sdb_part::get_sharding_key");
  /*
    For RANGE/LIST, When partition expression cannot be pushed down, we shard
    the cl by mysql part id. One partition responses one sub cl. For HASH, We
    don't care what the expression is like, just shard by the fields in it.
  */
  bson::BSONObjBuilder builder;
  switch (part_info->part_type) {
    case RANGE_PARTITION: {
      // RANGE COLUMNS(<column_list>)
      if (part_info->column_list) {
        List_iterator_fast<char> it(part_info->part_field_list);
        char *field_name = NULL;
        while ((field_name = it++)) {
          builder.append(field_name, 1);
        }
      }
      // RANGE (<field>)
      else if (Item::FIELD_ITEM == part_info->part_expr->type()) {
        builder.append(sdb_item_name(part_info->part_expr), 1);
      }
      // RANGE (<func>)
      else {
        builder.append(SDB_FIELD_INNER_PART_ID, 1);
      }
      break;
    }
    case LIST_PARTITION: {
      builder.append(SDB_FIELD_INNER_PART_ID, 1);
      break;
    }
    case HASH_PARTITION: {
      // (LINEAR) KEY(<column_list>)
      if (part_info->list_of_part_fields) {
        uint field_num = part_info->part_field_list.elements;
        for (uint i = 0; i < field_num; ++i) {
          Field *field = part_info->part_field_array[i];
          builder.append(sdb_field_name(field), 1);
        }
      }
      // (LINEAR) HASH(<expr>)
      else {
        part_info->part_expr->traverse_cond(&sdb_traverse_and_append_field,
                                            (void *)&builder, Item::PREFIX);
      }
      break;
    }
    default: { DBUG_ASSERT(0); }
  }
  sharding_key = builder.obj();
  DBUG_VOID_RETURN;
}

int ha_sdb_part::get_cl_options(TABLE *form, HA_CREATE_INFO *create_info,
                                bson::BSONObj &options,
                                bson::BSONObj &partition_options,
                                bool &explicit_not_auto_partition) {
  DBUG_ENTER("ha_sdb_part::get_cl_options");

  int rc = 0;
  bson::BSONObj sharding_key;
  bson::BSONObj table_options;
  bson::BSONElement opt_ele;
  bson::BSONObjBuilder builder;
  partition_type part_type = form->part_info->part_type;
  const char *shard_type = NULL;
  bool is_main_cl = false;
/*Mariadb hasn't sql compress*/
#if defined IS_MYSQL
  enum enum_compress_type sql_compress =
      sdb_str_compress_type(create_info->compress.str);
#elif defined IS_MARIADB
  enum enum_compress_type sql_compress = SDB_COMPRESS_TYPE_DEAFULT;
#endif
  if (sql_compress == SDB_COMPRESS_TYPE_INVALID) {
    rc = ER_WRONG_ARGUMENTS;
    my_printf_error(rc, "Invalid compression type", MYF(0));
    goto error;
  }

  if (create_info && create_info->comment.str) {
    rc = parse_comment_options(create_info->comment.str, table_options,
                               explicit_not_auto_partition, &partition_options);
    if (rc != 0) {
      goto error;
    }
  }
  /*
    Handle the options that may be conflicted with COMMENT, including
    ShardingKey, ShardingType, IsMainCL. It's permitted to repeatly specific the
    same option.
  */
  get_sharding_key(form->part_info, sharding_key);
  shard_type = (HASH_PARTITION == part_type ? "hash" : "range");
  rc = sdb_check_shard_info(table_options, sharding_key, shard_type);
  if (rc != 0) {
    goto error;
  }

  opt_ele = table_options.getField(SDB_FIELD_ISMAINCL);
  is_main_cl = RANGE_PARTITION == part_type || LIST_PARTITION == part_type;
  if (bson::EOO == opt_ele.type()) {
    if (is_main_cl) {
      builder.append(SDB_FIELD_ISMAINCL, true);
    }
  } else {
    if (opt_ele.type() != bson::Bool) {
      rc = ER_WRONG_ARGUMENTS;
      my_printf_error(rc, "Type of option IsMainCL should be 'Bool'", MYF(0));
      goto error;
    }
    if (opt_ele.boolean() != is_main_cl) {
      rc = ER_WRONG_ARGUMENTS;
      my_printf_error(rc, "Ambiguous option IsMainCL", MYF(0));
      goto error;
    }
  }
  builder.appendElements(table_options);
  table_options = builder.obj();

  {
    bson::BSONObjBuilder tmp_builder;
    rc = auto_fill_default_options(sql_compress, table_options, sharding_key,
                                   tmp_builder);
    if (rc) {
      goto error;
    }
    options = tmp_builder.obj();
  }

done:
  DBUG_RETURN(rc);
error:
  goto done;
}

int ha_sdb_part::get_scl_options(partition_info *part_info,
                                 partition_element *part_elem,
                                 const bson::BSONObj &mcl_options,
                                 const bson::BSONObj &partition_options,
                                 bool explicit_not_auto_partition,
                                 bson::BSONObj &scl_options) {
  DBUG_ENTER("ha_sdb_part::get_scl_options");

  static const char *INHERITABLE_OPT[] = {
      SDB_FIELD_REPLSIZE, SDB_FIELD_COMPRESSED, SDB_FIELD_COMPRESSION_TYPE,
      SDB_FIELD_AUTOINDEXID, SDB_FIELD_STRICT_DATA_MODE};
  static const uint INHERITABLE_OPT_NUM =
      sizeof(INHERITABLE_OPT) / sizeof(const char *);
  /*
    There are 3 level cl options:
    1. mcl_options      : table_options of top level COMMENT
    2. partition_options: partition_options of top level COMMENT
    3. table_options    : table_options of partition COMMENT
    The lower the level, the higher the priority.
  */
  int rc = 0;
  bson::BSONObj table_options;
  bson::BSONObj sharding_key;
  bson::BSONObjBuilder builder;

  if (part_elem->part_comment) {
    rc = parse_comment_options(part_elem->part_comment, table_options,
                               explicit_not_auto_partition);
    if (rc != 0) {
      goto error;
    }
  }

  // Generate scl sharding key;
  if (part_info->is_sub_partitioned()) {
    bson::BSONObjBuilder key_builder;
    // KEY SUBPARTITION
    if (part_info->list_of_subpart_fields) {
      List_iterator_fast<char> it(part_info->subpart_field_list);
      char *field_name = NULL;
      while ((field_name = it++)) {
        key_builder.append(field_name, 1);
      }
    }
    // HASH SUBPARTITION
    else {
      part_info->subpart_expr->traverse_cond(
          &sdb_traverse_and_append_field, (void *)&key_builder, Item::PREFIX);
    }
    sharding_key = key_builder.obj();

  } else if (!explicit_not_auto_partition) {
    rc =
        ha_sdb::get_sharding_key(part_info->table, table_options, sharding_key);
    if (rc != 0) {
      goto error;
    }
  }

  // Check the options about shard, which may be conflicted with COMMENT;
  rc = sdb_check_shard_info(table_options, sharding_key, "hash");
  if (rc != 0) {
    goto error;
  }
  rc = sdb_check_shard_info(partition_options, sharding_key, "hash");
  if (rc != 0) {
    goto error;
  }

  // Merge mcl_options & partition_options into table_options.
  {
    bson::BSONObjIterator it(partition_options);
    while (it.more()) {
      bson::BSONElement part_opt = it.next();
      if (!table_options.hasField(part_opt.fieldName())) {
        builder.append(part_opt);
      }
    }
  }

  for (uint i = 0; i < INHERITABLE_OPT_NUM; ++i) {
    const char *curr_opt = INHERITABLE_OPT[i];
    bson::BSONElement mcl_opt = mcl_options.getField(curr_opt);
    if (mcl_opt.type() != bson::EOO && !table_options.hasField(curr_opt) &&
        !partition_options.hasField(curr_opt)) {
      builder.append(mcl_opt);
    }
  }
  builder.appendElements(table_options);
  table_options = builder.obj();

  {
    bson::BSONObjBuilder tmp_builder;
    rc = auto_fill_default_options(SDB_COMPRESS_TYPE_DEAFULT, table_options,
                                   sharding_key, tmp_builder);
    if (rc) {
      goto error;
    }
    scl_options = tmp_builder.obj();
  }

done:
  DBUG_RETURN(rc);
error:
  goto done;
}

int ha_sdb_part::get_attach_options(partition_info *part_info,
                                    uint curr_part_id,
                                    bson::BSONObj &attach_options) {
  DBUG_ENTER("ha_sdb_part::get_attach_options");

  int rc = 0;
  bson::BSONObjBuilder builder;
  // LIST or RANGE(<func>)
  if (is_sharded_by_part_id(part_info)) {
    bson::BSONObjBuilder low_builder(builder.subobjStart(SDB_FIELD_LOW_BOUND));
    low_builder.append(SDB_FIELD_INNER_PART_ID, curr_part_id);
    low_builder.done();

    bson::BSONObjBuilder up_builder(builder.subobjStart(SDB_FIELD_UP_BOUND));
    up_builder.append(SDB_FIELD_INNER_PART_ID, curr_part_id + 1);
    up_builder.done();
  }
  // RANGE COLUMNS(<column_list>)
  else if (part_info->column_list) {
    if (0 == curr_part_id) {
      bson::BSONObjBuilder low_builder(
          builder.subobjStart(SDB_FIELD_LOW_BOUND));
      uint field_num = part_info->part_field_list.elements;
      for (uint i = 0; i < field_num; ++i) {
        Field *field = part_info->part_field_array[i];
        low_builder.appendMinKey(sdb_field_name(field));
      }
      low_builder.done();
    } else {
      rc = sdb_get_bound(part_info, curr_part_id, true, builder);
      if (rc != 0) {
        goto error;
      }
    }

    rc = sdb_get_bound(part_info, curr_part_id, false, builder);
    if (rc != 0) {
      goto error;
    }
  }
  // RANGE(<field>)
  else if (Item::FIELD_ITEM == part_info->part_expr->type()) {
    const char *sharding_key = sdb_item_name(part_info->part_expr);
    longlong *range_array = part_info->range_int_array;

    bson::BSONObjBuilder low_builder(builder.subobjStart(SDB_FIELD_LOW_BOUND));
    if (0 == curr_part_id) {
      low_builder.appendMinKey(sharding_key);
    } else {
      low_builder.append(sharding_key, range_array[curr_part_id - 1]);
    }
    low_builder.done();

    bson::BSONObjBuilder up_builder(builder.subobjStart(SDB_FIELD_UP_BOUND));
    up_builder.append(sharding_key, range_array[curr_part_id]);
    up_builder.done();

  }
  // impossible branch
  else {
    DBUG_ASSERT(0);
    rc = HA_ERR_INTERNAL_ERROR;
  }

  attach_options = builder.obj();
done:
  DBUG_RETURN(rc);
error:
  goto done;
}

int ha_sdb_part::create_and_attach_scl(Sdb_conn *conn, Sdb_cl &mcl,
                                       partition_info *part_info,
                                       const bson::BSONObj &mcl_options,
                                       const bson::BSONObj &partition_options,
                                       bool explicit_not_auto_partition) {
  DBUG_ENTER("ha_sdb_part::create_and_attach_scl");
  static const char *PART_SEP = "#P#";

  int rc = 0;
  bson::BSONObj scl_options;
  bson::BSONObj attach_options;
  uint curr_part_id = 0;
  char *cs_name = const_cast<char *>(mcl.get_cs_name());

  List_iterator_fast<partition_element> part_it(part_info->partitions);
  partition_element *part_elem;
  while ((part_elem = part_it++)) {
    // scl_name = mcl_name + '#P#' + partition_name
    char scl_name[SDB_CL_NAME_MAX_SIZE] = {0};
    char scl_fullname[SDB_CL_FULL_NAME_MAX_SIZE] = {0};
    uint name_len = strlen(mcl.get_cl_name()) + strlen(PART_SEP) +
                    strlen(part_elem->partition_name);
    if (name_len >= SDB_CL_NAME_MAX_SIZE) {
      rc = ER_WRONG_ARGUMENTS;
      my_printf_error(rc, "Too long table name", MYF(0));
    }
    sprintf(scl_name, "%s%s%s", mcl.get_cl_name(), PART_SEP,
            part_elem->partition_name);

    rc = get_scl_options(part_info, part_elem, mcl_options, partition_options,
                         explicit_not_auto_partition, scl_options);
    if (0 != rc) {
      goto error;
    }

    rc = conn->create_cl(cs_name, scl_name, scl_options);
    if (0 != rc) {
      goto error;
    }

    rc = get_attach_options(part_info, curr_part_id++, attach_options);
    if (0 != rc) {
      goto error;
    }

    sprintf(scl_fullname, "%s.%s", cs_name, scl_name);
    rc = mcl.attach_collection(scl_fullname, attach_options);
    if (rc != 0) {
      handle_sdb_error(rc, MYF(0));
      conn->drop_cl(cs_name, scl_name);
      goto error;
    }
  }
done:
  DBUG_RETURN(rc);
error:
  goto done;
}

bool ha_sdb_part::check_if_alter_table_options(THD *thd,
                                               HA_CREATE_INFO *create_info) {
  bool rs = false;
  if (SQLCOM_ALTER_TABLE == thd_sql_command(thd)) {
    SQL_I_List<TABLE_LIST> &table_list = sdb_lex_first_select(thd)->table_list;
    DBUG_ASSERT(table_list.elements == 1);
    TABLE_LIST *src_table = table_list.first;

    if (src_table->table->s->db_type() == create_info->db_type &&
        src_table->table->s->get_table_ref_type() != TABLE_REF_TMP_TABLE) {
      const char *src_tab_opt =
          strstr(src_table->table->s->comment.str, SDB_COMMENT);
      const char *dst_tab_opt = strstr(create_info->comment.str, SDB_COMMENT);
      src_tab_opt = src_tab_opt ? src_tab_opt : "";
      dst_tab_opt = dst_tab_opt ? dst_tab_opt : "";
      if (strcmp(src_tab_opt, dst_tab_opt) != 0) {
        rs = true;
      }
    }
  }
  return rs;
}

int ha_sdb_part::create(const char *name, TABLE *form,
                        HA_CREATE_INFO *create_info) {
  DBUG_ENTER("ha_sdb_part::create");

  int rc = 0;
  Sdb_conn *conn = NULL;
  Sdb_cl cl;
  bson::BSONObjBuilder build;
  bson::BSONObj options;
  bson::BSONObj partition_options;
  bool explicit_not_auto_partition = false;
  bool created_cs = false;
  bool created_cl = false;
  partition_info *part_info = form->part_info;

  if (sdb_execute_only_in_mysql(ha_thd())) {
    rc = 0;
    goto done;
  }

  if (check_if_alter_table_options(ha_thd(), create_info)) {
    rc = HA_ERR_WRONG_COMMAND;
    my_printf_error(rc,
                    "Cannot change table options of comment. "
                    "Try drop and create again.",
                    MYF(0));
    goto error;
  }

  /* Not allowed to create temporary partitioned tables. */
  DBUG_ASSERT(create_info && !(create_info->options & HA_LEX_CREATE_TMP_TABLE));

  for (Field **fields = form->field; *fields; fields++) {
    Field *field = *fields;

    if (field->key_length() >= SDB_FIELD_MAX_LEN) {
      my_error(ER_TOO_BIG_FIELDLENGTH, MYF(0), sdb_field_name(field),
               static_cast<ulong>(SDB_FIELD_MAX_LEN));
      rc = HA_WRONG_CREATE_OPTION;
      goto error;
    }

    if (strcasecmp(sdb_field_name(field), SDB_OID_FIELD) == 0) {
      my_error(ER_WRONG_COLUMN_NAME, MYF(0), sdb_field_name(field));
      rc = HA_WRONG_CREATE_OPTION;
      goto error;
    }

    if (Field::NEXT_NUMBER == MTYP_TYPENR(field->unireg_check)) {
      bson::BSONObj auto_inc_options;
      build_auto_inc_option(field, create_info, auto_inc_options);
      build.append(SDB_FIELD_NAME_AUTOINCREMENT, auto_inc_options);
    }
  }

  rc = sdb_parse_table_name(name, db_name, SDB_CS_NAME_MAX_SIZE, table_name,
                            SDB_CL_NAME_MAX_SIZE);
  if (0 != rc) {
    goto error;
  }

  rc = get_cl_options(form, create_info, options, partition_options,
                      explicit_not_auto_partition);
  if (0 != rc) {
    goto error;
  }

  build.appendElements(options);
  rc = check_sdb_in_thd(ha_thd(), &conn, true);
  if (0 != rc) {
    goto error;
  }
  DBUG_ASSERT(conn->thread_id() == sdb_thd_id(ha_thd()));

  rc = conn->create_cl(db_name, table_name, build.obj(), &created_cs,
                       &created_cl);
  if (0 != rc) {
    goto error;
  }

  rc = conn->get_cl(db_name, table_name, cl);
  if (0 != rc) {
    goto error;
  }

  if (RANGE_PARTITION == part_info->part_type ||
      LIST_PARTITION == part_info->part_type) {
    rc = create_and_attach_scl(conn, cl, part_info, options, partition_options,
                               explicit_not_auto_partition);
    if (0 != rc) {
      goto error;
    }
  }

  for (uint i = 0; i < form->s->keys; i++) {
    rc = sdb_create_index(form->s->key_info + i, cl,
                          is_sharded_by_part_id(part_info));
    if (0 != rc) {
      // we disabled sharding index,
      // so do not ignore SDB_IXM_EXIST_COVERD_ONE
      goto error;
    }
  }

done:
  DBUG_RETURN(rc);
error:
  handle_sdb_error(rc, MYF(0));
  if (created_cs) {
    conn->drop_cs(db_name);
  } else if (created_cl) {
    conn->drop_cl(db_name, table_name);
  }
  goto done;
}

#endif  // IS_MYSQL
