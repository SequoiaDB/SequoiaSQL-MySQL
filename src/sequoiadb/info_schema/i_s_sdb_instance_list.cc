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

#include "i_s_common.h"
#include "server_ha_def.h"
#include "sdb_cl.h"
#include <sql_show.h>

enum enum_i_s_inst_list_field {
  I_S_FLD_INSTANCE_ID = 0,
  I_S_FLD_INSTANCE_GROUP_NAME,
  I_S_FLD_HOST_NAME,
  I_S_FLD_IP,
  I_S_FLD_PORT,
  I_S_FLD_DB_TYPE,
  I_S_FLD_LOWER_CASE_TABLE_NAMES,
  I_S_FLD_DATA_GROUP,
  I_S_FLD_END
};

static ST_FIELD_INFO i_s_inst_list_info[I_S_FLD_END + 1];

static void i_s_init_inst_list_info() {
  /* ST_FIELD_INFO: field_name, field_length, field_type, value, field_flags,
   * old_name, open_method */
  ST_FIELD_INFO ii = {"INSTANCE_ID", 1,  MYSQL_TYPE_LONG, 0,
                      MY_I_S_MAYBE_NULL,  "", SKIP_OPEN_TABLE};
  i_s_inst_list_info[I_S_FLD_INSTANCE_ID] = ii;

  ST_FIELD_INFO ign = {"INSTANCE_GROUP_NAME", STRING_BUFFER_USUAL_SIZE,
                       MYSQL_TYPE_STRING, 0, MY_I_S_MAYBE_NULL,
                       "", SKIP_OPEN_TABLE};
  i_s_inst_list_info[I_S_FLD_INSTANCE_GROUP_NAME] = ign;

  ST_FIELD_INFO hn = {"HOST_NAME", STRING_BUFFER_USUAL_SIZE,
                      MYSQL_TYPE_STRING, 0, MY_I_S_MAYBE_NULL,
                      "", SKIP_OPEN_TABLE};
  i_s_inst_list_info[I_S_FLD_HOST_NAME] = hn;

  ST_FIELD_INFO ip = {"IP", STRING_BUFFER_USUAL_SIZE,
                      MYSQL_TYPE_STRING, 0, MY_I_S_MAYBE_NULL,
                      "", SKIP_OPEN_TABLE};
  i_s_inst_list_info[I_S_FLD_IP] = ip;

  ST_FIELD_INFO p = {"PORT", 1,  MYSQL_TYPE_LONG, 0,
                      MY_I_S_MAYBE_NULL,  "", SKIP_OPEN_TABLE};
  i_s_inst_list_info[I_S_FLD_PORT] = p;

  ST_FIELD_INFO dt = {"DB_TYPE", STRING_BUFFER_USUAL_SIZE,
                      MYSQL_TYPE_STRING, 0, MY_I_S_MAYBE_NULL,
                      "", SKIP_OPEN_TABLE};
  i_s_inst_list_info[I_S_FLD_DB_TYPE] = dt;

  ST_FIELD_INFO lctn = {"LOWER_CASE_TABLE_NAMES", 1,  MYSQL_TYPE_LONG, 0,
                        MY_I_S_MAYBE_NULL,  "", SKIP_OPEN_TABLE};
  i_s_inst_list_info[I_S_FLD_LOWER_CASE_TABLE_NAMES] = lctn;

  ST_FIELD_INFO dg = {"DATA_GROUP", STRING_BUFFER_USUAL_SIZE,
                      MYSQL_TYPE_STRING, 0, MY_I_S_MAYBE_NULL,
                      "", SKIP_OPEN_TABLE};
  i_s_inst_list_info[I_S_FLD_DATA_GROUP] = dg;

  i_s_inst_list_info[I_S_FLD_END] = I_S_END_FIELD_INFO;
}

static const char *i_s_inst_list_sdb_name[I_S_FLD_END + 1];

static void i_s_init_inst_list_sdb_name() {
  i_s_inst_list_sdb_name[I_S_FLD_INSTANCE_ID] = "InstanceID";
  i_s_inst_list_sdb_name[I_S_FLD_INSTANCE_GROUP_NAME] = "InstanceGroupName";
  i_s_inst_list_sdb_name[I_S_FLD_HOST_NAME] = "HostName";
  i_s_inst_list_sdb_name[I_S_FLD_IP] = "IP";
  i_s_inst_list_sdb_name[I_S_FLD_PORT] = "Port";
  i_s_inst_list_sdb_name[I_S_FLD_DB_TYPE] = "DBType";
  i_s_inst_list_sdb_name[I_S_FLD_LOWER_CASE_TABLE_NAMES] = "LowerCaseTableNames";
  i_s_inst_list_sdb_name[I_S_FLD_DATA_GROUP] = "DataGroup";
  i_s_inst_list_sdb_name[I_S_FLD_END] = NULL;
}

static PSI_memory_key key_memory_i_s_inst_list;

#ifdef HAVE_PSI_INTERFACE

#if defined IS_MYSQL
static PSI_memory_info all_i_s_memory[] = {
    {&key_memory_i_s_inst_list, "i_s_inst_list", PSI_FLAG_GLOBAL}};

static void init_i_s_psi_keys(void) {
  const char *category = "sequoiadb";
  int count = array_elements(all_i_s_memory);
  mysql_memory_register(category, all_i_s_memory, count);
}

#elif defined IS_MARIADB
static void init_i_s_psi_keys(void) {}
#endif

#endif  // HAVE_PSI_INTERFACE

static HASH i_s_name_id_pair_hash;

static int i_s_inst_list_fill_table(THD *thd, TABLE_LIST *tables, Item *cond) {
  int rc = 0;
  Sdb_conn *conn = NULL;
  Sdb_cl cl;
  TABLE *table = tables->table;
  bson::BSONObj obj;

  try {
    rc = check_sdb_in_thd(thd, &conn, true);
    if (rc != 0) {
      my_printf_error(ER_CANT_FIND_SYSTEM_REC,
                      "Failed to connect to SequoiaDB, error: %d", MYF(0), rc);
      goto error;
    }

    rc = conn->get_cl(HA_GLOBAL_INFO, HA_REGISTRY_CL, cl);
    if (rc != 0) {
      my_printf_error(ER_CANT_FIND_SYSTEM_REC,
                      "Failed to get %s.%s collection, error: %d", MYF(0),
                      HA_GLOBAL_INFO, HA_REGISTRY_CL, rc);
      goto error;
    }

    rc = cl.query();
    if (rc != 0) {
      my_printf_error(ER_CANT_FIND_SYSTEM_REC,
                      "Failed to query, error: %d", MYF(0), rc);
      goto error;
    }

    while (0 == (rc = cl.next(obj, false))) {
      bson::BSONObjIterator iter(obj);
      while (iter.more()) {
        bson::BSONElement ele = iter.next();
        rc = i_s_store_elem_to_field(i_s_name_id_pair_hash, table, ele);
        if (rc != 0) {
          goto error;
        }
      }
      if (schema_table_store_record(thd, table)) {
        rc = 1;
        goto error;
      }
    }
    if (rc == HA_ERR_END_OF_FILE) {
      rc = 0;
    }
    if (rc != 0) {
      my_printf_error(ER_CANT_FIND_SYSTEM_REC,
                      "Failed to get record, error: %d", MYF(0), rc);
      goto error;
    }

  } catch (std::exception &e) {
    my_printf_error(ER_CANT_FIND_SYSTEM_REC, "Exception occurred: %s", MYF(0),
                    e.what());
    rc = 1;
    goto error;
  }

done:
  return rc;
error:
  if (!thd->get_stmt_da()->is_error()) {
    my_error(ER_CANT_FIND_SYSTEM_REC, MYF(0));
  }
  goto done;
}

static int i_s_sdb_inst_list_init(void *p) {
  int rc = 0;
  ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE *)p;
  uint i = 0;
  const char *name = NULL;
  i_s_name_id_pair *new_pair = NULL;
  bool hash_inited = false;

  i_s_init_inst_list_info();
  i_s_init_inst_list_sdb_name();

  schema->fields_info = i_s_inst_list_info;
  schema->fill_table = i_s_inst_list_fill_table;

#ifdef HAVE_PSI_INTERFACE
  init_i_s_psi_keys();
#endif

  rc = sdb_hash_init(&i_s_name_id_pair_hash, &my_charset_utf8mb4_bin, 32, 0, 0,
                     (my_hash_get_key)i_s_pair_get_key, my_free, 0,
                     key_memory_i_s_inst_list);
  if (rc) {
    goto error;
  }
  hash_inited = true;

  while ((name = i_s_inst_list_sdb_name[i]) != NULL) {
    new_pair = (i_s_name_id_pair *)sdb_my_malloc(key_memory_i_s_inst_list,
                                                 sizeof(i_s_name_id_pair),
                                                 MYF(MY_WME | MY_ZEROFILL));
    if (!new_pair) {
      rc = 1;
      goto error;
    }

    new_pair->name = name;
    new_pair->name_len = strlen(name);
    new_pair->id = i;

    if (my_hash_insert(&i_s_name_id_pair_hash, (uchar *)new_pair)) {
      rc = 1;
      goto error;
    }

    ++i;
  }
done:
  return rc;
error:
  if (new_pair) {
    my_free(new_pair);
  }
  if (hash_inited) {
    my_hash_free(&i_s_name_id_pair_hash);
  }
  goto done;
}

static int i_s_sdb_inst_list_done(void *p) {
  my_hash_free(&i_s_name_id_pair_hash);
  return 0;
}

sdb_define_plugin(i_s_sdb_instance_list,
                  MYSQL_INFORMATION_SCHEMA_PLUGIN, /* the plugin type */
                  &i_s_info,                       /* plugin descriptor */
                  "SDB_INSTANCE_LIST",             /* plugin name */
                  plugin_author,                   /* plugin author */
                  "SequoiaDB SQL Instance List",   /* descriptor text */
                  PLUGIN_LICENSE_GPL,                     /* license */
                  i_s_sdb_inst_list_init,                 /* plugin init */
                  i_s_sdb_inst_list_done,                 /* plugin deinit */
                  0x0302,                                 /* version */
                  NULL,                                   /* status variables */
                  NULL                                    /* system variables */
);
