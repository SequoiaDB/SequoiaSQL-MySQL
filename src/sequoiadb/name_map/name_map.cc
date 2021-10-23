#include "client.hpp"
#include "ha_sdb_def.h"
#include "server_ha_def.h"

#include <my_global.h>
#include <my_base.h>
#include <mysql/plugin.h>
#include <mysql/plugin_audit.h>
#include <string.h>
#include <mysql/psi/mysql_thread.h>

// 'sql_class.h' can be inclued if 'MYSQL_SERVER' is defined
#ifndef MYSQL_SERVER
#define MYSQL_SERVER
#endif

#include "ha_sdb_errcode.h"
#include "sdb_conn.h"
#include "sdb_cl.h"
#include "name_map.h"
#include "ha_sdb_log.h"
#include "ha_sdb_util.h"
#include <my_config.h>

char MetadataMapping::m_sql_group[SDB_CS_NAME_MAX_SIZE + 1] = {0};
bool MetadataMapping::m_enabled = false;
int MetadataMapping::m_mapping_group_size = NM_MAPPING_GROUP_SIZE;
int MetadataMapping::m_mapping_group_num = NM_MAPPING_GROUP_NUM;

static int get_table_map_cl(Sdb_conn &sdb_conn, const char *sql_group_name,
                            Sdb_cl &table_map) {
  static bool indexes_created = false;
  const char *sql_group_cs_name = sql_group_name;

  int rc = 0;
  bson::BSONObj cl_options, index_ref, key_options;

  rc = sdb_conn.get_cl(sql_group_cs_name, NM_TABLE_MAP, table_map, true);
  if (SDB_DMS_CS_NOTEXIST == get_sdb_code(rc) ||
      SDB_DMS_NOTEXIST == get_sdb_code(rc)) {
    SDB_LOG_INFO("NM: Creating table '%s.%s'", sql_group_cs_name, NM_TABLE_MAP);
    cl_options = BSON(SDB_FIELD_GROUP << NM_SYS_META_GROUP);
    rc = sdb_conn.create_cl(sql_group_cs_name, NM_TABLE_MAP, cl_options);
    if (SDB_DMS_CS_EXIST == rc) {
      rc = sdb_conn.create_cl(sql_group_cs_name, NM_TABLE_MAP, cl_options);
    }
    rc = (SDB_DMS_EXIST == get_sdb_code(rc)) ? 0 : rc;
    if (0 != rc) {
      SDB_LOG_ERROR("NM: Unable to get table '%s', sequoiadb error: %d",
                    NM_TABLE_MAP, rc);
      goto error;
    }
  }

  if (!indexes_created) {
    rc = sdb_conn.get_cl(sql_group_cs_name, NM_TABLE_MAP, table_map);
    if (0 != rc) {
      SDB_LOG_ERROR("NM: Unable to get table '%s.%s', sequoiadb error: %d",
                    sql_group_cs_name, NM_TABLE_MAP, rc);
      goto error;
    }

    index_ref = BSON(NM_FIELD_DB_NAME << 1 << NM_FIELD_TABLE_NAME << 1);
    key_options = BSON(SDB_FIELD_UNIQUE << 1);
    rc = table_map.create_index(index_ref, NM_DB_TABLE_INDEX, key_options);
    rc = (SDB_IXM_REDEF == get_sdb_code(rc)) ? 0 : rc;
    if (0 != rc) {
      SDB_LOG_ERROR("HA: Unable to create index for '%s', sequoiadb error: %d",
                    NM_TABLE_MAP, rc);
      goto error;
    }
    indexes_created = true;
  }
done:
  return rc;
error:
  goto done;
}

static int check_table_mapping_limit(Sdb_conn *sdb_conn,
                                     const char *sql_group_cs_name,
                                     const char *cs_name, int &cl_count) {
  int rc = 0;
  bson::BSONObj obj;

  static const char GET_CL_COUNT[] =
      "SELECT COUNT(CSName) AS CLCount FROM %s.%s WHERE CSName = '%s' AND "
      "CLCount = 1";
  char query[sizeof(GET_CL_COUNT) + SDB_CS_NAME_MAX_SIZE * 3] = {0};
  sprintf(query, GET_CL_COUNT, sql_group_cs_name, NM_TABLE_MAP, cs_name);

  rc = sdb_conn->execute(query);
  rc = rc ? rc : sdb_conn->next(obj, false);
  if (0 != rc) {
    SDB_LOG_ERROR("Failed to query mapping table, error: %d", rc);
    goto error;
  }
  cl_count = obj.getIntField(NM_FIELD_CL_COUNT);
done:
  return rc;
error:
  goto done;
}

static void remove_dot(char *cs_name) {
  for (uint i = 0; i < strlen(cs_name); i++) {
    if ('.' == cs_name[i]) {
      cs_name[i] = '_';
    }
  }
}

int MetadataMapping::calculate_mapping_slot(Sdb_conn *sdb_conn,
                                            const char *sql_group_cs_name,
                                            const char *db_name, int &slot) {
  int rc = 0;
  int i = 0;
  bool cs_slot_is_occupied[NM_MAX_MAPPING_GROUP_SIZE];
  const char *cs_name = NULL;
  bson::BSONObj obj;

  while (i < m_mapping_group_num) {
    cs_slot_is_occupied[i++] = false;
  }
  static const char GET_TABLE_MAP_STATS[] =
      "SELECT CSName, COUNT(CLCount) AS CLCount FROM %s.%s WHERE DBName = '%s' "
      " AND CSName <> '%s' GROUP BY CSName ORDER BY CLCount DESC";
  char query[sizeof(GET_TABLE_MAP_STATS) + SDB_CS_NAME_MAX_SIZE * 3] = {0};
  sprintf(query, GET_TABLE_MAP_STATS, sql_group_cs_name, NM_TABLE_MAP, db_name,
          db_name);
  rc = sdb_conn->execute(query);
  if (0 != rc) {
    goto error;
  }

  while (0 == (rc = sdb_conn->next(obj, false))) {
    cs_name = obj.getStringField(NM_FIELD_CS_NAME);
    int cs_cnt = obj.getIntField(NM_FIELD_CL_COUNT);
    if (NULL == cs_name || cs_name[0] == '\0') {
      // start with "group_name#db_name#0"
      slot = 0;
      break;
    }
    slot = atoi(strrchr(cs_name, '#') + 1);
    if (cs_cnt < m_mapping_group_size) {
      break;
    } else {
      cs_slot_is_occupied[slot] = true;
    }
  }

  rc = (rc == HA_ERR_END_OF_FILE) ? 0 : rc;
  if (cs_slot_is_occupied[slot]) {
    i = slot;
    do {
      i = (i + 1) % m_mapping_group_num;
      if (!cs_slot_is_occupied[i]) {
        slot = i;
        break;
      }
    } while (i != slot);
  }

  DBUG_ASSERT(slot < m_mapping_group_num);
  if (cs_slot_is_occupied[slot]) {
    rc = SDB_HA_EXCEPTION;
    SDB_LOG_ERROR("NM: too many tables in current database, limit: %d",
                  m_mapping_group_size * m_mapping_group_num);
    goto error;
  }
done:
  return rc;
error:
  goto done;
}

int MetadataMapping::calculate_mapping_cs(Sdb_conn *sdb_conn,
                                          const char *db_name, char *cs_name,
                                          const char *sql_group_name,
                                          bool pefer_origin_name) {
  int rc = 0, slot = -1;
  char sql_group_cs_name[SDB_CS_NAME_MAX_SIZE + 1] = {0};
  snprintf(sql_group_cs_name, SDB_CS_NAME_MAX_SIZE, "%s_%s",
           NM_SQL_GROUP_PREFIX, sql_group_name);

  if (pefer_origin_name) {
    int cl_num = 0;
    // check if original name can be used
    rc =
        check_table_mapping_limit(sdb_conn, sql_group_cs_name, db_name, cl_num);
    if (0 != rc) {
      SDB_LOG_ERROR(
          "Failed to check if the number of tables in '%s' exceed the limit "
          "while calculating mapping CS, error: %d",
          db_name, rc);
      goto error;
    } else if (cl_num < m_mapping_group_size) {  // try to use original name
      snprintf(cs_name, SDB_CS_NAME_MAX_SIZE, "%s", db_name);
      goto done;
    }
  }

  // 'pefer_origin_name' false or the number of tables in database
  // 'db_name' exceed the limit
  rc = calculate_mapping_slot(sdb_conn, sql_group_cs_name, db_name, slot);
  if (0 != rc) {
    SDB_LOG_ERROR("Failed to calculate mapping CS, error: %d", rc);
    goto error;
  }
  snprintf(cs_name, SDB_CS_NAME_MAX_SIZE, "%s#%s#%d", sql_group_name, db_name,
           slot);
done:
  return rc;
error:
  goto done;
}

int MetadataMapping::get_table_mapping(Sdb_conn *sdb_conn, const char *db_name,
                                       const char *table_name, char *cs_name,
                                       char *cl_name, bool is_tmp_table,
                                       enum_mapping_state *state,
                                       int *cl_count) {
  int rc = 0;
  Sdb_cl mapping_table;
  char sql_group_cs_name[SDB_CS_NAME_MAX_SIZE + 1] = {0};
  bson::BSONObj obj, cond;
  bson::BSONObjBuilder cond_builder;
  char tmp_table_db_name[SDB_CS_NAME_MAX_SIZE + 1] = {0};
  snprintf(sql_group_cs_name, SDB_CS_NAME_MAX_SIZE, "%s_%s",
           NM_SQL_GROUP_PREFIX, m_sql_group);

  // build database name for temporary table
  if (is_tmp_table) {
    snprintf(tmp_table_db_name, SDB_CS_NAME_MAX_SIZE, "%s#%s#%s#%lu",
             m_sql_group, glob_hostname, db_name, (ulong)getpid());
    db_name = tmp_table_db_name;
  }

  if (!m_enabled) {
    snprintf(cs_name, SDB_CS_NAME_MAX_SIZE, "%s", db_name);
    if (is_tmp_table) {
      remove_dot(cs_name);
    }
    goto done;
  }

  rc = get_table_map_cl(*sdb_conn, sql_group_cs_name, mapping_table);
  if (SDB_CLS_GRP_NOT_EXIST == get_sdb_code(rc)) {
    // group used to store mapping info does not exist, use original name
    sprintf(cs_name, "%s", db_name);
    rc = 0;
    goto done;
  } else if (0 != rc) {
    SDB_LOG_ERROR("Failed to get mapping table, error: %d", rc);
    goto error;
  }

  cond_builder.append(NM_FIELD_DB_NAME, db_name);
  cond_builder.append(NM_FIELD_TABLE_NAME, table_name);
  cond = cond_builder.done();

  rc = mapping_table.query(cond);
  rc = rc ? rc : mapping_table.next(obj, false);
  if (0 == rc) {
    const char *mapping_name = NULL;
    mapping_name = obj.getStringField(NM_FIELD_CS_NAME);
    DBUG_ASSERT('\0' != mapping_name[0]);
    sprintf(cs_name, "%s", mapping_name);
    if (NULL != state) {
      *state = (enum_mapping_state)obj.getIntField(NM_FIELD_STATE);
      DBUG_ASSERT(*state == NM_STATE_CREATING || *state == NM_STATE_CREATED);
    }
    if (NULL != cl_count) {
      *cl_count = obj.getIntField(NM_FIELD_CL_COUNT);
    }
  } else if (HA_ERR_END_OF_FILE == rc) {
    rc = 0;
  } else {
    SDB_LOG_ERROR("Failed to query table mapping collection, error: %d", rc);
  }
done:
  return rc;
error:
  goto done;
}

int MetadataMapping::add_table_mapping(Sdb_conn *sdb_conn, const char *db_name,
                                       const char *table_name, char *cs_name,
                                       char *cl_name, bool pefer_origin_name,
                                       bool is_tmp_table, bool is_part_table) {
  int rc = 0, cl_count = 0, retry_count = 0, cl_num = 0;
  Sdb_cl mapping_table;
  char sql_group_cs_name[SDB_CS_NAME_MAX_SIZE + 1] = {0};
  bson::BSONObj rule, cond, obj;
  bson::BSONObjBuilder rule_builder, obj_builder, cond_builder;
  char tmp_table_db_name[SDB_CS_NAME_MAX_SIZE + 1] = {0};
  bool inserted = false;

  snprintf(sql_group_cs_name, SDB_CS_NAME_MAX_SIZE, "%s_%s",
           NM_SQL_GROUP_PREFIX, m_sql_group);

  // build database name for temporary table
  if (is_tmp_table) {
    snprintf(tmp_table_db_name, SDB_CS_NAME_MAX_SIZE, "%s#%s#%s#%lu",
             m_sql_group, glob_hostname, db_name, (ulong)getpid());
    db_name = tmp_table_db_name;
  }

  if (!m_enabled) {
    snprintf(cs_name, SDB_CS_NAME_MAX_SIZE, "%s", db_name);
    if (is_tmp_table) {
      remove_dot(cs_name);
    }
    goto done;
  }

retry:
  // 1. calculate mapping CS name
  rc = calculate_mapping_cs(sdb_conn, db_name, cs_name, m_sql_group,
                            pefer_origin_name);
  if (0 != rc) {
    SDB_LOG_ERROR("Failed to calculate mapping CS, error: %d", rc);
    goto error;
  }
  if (is_tmp_table) {
    remove_dot(cs_name);
  }

  // 2. persistent mapping info
  rc = get_table_map_cl(*sdb_conn, sql_group_cs_name, mapping_table);
  if (0 != rc) {
    SDB_LOG_ERROR("Failed to get mapping table, error: %d", rc);
    goto error;
  }
  cl_count = (is_part_table) ? 0 : 1;
  cond_builder.reset();
  cond_builder.append(NM_FIELD_DB_NAME, db_name);
  cond_builder.append(NM_FIELD_TABLE_NAME, table_name);
  cond = cond_builder.done();

  obj_builder.reset();
  obj_builder.append(NM_FIELD_CS_NAME, cs_name);
  obj_builder.append(NM_FIELD_CL_NAME, cl_name);
  obj_builder.append(NM_FIELD_CL_COUNT, cl_count);
  obj_builder.append(NM_FIELD_STATE, (int)NM_STATE_CREATING);
  obj = obj_builder.done();

  rule = BSON("$set" << obj);
  rc = mapping_table.upsert(rule, cond);
  if (0 != rc) {
    SDB_LOG_ERROR("Failed to add table mapping '%s.%s', error: %d", db_name,
                  table_name, rc);
    goto error;
  }
  inserted = true;

  // 3.  check if the number of collection in 'cs_name' exceed the limit
  rc = check_table_mapping_limit(sdb_conn, sql_group_cs_name, cs_name, cl_num);
  if (0 != rc) {
    SDB_LOG_ERROR(
        "Failed to check if the number of mapping in '%s' exceed the limit "
        "after adding table mapping, error: %d",
        cs_name, rc);
    goto error;
  }

  if (cl_num > m_mapping_group_size && retry_count < SDB_MAX_RETRY_TIME) {
    retry_count++;
    rc = mapping_table.del(cond);
    if (0 != rc) {
      SDB_LOG_ERROR("Failed to clear table '%s.%s' mapping", db_name,
                    table_name);
      goto error;
    }
    cl_num = 0;
    inserted = false;
    goto retry;
  } else if (cl_num > m_mapping_group_size) {
    SDB_LOG_ERROR("Number of table mapping in '%s' exceed the limit %d",
                  cs_name, m_mapping_group_size);
    rc = SDB_DMS_NOSPC;
  }
done:
  return rc;
error:
  if (inserted) {
    rc = mapping_table.del(cond);
    if (0 != rc) {
      SDB_LOG_ERROR("Failed to clear table '%s.%s' mapping", db_name,
                    table_name);
    }
  }
  goto done;
}

int MetadataMapping::remove_table_mapping(Sdb_conn *sdb_conn,
                                          const char *db_name,
                                          const char *table_name,
                                          bool is_tmp_table) {
  int rc = 0;
  Sdb_cl mapping_table;
  char sql_group_cs_name[SDB_CS_NAME_MAX_SIZE + 1] = {0};
  bson::BSONObj cond;
  bson::BSONObjBuilder cond_builder;
  char tmp_table_db_name[SDB_CS_NAME_MAX_SIZE + 1] = {0};

  if (!m_enabled) {
    goto done;
  }
  snprintf(sql_group_cs_name, SDB_CS_NAME_MAX_SIZE, "%s_%s",
           NM_SQL_GROUP_PREFIX, m_sql_group);

  // build database name for temporary table
  if (is_tmp_table) {
    snprintf(tmp_table_db_name, SDB_CS_NAME_MAX_SIZE, "%s#%s#%s#%lu",
             m_sql_group, glob_hostname, db_name, (ulong)getpid());
    db_name = tmp_table_db_name;
  }

  rc = get_table_map_cl(*sdb_conn, sql_group_cs_name, mapping_table);
  if (0 != rc) {
    SDB_LOG_ERROR("Failed to get mapping table, error: %d", rc);
    goto error;
  }
  cond_builder.append(NM_FIELD_DB_NAME, db_name);
  cond_builder.append(NM_FIELD_TABLE_NAME, table_name);
  cond = cond_builder.done();

  rc = mapping_table.del(cond);
  if (0 != rc) {
    SDB_LOG_ERROR("Failed to remove table mapping '%s.%s'", db_name,
                  table_name);
    goto error;
  }
done:
  return 0;
error:
  goto done;
}

int MetadataMapping::set_table_mapping_state(Sdb_conn *sdb_conn,
                                             const char *db_name,
                                             const char *table_name,
                                             bool is_tmp_table,
                                             enum_mapping_state state) {
  int rc = 0;
  Sdb_cl mapping_table;
  char sql_group_cs_name[SDB_CS_NAME_MAX_SIZE + 1] = {0};
  bson::BSONObj rule, cond, obj;
  bson::BSONObjBuilder rule_builder, cond_builder;
  char tmp_table_db_name[SDB_CS_NAME_MAX_SIZE + 1] = {0};

  if (!m_enabled) {
    goto done;
  }
  snprintf(sql_group_cs_name, SDB_CS_NAME_MAX_SIZE, "%s_%s",
           NM_SQL_GROUP_PREFIX, m_sql_group);

  // build database name for temporary table
  if (is_tmp_table) {
    snprintf(tmp_table_db_name, SDB_CS_NAME_MAX_SIZE, "%s#%s#%s#%lu",
             m_sql_group, glob_hostname, db_name, (ulong)getpid());
    db_name = tmp_table_db_name;
  }

  rc = get_table_map_cl(*sdb_conn, sql_group_cs_name, mapping_table);
  if (0 != rc) {
    SDB_LOG_ERROR("Failed to get mapping table, error: %d", rc);
    goto error;
  }
  cond_builder.append(NM_FIELD_DB_NAME, db_name);
  cond_builder.append(NM_FIELD_TABLE_NAME, table_name);
  cond = cond_builder.done();

  obj = BSON(NM_FIELD_STATE << state);
  rule = BSON("$set" << obj);
  rc = mapping_table.update(rule, cond);
  if (0 != rc) {
    SDB_LOG_ERROR("Failed to update mapping '%s.%s' state to %d, error: %d",
                  db_name, table_name, state, rc);
    goto error;
  }
done:
  return 0;
error:
  goto done;
}

int MetadataMapping::update_table_mapping(
    Sdb_conn *sdb_conn, const char *src_db_name, const char *src_table_name,
    const char *dst_db_name, const char *dst_table_name, bool is_tmp_table) {
  int rc = 0;
  Sdb_cl mapping_table;
  char sql_group_cs_name[SDB_CS_NAME_MAX_SIZE + 1] = {0};
  bson::BSONObj rule, cond, obj;
  bson::BSONObjBuilder rule_builder, cond_builder;
  char tmp_table_db_name[SDB_CS_NAME_MAX_SIZE + 1] = {0};

  if (!m_enabled) {
    goto done;
  }
  snprintf(sql_group_cs_name, SDB_CS_NAME_MAX_SIZE, "%s_%s",
           NM_SQL_GROUP_PREFIX, m_sql_group);

  // build database name for temporary table
  if (is_tmp_table) {
    snprintf(tmp_table_db_name, SDB_CS_NAME_MAX_SIZE, "%s#%s#%s#%lu",
             m_sql_group, glob_hostname, src_db_name, (ulong)getpid());
    src_db_name = tmp_table_db_name;
  }

  rc = get_table_map_cl(*sdb_conn, sql_group_cs_name, mapping_table);
  if (0 != rc) {
    SDB_LOG_ERROR("Failed to get mapping table, error: %d", rc);
    goto error;
  }
  cond_builder.append(NM_FIELD_DB_NAME, src_db_name);
  cond_builder.append(NM_FIELD_TABLE_NAME, src_table_name);
  cond = cond_builder.done();

  obj = BSON(NM_FIELD_TABLE_NAME << dst_table_name << NM_FIELD_CL_NAME
                                 << dst_table_name);
  rule = BSON("$set" << obj);
  rc = mapping_table.update(rule, cond);
  if (0 != rc) {
    SDB_LOG_ERROR("Failed to update mapping '%s.%s' to '%s.%s', error: %d",
                  src_db_name, src_table_name, dst_db_name, dst_table_name, rc);
    goto error;
  }
done:
  return 0;
error:
  goto done;
}

int MetadataMapping::get_sequence_mapping_cs(const char *db_name, char *cs_name,
                                             bool is_tmp_table,
                                             bool pefer_origin_name) {
  int rc = 0;
  if (is_tmp_table) {  // handle temporary sequence
    // database name of temporary table "hostname#db_name#pid"
    snprintf(cs_name, SDB_CS_NAME_MAX_SIZE, "%s#%s#%lu", glob_hostname, db_name,
             (ulong)getpid());
    remove_dot(cs_name);
  } else if (pefer_origin_name) {
    // only one instance group or instance group function is not open
    sprintf(cs_name, "%s", db_name);
  } else {  // multi instance group situation, format: InstGroupName#DBName#0
    if (m_enabled) {
      snprintf(cs_name, SDB_CS_NAME_MAX_SIZE, "%s#%s#0", m_sql_group, db_name);
    } else {
      snprintf(cs_name, SDB_CS_NAME_MAX_SIZE, "%s", db_name);
    }
  }
  return rc;
}

int MetadataMapping::drop_db_mapping(Sdb_conn *sdb_conn, const char *db_name) {
  int rc = 0;
  Sdb_cl mapping_table;
  bson::BSONObj cond, hint;
  char sql_group_cs_name[SDB_CS_NAME_MAX_SIZE + 1] = {0};

  if (!m_enabled) {
    goto done;
  }
  snprintf(sql_group_cs_name, SDB_CS_NAME_MAX_SIZE, "%s_%s",
           NM_SQL_GROUP_PREFIX, m_sql_group);

  rc = get_table_map_cl(*sdb_conn, sql_group_cs_name, mapping_table);
  if (SDB_CLS_GRP_NOT_EXIST == get_sdb_code(rc)) {
    rc = 0;
    goto done;
  } else if (0 != rc) {
    SDB_LOG_ERROR("Failed to get mapping table, error: %d", rc);
    goto error;
  }

  cond = BSON(NM_FIELD_DB_NAME << db_name);
  rc = mapping_table.del(cond, hint);
  if (0 != rc) {
    SDB_LOG_ERROR("Failed to drop all table mapping in database '%s'", db_name);
    goto error;
  }
done:
  return rc;
error:
  goto done;
}

int MetadataMapping::remove_parts_mapping(Sdb_conn *sdb_conn,
                                          const char *db_name,
                                          const char *table_name) {
  int rc = 0;
  Sdb_cl mapping_table;
  char sql_group_cs_name[SDB_CS_NAME_MAX_SIZE + 1] = {0};
  bson::BSONObj obj, cond;
  char part_prefix[SDB_CL_NAME_MAX_SIZE + 1] = {0};

  if (!m_enabled) {
    goto done;
  }
  snprintf(part_prefix, SDB_CL_NAME_MAX_SIZE, "%s#P#", table_name);
  snprintf(sql_group_cs_name, SDB_CS_NAME_MAX_SIZE, "%s_%s",
           NM_SQL_GROUP_PREFIX, m_sql_group);

  rc = get_table_map_cl(*sdb_conn, sql_group_cs_name, mapping_table);
  if (0 != rc) {
    SDB_LOG_ERROR("Failed to get mapping table, error: %d", rc);
    goto error;
  }

  obj = BSON("$regex" << part_prefix);
  cond = BSON(NM_FIELD_DB_NAME << db_name << NM_FIELD_TABLE_NAME << obj);
  rc = mapping_table.del(cond);
  if (0 != rc) {
    SDB_LOG_ERROR("Failed to remove parts mapping '%s.%s'", db_name,
                  table_name);
    goto error;
  }
done:
  return 0;
error:
  goto done;
}
