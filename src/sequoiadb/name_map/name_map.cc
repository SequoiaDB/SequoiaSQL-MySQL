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

char Metadata_Mapping::m_sql_group[SDB_CS_NAME_MAX_SIZE + 1] = {0};
bool Metadata_Mapping::m_enabled = false;
int Metadata_Mapping::m_mapping_unit_size = NM_MAPPING_GROUP_SIZE;
int Metadata_Mapping::m_mapping_unit_count = NM_MAPPING_GROUP_NUM;
bool Metadata_Mapping::m_pefer_origin_name = true;

static int get_table_map_cl(Sdb_conn &sdb_conn, const char *sql_group_name,
                            Sdb_cl &table_map) {
  static bool indexes_created = false;
  const char *sql_group_cs_name = sql_group_name;

  int rc = 0;
  bson::BSONObj cl_options, index_ref, key_options;

  try {
    rc = sdb_conn.get_cl(sql_group_cs_name, NM_TABLE_MAP, table_map, true);
    if (SDB_DMS_CS_NOTEXIST == get_sdb_code(rc) ||
        SDB_DMS_NOTEXIST == get_sdb_code(rc)) {
      SDB_LOG_INFO("NM: Creating table '%s.%s'", sql_group_cs_name,
                   NM_TABLE_MAP);
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
        SDB_LOG_ERROR(
            "HA: Unable to create index for '%s', sequoiadb error: %d",
            NM_TABLE_MAP, rc);
        goto error;
      }
      indexes_created = true;
    }
  }
  SDB_EXCEPTION_CATCHER(rc, "Failed to get mapping table '%s', exception:%s",
                        NM_TABLE_MAP, e.what());
done:
  return rc;
error:
  goto done;
}

static int check_table_mapping_limit(Sdb_conn *sdb_conn,
                                     const char *sql_group_cs_name,
                                     const char *db_name, const char *cs_name,
                                     int &cl_count) {
  int rc = 0;
  bson::BSONObj obj;
  const char *tmp_cs_name = NULL;
  char query[SDB_CS_NAME_MAX_SIZE * 3 + 150] = {0};
  // "SELECT SUM(IsPhysicalTable) AS CLCount FROM
  // sql_group_cs_name.TABLE_MAPPING WHERE
  // DBName = 'db_name' AND CSName = 'cs_name'"
  snprintf(query, sizeof(query),
           "SELECT %s, SUM(%s) AS %s FROM %s.%s WHERE %s = '%s' AND %s = '%s'",
           NM_FIELD_CS_NAME, NM_FIELD_IS_PHY_TABLE, NM_FIELD_CL_COUNT,
           sql_group_cs_name, NM_TABLE_MAP, NM_FIELD_DB_NAME, db_name,
           NM_FIELD_CS_NAME, cs_name);

  rc = sdb_conn->execute(query);
  rc = rc ? rc : sdb_conn->next(obj, false);
  if (0 != rc) {
    SDB_LOG_ERROR("Failed to query mapping table, error: %d", rc);
    goto error;
  }
  tmp_cs_name = obj.getStringField(NM_FIELD_CS_NAME);
  if (NULL == tmp_cs_name || tmp_cs_name[0] == '\0') {
    cl_count = 0;
  } else {
    cl_count = obj.getIntField(NM_FIELD_CL_COUNT);
  }
  DBUG_ASSERT(cl_count >= 0);
done:
  return rc;
error:
  goto done;
}

int Metadata_Mapping::add_mapping(const char *db_name, const char *table_name) {
  return add_table_mapping(*m_sdb_conn, db_name, table_name);
}

int Metadata_Mapping::delete_mapping(const char *db_name,
                                     const char *table_name) {
  return remove_table_mapping(*m_sdb_conn, db_name, table_name);
}

int Metadata_Mapping::get_mapping(const char *db_name, const char *table_name) {
  return get_table_mapping(*m_sdb_conn, db_name, table_name);
}

int Metadata_Mapping::rename_mapping(const char *db_name, const char *from,
                                     const char *to) {
  return update_table_mapping(*m_sdb_conn, db_name, from, db_name, to);
}

int Metadata_Mapping::get_fixed_mapping(const char *db_name,
                                        const char *table_name) {
  return get_sequence_mapping_cs(db_name, table_name);
}

int Metadata_Mapping::calculate_mapping_slot(Sdb_conn *sdb_conn,
                                             const char *sql_group_cs_name,
                                             const char *db_name, int &slot) {
  int rc = 0;
  int i = 0;
  bool cs_slot_is_occupied[NM_MAX_MAPPING_GROUP_SIZE];
  const char *cs_name = NULL;
  bson::BSONObj obj;

  while (i < m_mapping_unit_count) {
    cs_slot_is_occupied[i++] = false;
  }
  char query[SDB_CS_NAME_MAX_SIZE * 3 + 150] = {0};
  // "SELECT CSName, COUNT(IsPhysicalTable) AS CLCount FROM %s.%s WHERE DBName =
  // '%s' AND CSName <> '%s' GROUP BY CSName ORDER BY CLCount DESC"
  snprintf(query, sizeof(query),
           "SELECT %s, SUM(%s) AS %s FROM %s.%s WHERE %s = '%s' AND %s <> "
           "'%s' GROUP BY %s ORDER BY %s ASC",
           NM_FIELD_CS_NAME, NM_FIELD_IS_PHY_TABLE, NM_FIELD_CL_COUNT,
           sql_group_cs_name, NM_TABLE_MAP, NM_FIELD_DB_NAME, db_name,
           NM_FIELD_CS_NAME, db_name, NM_FIELD_CS_NAME, NM_FIELD_CL_COUNT);
  rc = sdb_conn->execute(query);
  if (0 != rc) {
    goto error;
  }

  while (0 == (rc = sdb_conn->next(obj, false))) {
    cs_name = obj.getStringField(NM_FIELD_CS_NAME);
    int cl_cnt = obj.getIntField(NM_FIELD_CL_COUNT);
    if (NULL == cs_name || cs_name[0] == '\0') {
      // start with "group_name#db_name#0"
      slot = 0;
      break;
    }
    slot = atoi(strrchr(cs_name, '#') + 1);
    if (cl_cnt < m_mapping_unit_size) {
      break;
    } else {
      cs_slot_is_occupied[slot] = true;
    }
  }

  rc = (rc == HA_ERR_END_OF_FILE) ? 0 : rc;
  if (cs_slot_is_occupied[slot]) {
    i = slot;
    do {
      i = (i + 1) % m_mapping_unit_count;
      if (!cs_slot_is_occupied[i]) {
        slot = i;
        break;
      }
    } while (i != slot);
  }

  DBUG_ASSERT(slot < m_mapping_unit_count);
  if (cs_slot_is_occupied[slot]) {
    rc = SDB_HA_EXCEPTION;
    SDB_LOG_ERROR("NM: too many tables in current database, limit: %d",
                  m_mapping_unit_size * m_mapping_unit_count);
    goto error;
  }
done:
  return rc;
error:
  goto done;
}

int Metadata_Mapping::calculate_mapping_cs(Sdb_conn *sdb_conn,
                                           const char *db_name, char *cs_name,
                                           const char *sql_group_name) {
  int rc = 0, slot = -1;
  char sql_group_cs_name[SDB_CS_NAME_MAX_SIZE + 1] = {0};
  snprintf(sql_group_cs_name, SDB_CS_NAME_MAX_SIZE, "%s_%s",
           NM_SQL_GROUP_PREFIX, sql_group_name);

  if (m_pefer_origin_name) {
    int cl_cnt = 0;
    // check if original name can be used
    rc = check_table_mapping_limit(sdb_conn, sql_group_cs_name, db_name,
                                   db_name, cl_cnt);
    if (0 != rc) {
      SDB_LOG_ERROR(
          "Failed to check if the number of tables in '%s' exceed the limit "
          "while calculating mapping CS, error: %d",
          db_name, rc);
      goto error;
    } else if (cl_cnt < m_mapping_unit_size) {  // try to use original name
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

  if (strlen(sql_group_name) + strlen(db_name) + 5 > SDB_CS_NAME_MAX_SIZE) {
    rc = HA_ERR_GENERIC;
    goto error;
  }
  if (NULL == strstr(db_name, "#")) {
    snprintf(cs_name, SDB_CS_NAME_MAX_SIZE, "%s#%s#%d", sql_group_name, db_name,
             slot);
  } else {
    // replace '#' with '@'
    int end = sprintf(cs_name, "%s#", sql_group_name);
    for (uint i = 0; i < strlen(db_name); i++) {
      cs_name[end++] = ('#' == db_name[i]) ? '@' : db_name[i];
    }
    sprintf(cs_name + end, "#%d", slot);
  }
done:
  return rc;
error:
  goto done;
}

int Metadata_Mapping::get_table_mapping(Sdb_conn *sdb_conn, const char *db_name,
                                        const char *table_name,
                                        enum_mapping_state *state) {
  int rc = 0;
  Sdb_cl mapping_table;
  char sql_group_cs_name[SDB_CS_NAME_MAX_SIZE + 1] = {0};
  bson::BSONObj obj, cond;
  bson::BSONObjBuilder cond_builder;
  // no mapping for table name
  snprintf(m_cl_name, SDB_CL_NAME_MAX_SIZE, "%s", table_name);
  if (!m_enabled) {
    snprintf(m_cs_name, SDB_CS_NAME_MAX_SIZE, "%s", db_name);
    goto done;
  }
  // use the cached name mapping
  if ('\0' != m_cs_name[0] && '\0' != m_cl_name[0]) {
    goto done;
  }
  snprintf(sql_group_cs_name, SDB_CS_NAME_MAX_SIZE, "%s_%s",
           NM_SQL_GROUP_PREFIX, m_sql_group);

  rc = get_table_map_cl(*sdb_conn, sql_group_cs_name, mapping_table);
  if (SDB_CLS_GRP_NOT_EXIST == get_sdb_code(rc)) {
    // group used to store mapping info does not exist, use original name
    sprintf(m_cs_name, "%s", db_name);
    sprintf(m_cl_name, "%s", table_name);
    rc = 0;
    goto done;
  } else if (0 != rc) {
    SDB_LOG_ERROR("Failed to get mapping table, error: %d", rc);
    goto error;
  }

  try {
    cond_builder.append(NM_FIELD_DB_NAME, db_name);
    cond_builder.append(NM_FIELD_TABLE_NAME, table_name);
    cond = cond_builder.done();
  }
  SDB_EXCEPTION_CATCHER(
      rc, "Failed to build bson while get '%s.%s' mapping, exception:%s",
      db_name, table_name, e.what());

  rc = mapping_table.query(cond);
  rc = rc ? rc : mapping_table.next(obj, false);
  if (0 == rc) {
    sprintf(m_cs_name, "%s", obj.getStringField(NM_FIELD_CS_NAME));
    sprintf(m_cl_name, "%s", obj.getStringField(NM_FIELD_CL_NAME));
    if (NULL != state) {
      *state = (enum_mapping_state)obj.getIntField(NM_FIELD_STATE);
      DBUG_ASSERT(*state == NM_STATE_CREATING || *state == NM_STATE_CREATED);
    }
    m_is_part_table = (obj.getIntField(NM_FIELD_IS_PHY_TABLE) == 0);
  } else if (HA_ERR_END_OF_FILE == rc) {
    // mapping does not exist for HASH or KEY partition
    goto error;
  } else {
    SDB_LOG_ERROR("Failed to query table mapping collection, error: %d", rc);
  }
done:
  return rc;
error:
  goto done;
}

int Metadata_Mapping::add_table_mapping(Sdb_conn *sdb_conn, const char *db_name,
                                        const char *table_name) {
  int rc = 0, retry_count = 0, cl_cnt = 0;
  Sdb_cl mapping_table;
  char sql_group_cs_name[SDB_CS_NAME_MAX_SIZE + 1] = {0};
  bson::BSONObj rule, cond, obj;
  bson::BSONObjBuilder rule_builder, obj_builder, cond_builder;
  bool inserted = false, is_phy_table = true;

  snprintf(sql_group_cs_name, SDB_CS_NAME_MAX_SIZE, "%s_%s",
           NM_SQL_GROUP_PREFIX, m_sql_group);
  // no mapping for table name
  snprintf(m_cl_name, SDB_CL_NAME_MAX_SIZE, "%s", table_name);
  if (!m_enabled) {
    snprintf(m_cs_name, SDB_CS_NAME_MAX_SIZE, "%s", db_name);
    goto done;
  }

retry:
  // 1. calculate mapping CS name
  rc = calculate_mapping_cs(sdb_conn, db_name, m_cs_name, m_sql_group);
  if (0 != rc) {
    SDB_LOG_ERROR("Failed to calculate mapping CS, error: %d", rc);
    goto error;
  }

  // 2. persistent mapping info
  rc = get_table_map_cl(*sdb_conn, sql_group_cs_name, mapping_table);
  if (0 != rc) {
    SDB_LOG_ERROR("Failed to get mapping table, error: %d", rc);
    goto error;
  }

  try {
    is_phy_table = (m_is_part_table) ? false : true;
    cond_builder.reset();
    cond_builder.append(NM_FIELD_DB_NAME, db_name);
    cond_builder.append(NM_FIELD_TABLE_NAME, table_name);
    cond = cond_builder.done();

    obj_builder.reset();
    obj_builder.append(NM_FIELD_CS_NAME, m_cs_name);
    obj_builder.append(NM_FIELD_CL_NAME, table_name);
    obj_builder.append(NM_FIELD_IS_PHY_TABLE, (int)is_phy_table);
    obj_builder.append(NM_FIELD_STATE, (int)NM_STATE_CREATING);
    obj = obj_builder.done();

    rule = BSON("$set" << obj);
  }
  SDB_EXCEPTION_CATCHER(
      rc, "Failed to build bson while add '%s.%s' mapping, exception:%s",
      db_name, table_name, e.what());

  rc = mapping_table.upsert(rule, cond);
  if (0 != rc) {
    SDB_LOG_ERROR("Failed to add table mapping '%s.%s', error: %d", db_name,
                  table_name, rc);
    goto error;
  }
  inserted = true;

  // 3.  check if the number of collection in 'cs_name' exceed the limit
  rc = check_table_mapping_limit(sdb_conn, sql_group_cs_name, db_name,
                                 m_cs_name, cl_cnt);
  if (0 != rc) {
    SDB_LOG_ERROR(
        "Failed to check if the number of mapping in '%s' exceed the limit "
        "after adding table mapping, error: %d",
        m_cs_name, rc);
    goto error;
  }

  if (cl_cnt > m_mapping_unit_size && retry_count < SDB_MAX_RETRY_TIME) {
    retry_count++;
    rc = mapping_table.del(cond);
    if (0 != rc) {
      SDB_LOG_ERROR("Failed to clear table '%s.%s' mapping, retry count:%d",
                    db_name, table_name, retry_count);
      goto error;
    }
    cl_cnt = 0;
    inserted = false;
    goto retry;
  } else if (cl_cnt > m_mapping_unit_size) {
    SDB_LOG_ERROR("Number of table mapping in '%s' exceed the limit %d",
                  m_cs_name, m_mapping_unit_size);
    rc = SDB_DMS_NOSPC;
  }
done:
  return rc;
error:
  if (inserted) {
    if (0 != mapping_table.del(cond)) {
      SDB_LOG_WARNING("Failed to clear table '%s.%s' mapping", db_name,
                      table_name);
    }
  }
  goto done;
}

int Metadata_Mapping::remove_table_mapping(Sdb_conn *sdb_conn,
                                           const char *db_name,
                                           const char *table_name) {
  int rc = 0;
  Sdb_cl mapping_table;
  char sql_group_cs_name[SDB_CS_NAME_MAX_SIZE + 1] = {0};
  bson::BSONObj cond;
  bson::BSONObjBuilder cond_builder;

  if (!m_enabled) {
    goto done;
  }
  try {
    snprintf(sql_group_cs_name, SDB_CS_NAME_MAX_SIZE, "%s_%s",
             NM_SQL_GROUP_PREFIX, m_sql_group);

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

#ifdef IS_MYSQL
    // delete partition mapping for MySQL
    if (m_is_part_table) {
      bson::BSONObj obj;
      char part_prefix[SDB_CL_NAME_MAX_SIZE + 1] = {0};
      snprintf(part_prefix, SDB_CL_NAME_MAX_SIZE, "%s#P#", table_name);
      obj = BSON("$regex" << part_prefix);
      cond = BSON(NM_FIELD_DB_NAME << db_name << NM_FIELD_TABLE_NAME << obj);
      rc = mapping_table.del(cond);
      if (0 != rc) {
        SDB_LOG_ERROR("Failed to remove parts mapping '%s.%s'", db_name,
                      table_name);
        goto error;
      }
    }
#endif
  }
  SDB_EXCEPTION_CATCHER(
      rc, "Failed to build bson while removing '%s.%s' mapping, exception:%s",
      db_name, table_name, e.what());
done:
  return 0;
error:
  goto done;
}

int Metadata_Mapping::set_table_mapping_state(Sdb_conn *sdb_conn,
                                              const char *db_name,
                                              const char *table_name,
                                              enum_mapping_state state) {
  int rc = 0;
  Sdb_cl mapping_table;
  char sql_group_cs_name[SDB_CS_NAME_MAX_SIZE + 1] = {0};
  bson::BSONObj rule, cond, obj;
  bson::BSONObjBuilder rule_builder, cond_builder;

  if (!m_enabled) {
    goto done;
  }
  snprintf(sql_group_cs_name, SDB_CS_NAME_MAX_SIZE, "%s_%s",
           NM_SQL_GROUP_PREFIX, m_sql_group);

  rc = get_table_map_cl(*sdb_conn, sql_group_cs_name, mapping_table);
  if (0 != rc) {
    SDB_LOG_ERROR("Failed to get mapping table, error: %d", rc);
    goto error;
  }
  try {
    cond_builder.append(NM_FIELD_DB_NAME, db_name);
    cond_builder.append(NM_FIELD_TABLE_NAME, table_name);
    cond = cond_builder.done();

    obj = BSON(NM_FIELD_STATE << state);
    rule = BSON("$set" << obj);
  }
  SDB_EXCEPTION_CATCHER(rc,
                        "Failed to setting '%s.%s' mapping state, exception:%s",
                        db_name, table_name, e.what());
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

int Metadata_Mapping::update_table_mapping(Sdb_conn *sdb_conn,
                                           const char *src_db_name,
                                           const char *src_table_name,
                                           const char *dst_db_name,
                                           const char *dst_table_name) {
  int rc = 0;
  Sdb_cl mapping_table;
  char sql_group_cs_name[SDB_CS_NAME_MAX_SIZE + 1] = {0};
  bson::BSONObj rule, cond, obj;
  bson::BSONObjBuilder rule_builder, cond_builder;

  if (!m_enabled) {
    goto done;
  }
  snprintf(sql_group_cs_name, SDB_CS_NAME_MAX_SIZE, "%s_%s",
           NM_SQL_GROUP_PREFIX, m_sql_group);

  rc = get_table_map_cl(*sdb_conn, sql_group_cs_name, mapping_table);
  if (0 != rc) {
    SDB_LOG_ERROR("Failed to get mapping table, error: %d", rc);
    goto error;
  }
  try {
    cond_builder.append(NM_FIELD_DB_NAME, src_db_name);
    cond_builder.append(NM_FIELD_TABLE_NAME, src_table_name);
    cond = cond_builder.done();

    obj = BSON(NM_FIELD_TABLE_NAME << dst_table_name << NM_FIELD_CL_NAME
                                   << dst_table_name);
    rule = BSON("$set" << obj);
  }
  SDB_EXCEPTION_CATCHER(rc,
                        "Failed to build bson while updating '%s.%s' mapping",
                        src_db_name, src_table_name);
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

int Metadata_Mapping::get_mapping_cs_by_db(Sdb_conn *sdb_conn,
                                           const char *db_name,
                                           std::vector<String> &mapping_cs) {
  int rc = 0;
  char sql_group_cs_name[SDB_CS_NAME_MAX_SIZE + 1] = {0};
  bson::BSONObj obj;
  char query[SDB_CS_NAME_MAX_SIZE * 3 + 100] = {0};

  if (!m_enabled) {
    String tmp_str(strlen(db_name));
    tmp_str.append(db_name);
    mapping_cs.push_back(tmp_str);
    goto done;
  }

  snprintf(sql_group_cs_name, SDB_CS_NAME_MAX_SIZE, "%s_%s",
           NM_SQL_GROUP_PREFIX, m_sql_group);

  // SELECT CSName, COUNT(CSName) AS CNT FROM %s.%s
  // WHERE DBName = '%s' GROUP BY CSName
  sprintf(query,
          "SELECT %s, COUNT(%s) AS CNT FROM %s.%s WHERE %s = '%s' GROUP BY %s",
          NM_FIELD_CS_NAME, NM_FIELD_CS_NAME, sql_group_cs_name, NM_TABLE_MAP,
          NM_FIELD_DB_NAME, db_name, NM_FIELD_CS_NAME);
  rc = sdb_conn->execute(query);
  if (0 != rc) {
    SDB_LOG_ERROR(
        "Failed to query mapping CS for database '%s' before dropping database",
        db_name);
    goto error;
  } else if (0 == rc) {
    while (0 == (rc = sdb_conn->next(obj, false))) {
      const char *cs_name = obj.getStringField(NM_FIELD_CS_NAME);
      if ('\0' != cs_name[0]) {
        String tmp_str(strlength(cs_name));
        tmp_str.append(cs_name);
        mapping_cs.push_back(tmp_str);
      }
    }
    if (HA_ERR_END_OF_FILE == rc) {
      rc = 0;
    }
  }
done:
  return rc;
error:
  goto error;
}

int Metadata_Mapping::get_sequence_mapping_cs(const char *db_name,
                                              const char *table_name) {
  int rc = 0;
  snprintf(m_cl_name, SDB_CL_NAME_MAX_SIZE, "%s", table_name);
  if (m_pefer_origin_name) {
    // only one instance group or instance group function is not open
    snprintf(m_cs_name, SDB_CL_NAME_MAX_SIZE, "%s", db_name);
    goto done;
  }

  // mapping not enabled
  if (!m_enabled) {
    snprintf(m_cs_name, SDB_CS_NAME_MAX_SIZE, "%s", db_name);
    goto done;
  }

  if (strlen(m_sql_group) + strlen(db_name) + 3 > SDB_CS_NAME_MAX_SIZE) {
    rc = HA_ERR_GENERIC;
    goto error;
  }

  // multi instance group with mapping
  if (NULL == strstr(db_name, "#")) {
    snprintf(m_cs_name, SDB_CS_NAME_MAX_SIZE, "%s#%s#0", m_sql_group, db_name);
  } else {
    // replace '#' with '@'
    int end = sprintf(m_cs_name, "%s#", m_sql_group);
    for (uint i = 0; i < strlen(db_name); i++) {
      m_cs_name[end++] = ('#' == db_name[i]) ? '@' : db_name[i];
    }
    sprintf(m_cs_name + end, "#0");
  }
done:
  return rc;
error:
  goto done;
}

int Metadata_Mapping::remove_table_mappings(Sdb_conn *sdb_conn,
                                            const char *db_name) {
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
  try {
    cond = BSON(NM_FIELD_DB_NAME << db_name);
  }
  SDB_EXCEPTION_CATCHER(rc,
                        "Failed to build bson while remove table mappings for "
                        "database:%s, exception:%s",
                        db_name, e.what());
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
