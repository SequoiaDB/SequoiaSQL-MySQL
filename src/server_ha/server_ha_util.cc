#include "server_ha_util.h"
#include "ha_sdb_errcode.h"

// get Sdb_conn error string
const char *ha_error_string(Sdb_conn &sdb_conn, int rc, char err_buf[]) {
  memset(err_buf, '0', HA_BUF_LEN);
  if (rc > SDB_ERR_INNER_CODE_BEGIN && rc < SDB_ERR_INNER_CODE_END) {
    bson::BSONObj result;
    if (sdb_conn.get_last_result_obj(result, false)) {
      sprintf(err_buf, "failed to get sequoiadb error, error code is: %d", rc);
    } else {
      const char *err = result.getStringField(SDB_FIELD_DESCRIPTION);
      if (0 == strlen(err)) {
        err = result.getStringField(SDB_FIELD_DETAIL);
      }
      snprintf(err_buf, HA_BUF_LEN, "%s", err);
    }
  } else {
    snprintf(err_buf, HA_BUF_LEN, "connector internal error code: %d", rc);
  }
  return err_buf;
}

// quote database or table name with ``, use '`' as escape character
char *ha_quote_name(const char *name, char *buff) {
  char *to = buff;
  char qtype = '`';

  *to++ = qtype;
  while (*name) {
    if (*name == qtype)
      *to++ = qtype;
    *to++ = *name++;
  }
  to[0] = qtype;
  to[1] = 0;
  return buff;
}

int ha_get_instance_state_cl(Sdb_conn &sdb_conn, const char *group_name,
                             Sdb_cl &cl) {
  static bool indexes_created = false;

  int rc = 0;
  char err_buf[HA_BUF_LEN] = {0};
  bson::BSONObj cl_options, index_ref, key_options;

  rc = sdb_conn.get_cl((char *)group_name, HA_INSTANCE_STATE_CL, cl);
  if (SDB_DMS_NOTEXIST == get_sdb_code(rc)) {
    sql_print_information(
        "HA: Instance state table '%s' does not exist, create it",
        HA_INSTANCE_STATE_CL);
    rc = sdb_conn.create_cl((char *)group_name, HA_INSTANCE_STATE_CL,
                            cl_options);
    rc = (SDB_DMS_EXIST == get_sdb_code(rc)) ? 0 : rc;
    HA_RC_CHECK(
        rc, error,
        "HA: Unable to create instance state table '%s', sequoiadb error: %s",
        HA_INSTANCE_STATE_CL, ha_error_string(sdb_conn, rc, err_buf));
  }

  if (!indexes_created) {
    rc = sdb_conn.get_cl((char *)group_name, HA_INSTANCE_STATE_CL, cl);
    HA_RC_CHECK(
        rc, error,
        "HA: Unable to get instance state table '%s', sequoiadb error: %s",
        HA_INSTANCE_STATE_CL, ha_error_string(sdb_conn, rc, err_buf));

    index_ref = BSON(HA_FIELD_DB << 1 << HA_FIELD_TABLE << 1 << HA_FIELD_TYPE
                                 << 1 << HA_FIELD_INSTANCE_ID << 1);
    key_options = BSON(SDB_FIELD_UNIQUE << 1);

    rc = cl.create_index(index_ref, HA_INST_STATE_DB_TABLE_TYPE_INSTID_INDEX,
                         key_options);
    rc = (SDB_IXM_REDEF == get_sdb_code(rc)) ? 0 : rc;
    HA_RC_CHECK(rc, error,
                "HA: Unable to create index for '%s', sequoiadb error: %s",
                HA_INSTANCE_STATE_CL, ha_error_string(sdb_conn, rc, err_buf));
    indexes_created = true;
  }
done:
  return rc;
error:
  goto done;
}

int ha_get_global_state_cl(Sdb_conn &sdb_conn, const char *group_name,
                           Sdb_cl &gstate_cl) {
  static bool indexes_created = false;

  int rc = 0;
  bson::BSONObj cl_options, key_options, index_ref;
  char err_buf[HA_BUF_LEN] = {0};

  rc = sdb_conn.get_cl((char *)group_name, HA_GLOBAL_STATE_CL, gstate_cl);
  if (SDB_DMS_NOTEXIST == get_sdb_code(rc)) {
    sql_print_information(
        "HA: Global state table '%s' does not exist, create it",
        HA_GLOBAL_STATE_CL);
    cl_options = BSON(SDB_FIELD_NAME_AUTOINCREMENT
                      << BSON(SDB_FIELD_NAME_FIELD
                              << HA_FIELD_JOIN_ID << SDB_FIELD_ACQUIRE_SIZE << 1
                              << SDB_FIELD_CACHE_SIZE << 1));
    rc = sdb_conn.create_cl((char *)group_name, HA_GLOBAL_STATE_CL, cl_options);
    rc = (SDB_DMS_EXIST == get_sdb_code(rc)) ? 0 : rc;
    HA_RC_CHECK(
        rc, error,
        "HA: Unable to create global state table '%s', sequoiadb error: %s",
        HA_GLOBAL_STATE_CL, ha_error_string(sdb_conn, rc, err_buf));
  }

  if (!indexes_created) {
    // create index on 'InstanceID'
    index_ref = BSON(HA_FIELD_INSTANCE_ID << 1);
    key_options = BSON(SDB_FIELD_UNIQUE << 1);
    rc = sdb_conn.get_cl((char *)group_name, HA_GLOBAL_STATE_CL, gstate_cl);
    HA_RC_CHECK(
        rc, error,
        "HA: Unable to get global state table '%s', sequoiadb error: %s",
        HA_GLOBAL_STATE_CL, ha_error_string(sdb_conn, rc, err_buf));
    rc = gstate_cl.create_index(index_ref, HA_GLOB_STATE_INSTID_INDEX,
                                key_options);
    rc = (SDB_IXM_REDEF == get_sdb_code(rc)) ? 0 : rc;
    HA_RC_CHECK(rc, error,
                "HA: Unable to create index for '%s', sequoiadb error: %s",
                HA_GLOBAL_STATE_CL, ha_error_string(sdb_conn, rc, err_buf));

    // create index on "JoinID"
    index_ref = BSON(HA_FIELD_JOIN_ID << 1);
    key_options = BSON(SDB_FIELD_UNIQUE << 1);
    rc = gstate_cl.create_index(index_ref, HA_GLOB_STATE_JOIN_ID_INDEX,
                                key_options);
    rc = (SDB_IXM_REDEF == get_sdb_code(rc)) ? 0 : rc;
    HA_RC_CHECK(rc, error,
                "HA: Unable to create index for '%s', sequoiadb error: %s",
                HA_GLOBAL_STATE_CL, ha_error_string(sdb_conn, rc, err_buf));
    indexes_created = true;
  }
done:
  return rc;
error:
  goto done;
}

int ha_get_sql_log_state_cl(Sdb_conn &sdb_conn, const char *group_name,
                            Sdb_cl &log_state_cl) {
  static bool indexes_created = false;

  int rc = 0;
  char err_buf[HA_BUF_LEN] = {0};
  bson::BSONObj cl_options, index_ref, key_options;

  rc = sdb_conn.get_cl((char *)group_name, HA_SQL_LOG_STATE_CL, log_state_cl);
  if (SDB_DMS_NOTEXIST == get_sdb_code(rc)) {
    sql_print_information(
        "HA: SQL log state table '%s' does not exist, create it",
        HA_SQL_LOG_STATE_CL);
    rc =
        sdb_conn.create_cl((char *)group_name, HA_SQL_LOG_STATE_CL, cl_options);
    rc = (SDB_DMS_EXIST == get_sdb_code(rc)) ? 0 : rc;
    HA_RC_CHECK(
        rc, error,
        "HA: Unable to create SQL log state table '%s', sequoiadb error: %s",
        HA_SQL_LOG_STATE_CL, ha_error_string(sdb_conn, rc, err_buf));
  }

  if (!indexes_created) {
    rc = sdb_conn.get_cl((char *)group_name, HA_SQL_LOG_STATE_CL, log_state_cl);
    HA_RC_CHECK(
        rc, error,
        "HA: Unable to get SQL log state table '%s', sequoiadb error: %s",
        HA_SQL_LOG_STATE_CL, ha_error_string(sdb_conn, rc, err_buf));
    index_ref =
        BSON(HA_FIELD_DB << 1 << HA_FIELD_TABLE << 1 << HA_FIELD_TYPE << 1);
    key_options = BSON(SDB_FIELD_UNIQUE << 1);
    rc = log_state_cl.create_index(
        index_ref, HA_SQL_LOG_STATE_DB_TABLE_TYPE_INDEX, key_options);
    rc = (SDB_IXM_REDEF == get_sdb_code(rc)) ? 0 : rc;
    HA_RC_CHECK(rc, error,
                "HA: Unable to create index for '%s', sequoiadb error: %s",
                HA_SQL_LOG_STATE_CL, ha_error_string(sdb_conn, rc, err_buf));
    indexes_created = true;
  }
done:
  return rc;
error:
  goto done;
}

int ha_get_lock_cl(Sdb_conn &sdb_conn, const char *group_name,
                   Sdb_cl &lock_cl) {
  static bool indexes_created = false;

  int rc = 0;
  char err_buf[HA_BUF_LEN] = {0};
  bson::BSONObj cl_options, index_ref, key_options;

  rc = sdb_conn.get_cl((char *)group_name, HA_LOCK_CL, lock_cl);
  if (SDB_DMS_NOTEXIST == get_sdb_code(rc)) {
    sql_print_information("HA: Lock table '%s' does not exist, create it",
                          HA_LOCK_CL);
    rc = sdb_conn.create_cl((char *)group_name, HA_LOCK_CL, cl_options);
    rc = (SDB_DMS_EXIST == get_sdb_code(rc)) ? 0 : rc;
    HA_RC_CHECK(rc, error,
                "HA: Unable to create lock table '%s', sequoiadb error: %s",
                HA_LOCK_CL, ha_error_string(sdb_conn, rc, err_buf));
  }

  if (!indexes_created) {
    rc = sdb_conn.get_cl((char *)group_name, HA_LOCK_CL, lock_cl);
    HA_RC_CHECK(rc, error,
                "HA: Unable to get lock table '%s', sequoiadb error: %s",
                HA_LOCK_CL, ha_error_string(sdb_conn, rc, err_buf));

    index_ref =
        BSON(HA_FIELD_DB << 1 << HA_FIELD_TABLE << 1 << HA_FIELD_TYPE << 1);
    key_options = BSON(SDB_FIELD_UNIQUE << 1);
    rc = lock_cl.create_index(index_ref, HA_LOCK_DB_TABLE_TYPE_INDEX,
                              key_options);
    rc = (SDB_IXM_REDEF == get_sdb_code(rc)) ? 0 : rc;
    HA_RC_CHECK(rc, error,
                "HA: Unable to create index for '%s', sequoiadb error: %s",
                HA_LOCK_CL, ha_error_string(sdb_conn, rc, err_buf));
    indexes_created = true;
  }
done:
  return rc;
error:
  goto done;
}

int ha_get_config_cl(Sdb_conn &sdb_conn, const char *group_name,
                     Sdb_cl &config_cl) {
  int rc = 0;
  char err_buf[HA_BUF_LEN] = {0};
  rc = sdb_conn.get_cl(HA_GLOBAL_INFO, HA_CONFIG_CL, config_cl);

  if (SDB_DMS_NOTEXIST == get_sdb_code(rc)) {
    sql_print_information(
        "HA: Global configuration table '%s:%s' doesn't exist, create it",
        HA_CONFIG_CL, HA_GLOBAL_INFO);

    bson::BSONObj cl_options, index_ref, key_options;
    cl_options = BSON(SDB_FIELD_NAME_AUTOINCREMENT
                      << BSON(SDB_FIELD_NAME_FIELD
                              << HA_FIELD_INSTANCE_ID << SDB_FIELD_ACQUIRE_SIZE
                              << 1 << SDB_FIELD_CACHE_SIZE << 1));
    rc = sdb_conn.create_cl(HA_GLOBAL_INFO, HA_CONFIG_CL, cl_options);
    rc = (SDB_DMS_EXIST == get_sdb_code(rc)) ? 0 : rc;
    HA_RC_CHECK(rc, error,
                "HA: Unable to create global configuration table '%s', "
                "sequoiadb error: %s",
                HA_CONFIG_CL, ha_error_string(sdb_conn, rc, err_buf));

    index_ref = BSON(HA_FIELD_INSTANCE_ID << 1);
    key_options = BSON(SDB_FIELD_UNIQUE << 1);
    rc = sdb_conn.get_cl(HA_GLOBAL_INFO, HA_CONFIG_CL, config_cl);
    HA_RC_CHECK(rc, error,
                "HA: Unable to get global configuration table '%s', "
                "sequoiadb error: %s",
                HA_CONFIG_CL, ha_error_string(sdb_conn, rc, err_buf));

    rc = config_cl.create_index(index_ref, HA_CONFIG_INSTID_INDEX, key_options);
    rc = (SDB_IXM_REDEF == get_sdb_code(rc)) ? 0 : rc;
    HA_RC_CHECK(rc, error,
                "HA: Unable to create index on '%s', sequoiadb error: %s",
                HA_CONFIG_CL, ha_error_string(sdb_conn, rc, err_buf));

    index_ref = BSON(HA_FIELD_HOST_NAME << 1 << HA_FIELD_PORT << 1);
    key_options = BSON(SDB_FIELD_UNIQUE << 1 << SDB_FIELD_NOT_NULL << 1);
    rc = config_cl.create_index(index_ref, HA_CONFIG_HOST_PORT_INDEX,
                                key_options);
    rc = (SDB_IXM_REDEF == get_sdb_code(rc)) ? 0 : rc;
    HA_RC_CHECK(rc, error,
                "HA: Unable to create index on '%s', sequoiadb error: %s",
                HA_CONFIG_CL, ha_error_string(sdb_conn, rc, err_buf));
  }
done:
  return rc;
error:
  goto done;
}
