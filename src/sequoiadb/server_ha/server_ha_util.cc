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

   Source File Name = server_ha_util.cc

   Descriptive Name = N/A

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          18/08/2025  SequoiaDB  Initial Draft

   Last Changed =

*******************************************************************************/
#include "server_ha_util.h"
#include "ha_sdb_errcode.h"
#include "ha_sdb_log.h"
#include "base64.h"
#define MY_AES_CBC my_aes_128_cbc

// get Sdb_conn error string
const char *ha_error_string(Sdb_conn &sdb_conn, int rc, char err_buf[]) {
  memset(err_buf, '0', HA_BUF_LEN);
  if (rc > SDB_ERR_INNER_CODE_BEGIN && rc < SDB_ERR_INNER_CODE_END) {
    bson::BSONObj result;
    if (sdb_conn.get_last_error(result)) {
      sprintf(err_buf, "failed to get sequoiadb error, error code is: %d", rc);
    } else {
      const char *err = result.getStringField(SDB_FIELD_DETAIL);
      if ('\0' == err[0]) {
        err = result.getStringField(SDB_FIELD_DESCRIPTION);
      }
      if ('\0' == err[0]) {
        sprintf(err_buf, "failed to get sequoiadb error, error code is: %d",
                rc);
      } else {
        snprintf(err_buf, HA_BUF_LEN, "%s", err);
      }
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

static int create_index_if_not_exists(Sdb_cl &cl, const char *index_name,
                                      const std::vector<std::string> &idx_elems,
                                      bool unique_index = TRUE,
                                      bool not_null = FALSE) {
  int rc = SDB_ERR_OK;
  char err_buf[HA_BUF_LEN] = {0};
  Sdb_conn *conn = cl.get_conn();
  assert(conn && index_name);
  bson::BSONObjBuilder index_ref_builder, key_options_builder;
  bson::BSONObj index_ref, key_options, index_info;

  rc = cl.get_index(index_name, index_info);
  if (0 == rc) {
    // Index already exist, no need to create it again
    goto done;
  } else if (SDB_IXM_NOTEXIST == get_sdb_code(rc)) {
    // Reset error if index does not exist, prepare create index
    rc = 0;
  }
  HA_RC_CHECK(
      rc, error, "Failed to get index '%s' on '%s', sequoiadb error: %s",
      index_name, cl.get_cl_name(), ha_error_string(*conn, rc, err_buf));

  // Prepare create index
  try {
    // Build index definition
    for (size_t i = 0; i < idx_elems.size(); i++) {
      index_ref_builder.append(idx_elems[i], 1);
    }
    index_ref = index_ref_builder.done();

    // Build index options
    if (unique_index) {
      key_options_builder.append(SDB_FIELD_UNIQUE, unique_index);
    }
    if (not_null) {
      key_options_builder.append(SDB_FIELD_NOT_NULL, not_null);
    }
    key_options = key_options_builder.done();
  }
  SDB_EXCEPTION_CATCHER(
      rc, "Failed to build index '%s' definition and options, exception:%s",
      index_name, e.what());

  // Create index
  rc = cl.create_index(index_ref, index_name, key_options);
  if (SDB_IXM_CREATING == get_sdb_code(rc) ||
      SDB_IXM_REDEF == get_sdb_code(rc)) {
    // Creating index or creating index task already exists, ignore this error
    rc = 0;
    goto done;
  }
  HA_RC_CHECK(rc, error, "HA: Failed to create index '%s', sequoiadb error: %s",
              index_name, ha_error_string(*conn, rc, err_buf));
done:
  return rc;
error:
  goto done; /* purecov: inspected */
}

int ha_get_instance_object_state_cl(Sdb_conn &sdb_conn, const char *group_name,
                                    Sdb_cl &cl, const char *data_group) {
  static bool indexes_created = false;

  int rc = 0;
  char err_buf[HA_BUF_LEN] = {0};
  bson::BSONObj cl_options;

  try {
    if (NULL != data_group && '\0' != data_group[0]) {
      cl_options = BSON(SDB_FIELD_GROUP << data_group);
    }

    rc = sdb_conn.get_cl((char *)group_name, HA_INSTANCE_OBJECT_STATE_CL, cl,
                         true);
    if (SDB_DMS_NOTEXIST == get_sdb_code(rc)) {
      sql_print_information("HA: Creating instance object state table '%s'",
                            HA_INSTANCE_OBJECT_STATE_CL);
      rc = sdb_conn.create_cl((char *)group_name, HA_INSTANCE_OBJECT_STATE_CL,
                              cl_options);
      rc = (SDB_DMS_EXIST == get_sdb_code(rc)) ? 0 : rc;
      HA_RC_CHECK(rc, error,
                  "HA: Unable to create instance object state table '%s', "
                  "sequoiadb error: %s",
                  HA_INSTANCE_OBJECT_STATE_CL,
                  ha_error_string(sdb_conn, rc, err_buf));

      rc = sdb_conn.get_cl((char *)group_name, HA_INSTANCE_OBJECT_STATE_CL, cl);
      HA_RC_CHECK(
          rc, error, "HA: Unable to get table '%s', sequoiadb error: %s",
          HA_INSTANCE_OBJECT_STATE_CL, ha_error_string(sdb_conn, rc, err_buf));
    }

    if (!indexes_created) {
      std::vector<std::string> idx_elems;
      idx_elems.push_back(HA_FIELD_DB);
      idx_elems.push_back(HA_FIELD_TABLE);
      idx_elems.push_back(HA_FIELD_TYPE);
      idx_elems.push_back(HA_FIELD_INSTANCE_ID);
      rc = create_index_if_not_exists(
          cl, HA_INST_OBJ_STATE_DB_TABLE_TYPE_INSTID_INDEX, idx_elems, true);
      HA_RC_CHECK(rc, error, "Failed to create index on '%s'",
                  cl.get_cl_name());
      indexes_created = true;
    }
  }
  SDB_EXCEPTION_CATCHER(rc, "Failed to get '%s' instance, exception:%s",
                        HA_INSTANCE_OBJECT_STATE_CL, e.what());
done:
  return rc;
error:
  goto done;
}

int ha_get_instance_state_cl(Sdb_conn &sdb_conn, const char *group_name,
                             Sdb_cl &cl, const char *data_group) {
  static bool indexes_created = false;

  int rc = 0;
  bson::BSONObj cl_options;
  char err_buf[HA_BUF_LEN] = {0};

  try {
    if (NULL != data_group && '\0' != data_group[0]) {
      cl_options = BSON(SDB_FIELD_GROUP << data_group);
    }

    rc = sdb_conn.get_cl((char *)group_name, HA_INSTANCE_STATE_CL, cl, true);
    if (SDB_DMS_NOTEXIST == get_sdb_code(rc)) {
      sql_print_information("HA: Creating instance state table '%s'",
                            HA_INSTANCE_STATE_CL);
      bson::BSONObjBuilder builder;
      bson::BSONObjBuilder sub_builder(
          builder.subobjStart(SDB_FIELD_NAME_AUTOINCREMENT));

      sub_builder.append(SDB_FIELD_NAME_FIELD, HA_FIELD_JOIN_ID);
      sub_builder.append(SDB_FIELD_ACQUIRE_SIZE, 1);
      sub_builder.append(SDB_FIELD_CACHE_SIZE, 1);
      sub_builder.doneFast();
      if (NULL != data_group && '\0' != data_group[0]) {
        builder.append(SDB_FIELD_GROUP, data_group);
      }
      cl_options = builder.done();
      rc = sdb_conn.create_cl((char *)group_name, HA_INSTANCE_STATE_CL,
                              cl_options);
      rc = (SDB_DMS_EXIST == get_sdb_code(rc)) ? 0 : rc;
      HA_RC_CHECK(
          rc, error,
          "HA: Unable to create instance state table '%s', sequoiadb error: %s",
          HA_INSTANCE_STATE_CL, ha_error_string(sdb_conn, rc, err_buf));

      rc = sdb_conn.get_cl((char *)group_name, HA_INSTANCE_STATE_CL, cl);
      HA_RC_CHECK(
          rc, error,
          "HA: Unable to get instance state table '%s', sequoiadb error: %s",
          HA_INSTANCE_STATE_CL, ha_error_string(sdb_conn, rc, err_buf));
    }

    if (!indexes_created) {
      // create index on 'InstanceID'
      std::vector<std::string> idx_elems;
      idx_elems.push_back(HA_FIELD_INSTANCE_ID);
      rc = create_index_if_not_exists(cl, HA_INST_STATE_INSTID_INDEX, idx_elems,
                                      true);
      HA_RC_CHECK(rc, error, "Failed to create index on '%s'",
                  cl.get_cl_name());

      idx_elems.clear();
      idx_elems.push_back(HA_FIELD_JOIN_ID);
      rc = create_index_if_not_exists(cl, HA_INST_STATE_JOIN_ID_INDEX,
                                      idx_elems, true);
      HA_RC_CHECK(rc, error, "Failed to create index on '%s'",
                  cl.get_cl_name());
      indexes_created = true;
    }
  }
  SDB_EXCEPTION_CATCHER(rc, "Failed to get '%s' instance, exception:%s",
                        HA_INSTANCE_STATE_CL, e.what());
done:
  return rc;
error:
  goto done;
}

int ha_get_object_state_cl(Sdb_conn &sdb_conn, const char *group_name,
                           Sdb_cl &cl, const char *data_group) {
  static bool indexes_created = false;

  int rc = 0;
  char err_buf[HA_BUF_LEN] = {0};
  bson::BSONObj cl_options;
  try {
    if (NULL != data_group && '\0' != data_group[0]) {
      cl_options = BSON(SDB_FIELD_GROUP << data_group);
    }

    rc = sdb_conn.get_cl((char *)group_name, HA_OBJECT_STATE_CL, cl, true);
    if (SDB_DMS_NOTEXIST == get_sdb_code(rc)) {
      sql_print_information("HA: Creating object state table '%s'",
                            HA_OBJECT_STATE_CL);
      rc = sdb_conn.create_cl((char *)group_name, HA_OBJECT_STATE_CL,
                              cl_options);
      rc = (SDB_DMS_EXIST == get_sdb_code(rc)) ? 0 : rc;
      HA_RC_CHECK(
          rc, error,
          "HA: Unable to create object state table '%s', sequoiadb error: %s",
          HA_OBJECT_STATE_CL, ha_error_string(sdb_conn, rc, err_buf));

      rc = sdb_conn.get_cl((char *)group_name, HA_OBJECT_STATE_CL, cl);
      HA_RC_CHECK(
          rc, error,
          "HA: Unable to get object state table '%s', sequoiadb error: %s",
          HA_OBJECT_STATE_CL, ha_error_string(sdb_conn, rc, err_buf));
    }

    if (!indexes_created) {
      std::vector<std::string> idx_elems;
      idx_elems.push_back(HA_FIELD_DB);
      idx_elems.push_back(HA_FIELD_TABLE);
      idx_elems.push_back(HA_FIELD_TYPE);
      rc = create_index_if_not_exists(cl, HA_OBJ_STATE_DB_TABLE_TYPE_INDEX,
                                      idx_elems, true);
      HA_RC_CHECK(rc, error, "Failed to create index on '%s'",
                  cl.get_cl_name());

      idx_elems.clear();
      idx_elems.push_back(HA_FIELD_DB);
      idx_elems.push_back(HA_FIELD_SQL_ID);
      rc = create_index_if_not_exists(cl, HA_OBJ_STATE_DB_SQLID_INDEX,
                                      idx_elems, true);
      HA_RC_CHECK(rc, error, "Failed to create index on '%s'",
                  cl.get_cl_name());
      indexes_created = true;
    }
  }
  SDB_EXCEPTION_CATCHER(rc, "Failed to get '%s' instance, exception:%s",
                        HA_OBJECT_STATE_CL, e.what());
done:
  return rc;
error:
  goto done;
}

int ha_get_lock_cl(Sdb_conn &sdb_conn, const char *group_name, Sdb_cl &lock_cl,
                   const char *data_group) {
  static bool indexes_created = false;

  int rc = 0;
  char err_buf[HA_BUF_LEN] = {0};
  bson::BSONObj cl_options;
  try {
    if (NULL != data_group && '\0' != data_group[0]) {
      cl_options = BSON(SDB_FIELD_GROUP << data_group);
    }

    rc = sdb_conn.get_cl((char *)group_name, HA_LOCK_CL, lock_cl, true);
    if (SDB_DMS_NOTEXIST == get_sdb_code(rc)) {
      sql_print_information("HA: Creating lock table '%s'", HA_LOCK_CL);
      rc = sdb_conn.create_cl((char *)group_name, HA_LOCK_CL, cl_options);
      rc = (SDB_DMS_EXIST == get_sdb_code(rc)) ? 0 : rc;
      HA_RC_CHECK(rc, error,
                  "HA: Unable to create lock table '%s', sequoiadb error: %s",
                  HA_LOCK_CL, ha_error_string(sdb_conn, rc, err_buf));

      rc = sdb_conn.get_cl((char *)group_name, HA_LOCK_CL, lock_cl);
      HA_RC_CHECK(rc, error,
                  "HA: Unable to get lock table '%s', sequoiadb error: %s",
                  HA_LOCK_CL, ha_error_string(sdb_conn, rc, err_buf));
    }

    if (!indexes_created) {
      std::vector<std::string> idx_elems;
      idx_elems.push_back(HA_FIELD_DB);
      idx_elems.push_back(HA_FIELD_TABLE);
      idx_elems.push_back(HA_FIELD_TYPE);
      rc = create_index_if_not_exists(lock_cl, HA_LOCK_DB_TABLE_TYPE_INDEX,
                                      idx_elems, true);
      HA_RC_CHECK(rc, error, "Failed to create index on '%s'",
                  lock_cl.get_cl_name());
      indexes_created = true;
    }
  }
  SDB_EXCEPTION_CATCHER(rc, "Failed to get '%s' instance, exception:%s",
                        HA_LOCK_CL, e.what());
done:
  return rc;
error:
  goto done;
}

int ha_get_registry_cl(Sdb_conn &sdb_conn, const char *group_name,
                       Sdb_cl &registry_cl, const char *data_group) {
  static bool indexes_created = false;
  int rc = 0;
  char err_buf[HA_BUF_LEN] = {0};
  rc = sdb_conn.get_cl(HA_GLOBAL_INFO, HA_REGISTRY_CL, registry_cl, true);
  try {
    if (SDB_DMS_NOTEXIST == get_sdb_code(rc)) {
      sql_print_information("HA: Creating registry table '%s:%s'",
                            HA_GLOBAL_INFO, HA_REGISTRY_CL);
      bson::BSONObj cl_options;
      bson::BSONObjBuilder builder;
      bson::BSONObjBuilder sub_builder(
          builder.subobjStart(SDB_FIELD_NAME_AUTOINCREMENT));

      sub_builder.append(SDB_FIELD_NAME_FIELD, HA_FIELD_INSTANCE_ID);
      sub_builder.append(SDB_FIELD_ACQUIRE_SIZE, 1);
      sub_builder.append(SDB_FIELD_CACHE_SIZE, 1);
      sub_builder.doneFast();
      if (NULL != data_group && '\0' != data_group[0]) {
        builder.append(SDB_FIELD_GROUP, data_group);
      }
      cl_options = builder.done();
      rc = sdb_conn.create_cl(HA_GLOBAL_INFO, HA_REGISTRY_CL, cl_options);
      rc = (SDB_DMS_EXIST == get_sdb_code(rc)) ? 0 : rc;
      HA_RC_CHECK(
          rc, error,
          "HA: Unable to create registry table '%s', sequoiadb error: %s",
          HA_REGISTRY_CL, ha_error_string(sdb_conn, rc, err_buf));

      rc = sdb_conn.get_cl(HA_GLOBAL_INFO, HA_REGISTRY_CL, registry_cl, true);
      HA_RC_CHECK(rc, error,
                  "HA: Unable to get table '%s', sequoiadb error: %s",
                  HA_REGISTRY_CL, ha_error_string(sdb_conn, rc, err_buf));
    }

    if (!indexes_created) {
      std::vector<std::string> idx_elems;
      idx_elems.push_back(HA_FIELD_INSTANCE_ID);
      rc = create_index_if_not_exists(registry_cl, HA_REGISTRY_INSTID_INDEX,
                                      idx_elems, true);
      HA_RC_CHECK(rc, error, "Failed to create index on '%s'",
                  registry_cl.get_cl_name());

      idx_elems.clear();
      idx_elems.push_back(HA_FIELD_HOST_NAME);
      idx_elems.push_back(HA_FIELD_PORT);
      rc = create_index_if_not_exists(registry_cl, HA_REGISTRY_HOST_PORT_INDEX,
                                      idx_elems, true, true);
      HA_RC_CHECK(rc, error, "Failed to create index on '%s'",
                  registry_cl.get_cl_name());
      indexes_created = true;
    }
  }
  SDB_EXCEPTION_CATCHER(rc, "Failed to get '%s' instance, exeception:%s",
                        HA_REGISTRY_CL, e.what());
done:
  return rc;
error:
  goto done;
}

int ha_get_pending_log_cl(Sdb_conn &sdb_conn, const char *group_name,
                          Sdb_cl &pending_log_cl, const char *data_group) {
  static bool indexes_created = false;

  int rc = 0;
  char err_buf[HA_BUF_LEN] = {0};

  rc = sdb_conn.get_cl((char *)group_name, HA_PENDING_LOG_CL, pending_log_cl,
                       true);
  try {
    if (SDB_DMS_NOTEXIST == get_sdb_code(rc)) {
      sql_print_information("HA: Creating '%s:%s'", group_name,
                            HA_PENDING_LOG_CL);

      bson::BSONObj cl_options;
      bson::BSONObjBuilder builder;
      bson::BSONObjBuilder sub_builder(
          builder.subobjStart(SDB_FIELD_NAME_AUTOINCREMENT));

      sub_builder.append(SDB_FIELD_NAME_FIELD, HA_FIELD_SQL_ID);
      sub_builder.append(SDB_FIELD_ACQUIRE_SIZE, 1);
      sub_builder.append(SDB_FIELD_CACHE_SIZE, 1);
      sub_builder.doneFast();
      if (NULL != data_group && '\0' != data_group[0]) {
        builder.append(SDB_FIELD_GROUP, data_group);
      }
      cl_options = builder.done();
      rc =
          sdb_conn.create_cl((char *)group_name, HA_PENDING_LOG_CL, cl_options);
      rc = (SDB_DMS_EXIST == get_sdb_code(rc)) ? 0 : rc;
      HA_RC_CHECK(rc, error, "HA: Unable to create '%s', sequoiadb error: %s",
                  HA_PENDING_LOG_CL, ha_error_string(sdb_conn, rc, err_buf));

      rc = sdb_conn.get_cl((char *)group_name, HA_PENDING_LOG_CL,
                           pending_log_cl);
      HA_RC_CHECK(rc, error, "HA: Unable to get '%s', sequoiadb error: %s",
                  HA_PENDING_LOG_CL, ha_error_string(sdb_conn, rc, err_buf));
    }

    if (!indexes_created) {
      std::vector<std::string> idx_elems;
      idx_elems.push_back(HA_FIELD_SQL_ID);
      rc = create_index_if_not_exists(
          pending_log_cl, HA_PENDING_LOG_PENDING_ID_INDEX, idx_elems, true);
      HA_RC_CHECK(rc, error, "Failed to create index on '%s'",
                  pending_log_cl.get_cl_name());
      indexes_created = true;
    }
  }
  SDB_EXCEPTION_CATCHER(rc, "Failed to get '%s' instance, exception:%s",
                        HA_PENDING_LOG_CL, e.what());
done:
  return rc;
error:
  goto done;
}

int ha_get_pending_object_cl(Sdb_conn &sdb_conn, const char *group_name,
                             Sdb_cl &pending_object_cl,
                             const char *data_group) {
  static bool indexes_created = false;

  int rc = 0;
  char err_buf[HA_BUF_LEN] = {0};

  rc = sdb_conn.get_cl((char *)group_name, HA_PENDING_OBJECT_CL,
                       pending_object_cl, true);
  try {
    if (SDB_DMS_NOTEXIST == get_sdb_code(rc)) {
      sql_print_information("HA: Creating '%s:%s'", group_name,
                            HA_PENDING_OBJECT_CL);
      bson::BSONObj cl_options;
      if (NULL != data_group && '\0' != data_group[0]) {
        cl_options = BSON(SDB_FIELD_GROUP << data_group);
      }
      rc = sdb_conn.create_cl((char *)group_name, HA_PENDING_OBJECT_CL,
                              cl_options);
      rc = (SDB_DMS_EXIST == get_sdb_code(rc)) ? 0 : rc;
      HA_RC_CHECK(rc, error, "HA: Unable to create '%s', sequoiadb error: %s",
                  HA_PENDING_OBJECT_CL, ha_error_string(sdb_conn, rc, err_buf));

      rc = sdb_conn.get_cl((char *)group_name, HA_PENDING_OBJECT_CL,
                           pending_object_cl);
      HA_RC_CHECK(rc, error, "HA: Unable to get '%s', sequoiadb error: %s",
                  HA_PENDING_OBJECT_CL, ha_error_string(sdb_conn, rc, err_buf));
    }

    if (!indexes_created) {
      std::vector<std::string> idx_elems;
      idx_elems.push_back(HA_FIELD_DB);
      idx_elems.push_back(HA_FIELD_TABLE);
      idx_elems.push_back(HA_FIELD_TYPE);
      rc = create_index_if_not_exists(pending_object_cl,
                                      HA_PENDING_OBJECT_DB_TABLE_TYPE_INDEX,
                                      idx_elems, true);
      HA_RC_CHECK(rc, error, "Failed to create index on '%s'",
                  pending_object_cl.get_cl_name());

      idx_elems.clear();
      idx_elems.push_back(HA_FIELD_SQL_ID);
      rc = create_index_if_not_exists(
          pending_object_cl, HA_PENDING_OBJECT_SQLID_INDEX, idx_elems, false);
      HA_RC_CHECK(rc, error, "Failed to create index on '%s'",
                  pending_object_cl.get_cl_name());
      indexes_created = true;
    }
  }
  SDB_EXCEPTION_CATCHER(rc, "Failed to get '%s' instance, exception:%s",
                        HA_PENDING_OBJECT_CL, e.what());
done:
  return rc;
error:
  goto done;
}

int ha_get_table_stats_cl(Sdb_conn &sdb_conn, const char *group_name,
                          Sdb_cl &table_stats_cl, const char *data_group,
                          bool autoCreate) {
  static bool indexes_created = false;

  int rc = 0;
  char err_buf[HA_BUF_LEN] = {0};
  Sdb_pool_conn pool_conn(0, true);
  Sdb_conn &tmp_conn = pool_conn;

  rc = sdb_conn.get_cl((char *)group_name, HA_TABLE_STATS_CL, table_stats_cl,
                       true);
  try {
    if (SDB_DMS_NOTEXIST == get_sdb_code(rc) && autoCreate) {
      sql_print_information("HA: Creating '%s:%s'", group_name,
                            HA_TABLE_STATS_CL);
      bson::BSONObjBuilder builder;
      bson::BSONObj cl_options;
      if (NULL != data_group && '\0' != data_group[0]) {
        builder.append(SDB_FIELD_GROUP, data_group);
      }
      builder.append(SDB_CL_ATTR_NO_TRANS, true);
      cl_options = builder.obj();

      // Use a new connection to create table to prevent the affect of
      // transaction of original connection.
      rc = tmp_conn.connect();
      if (rc) {
        goto error;
      }
      rc =
          tmp_conn.create_cl((char *)group_name, HA_TABLE_STATS_CL, cl_options);
      rc = (SDB_DMS_EXIST == get_sdb_code(rc)) ? 0 : rc;
      HA_RC_CHECK(rc, error, "HA: Unable to create '%s', sequoiadb error: %s",
                  HA_TABLE_STATS_CL, ha_error_string(sdb_conn, rc, err_buf));

      rc = sdb_conn.get_cl((char *)group_name, HA_TABLE_STATS_CL,
                           table_stats_cl);
      HA_RC_CHECK(rc, error, "HA: Unable to get '%s', sequoiadb error: %s",
                  HA_TABLE_STATS_CL, ha_error_string(sdb_conn, rc, err_buf));
    }

    if (!indexes_created && autoCreate) {
      std::vector<std::string> idx_elems;
      idx_elems.push_back(SDB_FIELD_NAME);
      idx_elems.push_back(SDB_FIELD_DETAILS "." SDB_FIELD_GROUP_NAME);
      rc = create_index_if_not_exists(
          table_stats_cl, HA_TABLE_STATS_CL_NAME_INDEX, idx_elems, true);
      HA_RC_CHECK(rc, error, "Failed to create index on '%s'",
                  table_stats_cl.get_cl_name());
      indexes_created = true;
    }
  }
  SDB_EXCEPTION_CATCHER(rc, "Failed to get '%s' instance, exception:%s",
                        HA_TABLE_STATS_CL, e.what());
done:
  return rc;
error:
  goto done;
}

int ha_get_index_stats_cl(Sdb_conn &sdb_conn, const char *group_name,
                          Sdb_cl &index_stats_cl, const char *data_group,
                          bool autoCreate) {
  static bool indexes_created = false;

  int rc = 0;
  char err_buf[HA_BUF_LEN] = {0};
  Sdb_pool_conn pool_conn(0, true);
  Sdb_conn &tmp_conn = pool_conn;

  rc = sdb_conn.get_cl((char *)group_name, HA_INDEX_STATS_CL, index_stats_cl,
                       true);
  try {
    if (SDB_DMS_NOTEXIST == get_sdb_code(rc) && autoCreate) {
      sql_print_information("HA: Creating '%s:%s'", group_name,
                            HA_INDEX_STATS_CL);
      bson::BSONObjBuilder builder;
      bson::BSONObj cl_options;
      if (NULL != data_group && '\0' != data_group[0]) {
        builder.append(SDB_FIELD_GROUP, data_group);
      }
      builder.append(SDB_CL_ATTR_NO_TRANS, true);
      cl_options = builder.obj();

      // Use a new connection to create table to prevent the affect of
      // transaction of original connection.
      rc = tmp_conn.connect();
      if (rc) {
        goto error;
      }
      rc =
          tmp_conn.create_cl((char *)group_name, HA_INDEX_STATS_CL, cl_options);
      rc = (SDB_DMS_EXIST == get_sdb_code(rc)) ? 0 : rc;
      HA_RC_CHECK(rc, error, "HA: Unable to create '%s', sequoiadb error: %s",
                  HA_INDEX_STATS_CL, ha_error_string(sdb_conn, rc, err_buf));

      rc = sdb_conn.get_cl((char *)group_name, HA_INDEX_STATS_CL,
                           index_stats_cl);
      HA_RC_CHECK(rc, error, "HA: Unable to get '%s', sequoiadb error: %s",
                  HA_INDEX_STATS_CL, ha_error_string(sdb_conn, rc, err_buf));
    }

    if (!indexes_created && autoCreate) {
      std::vector<std::string> idx_elems;
      idx_elems.push_back(SDB_FIELD_COLLECTION);
      idx_elems.push_back(SDB_FIELD_INDEX);
      rc = create_index_if_not_exists(
          index_stats_cl, HA_INDEX_STATS_CL_COLLECTION_INDEX, idx_elems, true);
      HA_RC_CHECK(rc, error, "Failed to create index on '%s'",
                  index_stats_cl.get_cl_name());
      indexes_created = true;
    }
  }
  SDB_EXCEPTION_CATCHER(rc, "Failed to get '%s' instance, exception:%s",
                        HA_INDEX_STATS_CL, e.what());
done:
  return rc;
error:
  goto done;
}

void ha_oid_to_time_str(bson::OID &oid, char *str_buf, const uint buf_len) {
  time_t timestamp = oid.asTimeT();
  struct tm time;
  localtime_r(&timestamp, &time);
  strftime(str_buf, buf_len, "%Y-%m-%d-%H.%M.%S", &time);
}

// decrypt encrypted password from 'HAUser' table
int ha_decrypt_inst_group_password(const char *base64_cipher,
                                   const char *sdb_md5_password,
                                   const char *key, const char *iv,
                                   char *passwd) {
  String src, dst, md5_key, md5_iv, md5_password, md5_hex_str;
  int rc = 0;
  bool oom = false;

  // decode base64 cipher to src
  int len = base64_needed_decoded_length(strlen(base64_cipher));
  oom = src.alloc(len);
  HA_RC_CHECK(oom, error,
              "HA: Out of memory while decrypting instance group password");

  len = base64_decode(base64_cipher, strlen(base64_cipher), src.c_ptr_quick(),
                      0, 0);
  HA_RC_CHECK(len < 0, error,
              "HA: Failed to decode 'CipherPassword' in base64 format, "
              "please use 'IV' and instance group key to check "
              "if 'CipherPassword' in base64 format is correct");
  src.length(len);

  // calculcate MD5 for instance group key and 'IV', prepare for decryption
  oom = md5_key.alloc(HA_MD5_BYTE_LEN);
  oom |= md5_iv.alloc(HA_MD5_BYTE_LEN);
  HA_RC_CHECK(oom, error,
              "HA: Out of memory while decrypting instance group password");

  md5_key.length(HA_MD5_BYTE_LEN);
  md5_iv.length(HA_MD5_BYTE_LEN);
#ifdef IS_MYSQL
  compute_md5_hash(md5_key.c_ptr_quick(), key, strlen(key));
  compute_md5_hash(md5_iv.c_ptr_quick(), iv, strlen(iv));
#else
  compute_md5_hash((uchar *)md5_key.c_ptr_quick(), key, strlen(key));
  compute_md5_hash((uchar *)md5_iv.c_ptr_quick(), iv, strlen(iv));
#endif

  rc = sdb_aes_decrypt(MY_AES_CBC, (uchar *)md5_key.c_ptr_quick(),
                       HA_MD5_BYTE_LEN, src, dst, (uchar *)md5_iv.c_ptr_quick(),
                       HA_MD5_BYTE_LEN);
  HA_RC_CHECK(rc, error, "HA: Decrypt aes cipher error: %d", rc);

  // check if password is correct
  oom = md5_password.alloc(HA_MD5_BYTE_LEN);
  oom |= md5_hex_str.alloc(HA_MD5_HEX_STR_LEN);
  HA_RC_CHECK(oom, error,
              "HA: Out of memory while decrypting instance group password");

  strcpy(passwd, dst.c_ptr_safe());
  md5_password.length(HA_MD5_BYTE_LEN);
  md5_hex_str.length(HA_MD5_HEX_STR_LEN);
#ifdef IS_MYSQL
  compute_md5_hash(md5_password.c_ptr_quick(), dst.c_ptr_quick(), dst.length());
#else
  compute_md5_hash((uchar *)md5_password.c_ptr_quick(), dst.c_ptr_quick(),
                   dst.length());
#endif
  array_to_hex(md5_hex_str.c_ptr_safe(), (uchar *)md5_password.c_ptr_safe(),
               HA_MD5_BYTE_LEN);
  rc = strcasecmp(sdb_md5_password, md5_hex_str.c_ptr_safe());
  HA_RC_CHECK(rc, error,
              "HA: Instance group password verification failed, "
              "please check if instance group key is correct.");
done:
  return rc;
error:
  rc = oom ? SDB_HA_OOM : SDB_HA_DECRYPT_PASSWORD;
  goto done;
}

// if 'bind_address' current instance is set and it's not '*'', set
// ip_addr to my_bind_addr_str, or get one of ip address(not include
// loopback address) from system
int ha_get_local_ip_address(char *ip_addr, int len) {
  int rc = 0;
  if (my_bind_addr_str && strcmp(my_bind_addr_str, "*") != 0) {
    strncpy(ip_addr, my_bind_addr_str, len);
    ip_addr[len - 1] = '\0';
  } else {
    struct ifaddrs *if_addr_struct = NULL, *ifa = NULL;
    void *addr_ptr = NULL;
    char addr_buf[INET_ADDRSTRLEN] = {0};
    char *first_available_ip = NULL;
    bool matched = FALSE;

    // get host IP address by hostname, from '/etc/hosts'
    struct hostent *hosts = gethostbyname(glob_hostname);

    // get host IP address by 'getifaddrs', on error, -1 is returned
    rc = getifaddrs(&if_addr_struct);
    HA_RC_CHECK(rc, error,
                "HA: System call 'getifaddrs()' error: %s, errno: %d",
                strerror(errno), errno);

    for (ifa = if_addr_struct; ifa != NULL && !matched; ifa = ifa->ifa_next) {
      // support IPV4 for now
      if (!ifa->ifa_addr || ifa->ifa_addr->sa_family != AF_INET) {
        continue;
      }
      addr_ptr = &((struct sockaddr_in *)ifa->ifa_addr)->sin_addr;
      DBUG_ASSERT(NULL != addr_ptr);
      const char *ip = inet_ntop(AF_INET, addr_ptr, addr_buf, INET_ADDRSTRLEN);
      if (NULL == ip) {
        sql_print_error("HA: System call 'inet_ntop' error: %s, errno: %d",
                        strerror(errno), errno);
        continue;
      } else if (strcmp(ip, HA_LOOPBACK_ADDRESS) == 0) {
        sql_print_information("HA: Skip loopback address");
        continue;
      }

      // handle available IP address now
      // check if current IP address is in hosts file
      for (int i = 0; hosts && hosts->h_addr_list[i]; i++) {
        if (hosts->h_addrtype != AF_INET) {
          continue;
        }

        const char *hosts_ip =
            inet_ntoa(*((struct in_addr *)hosts->h_addr_list[i]));
        if (NULL == hosts_ip) {
          sql_print_error("HA: System call 'inet_ntoa' error: %s, errno: %d",
                          strerror(errno), errno);
          continue;
        }

        if (strcmp(hosts_ip, ip) == 0) {
          strncpy(ip_addr, hosts_ip, len);
          ip_addr[len - 1] = '\0';
          matched = TRUE;
          break;
        }
      }

      if (NULL == hosts) {
        strncpy(ip_addr, ip, len);
        ip_addr[len - 1] = '\0';
        break;
      }

      // set 'ip_addr' in case it is not set
      if (NULL == first_available_ip && !matched) {
        first_available_ip = strncpy(ip_addr, ip, len);
        ip_addr[len - 1] = '\0';
      }
    }

    // no matched IP address in 'hosts' and network interface address
    if (!matched) {
      sql_print_warning(
          "HA: No matched IP addresses in 'hosts' file and network interface "
          "address, 'hosts' file may not be configured correctly");
    }

    rc = strlen(ip_addr) ? 0 : SDB_HA_GET_LOCAL_IP;
    freeifaddrs(if_addr_struct);
  }
done:
  return rc;
error:
  rc = SDB_HA_GET_LOCAL_IP;
  goto done;
}

