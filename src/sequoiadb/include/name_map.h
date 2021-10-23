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

#ifndef NAME_MAP__H
#define NAME_MAP__H
#include "sdb_conn.h"

enum enum_mapping_state {
  NM_STATE_NONE = 0,
  NM_STATE_CREATING,
  NM_STATE_CREATED
};

class MetadataMapping {
 private:
  static char m_sql_group[SDB_CS_NAME_MAX_SIZE + 1];
  static bool m_enabled;
  static int m_mapping_group_size;
  static int m_mapping_group_num;

  MetadataMapping() {}

 private:
  int calculate_mapping_slot(Sdb_conn *sdb_conn, const char *sql_group_cs_name,
                             const char *db_name, int &slot);

  int calculate_mapping_cs(Sdb_conn *sdb_conn, const char *db_name,
                           char *cs_name, const char *sql_group_name,
                           bool pefer_origin_name);

 public:
  static MetadataMapping *get_instance() {
    static MetadataMapping inst;
    return &inst;
  }

  static void enable_metadata_mapping(bool enable) { m_enabled = enable; }

  static void set_sql_group(const char *sql_group) {
    // set group name
    if (NULL != sql_group && '\0' != sql_group[0]) {
      char *dst_pos = m_sql_group;
      const char *src_pos = sql_group;
      while (*src_pos != '\0') {
        *dst_pos = toupper(*src_pos);
        ++src_pos;
        ++dst_pos;
      }
    } else {
      // set default group name
      sprintf(m_sql_group, "%s", NM_DEFAULT_GROUP_NAME);
    }
  }

  static const char *get_sql_group() { return m_sql_group; }

  static void set_mapping_group_size(int mapping_size) {
    m_mapping_group_size = mapping_size;
  }

  static void set_mapping_group_number(int mapping_num) {
    m_mapping_group_num = mapping_num;
  }

  int get_table_mapping(Sdb_conn *sdb_conn, const char *db_name,
                        const char *table_name, char *cs_name, char *cl_name,
                        bool is_temporary_table = false,
                        enum_mapping_state *state = NULL, int *cl_count = NULL);

  int add_table_mapping(Sdb_conn *sdb_conn, const char *db_name,
                        const char *table_name, char *cs_name, char *cl_name,
                        bool pefer_origin_name, bool is_tmp_table = false,
                        bool is_part_table = false);

  int remove_table_mapping(Sdb_conn *sdb_conn, const char *db_name,
                           const char *table_name, bool is_tmp_table = false);

  int update_table_mapping(Sdb_conn *sdb_conn, const char *src_db_name,
                           const char *src_table_name, const char *dst_db_name,
                           const char *dst_table_name,
                           bool is_tmp_table = false);

  int set_table_mapping_state(Sdb_conn *sdb_conn, const char *db_name,
                              const char *table_name, bool is_tmp_table,
                              enum_mapping_state state);

  int get_sequence_mapping_cs(const char *db_name, char *cs_name,
                              bool is_tmp_table, bool pefer_origin_name);

  int drop_db_mapping(Sdb_conn *sdb_conn, const char *db_name);

  int remove_parts_mapping(Sdb_conn *sdb_conn, const char *db_name,
                           const char *table_name);
};

#endif
