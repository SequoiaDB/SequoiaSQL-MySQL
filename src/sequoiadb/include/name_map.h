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

class Sdb_conn;

enum enum_mapping_state {
  NM_STATE_NONE = 0,
  NM_STATE_CREATING,
  NM_STATE_CREATED
};

class Name_mapping {
 public:
  virtual int add_mapping(const char *db_name, const char *table_name) = 0;
  virtual int delete_mapping(const char *db_name, const char *table_name) = 0;
  virtual int get_mapping(const char *db_name, const char *table_name) = 0;
  virtual int rename_mapping(const char *db_name, const char *from,
                             const char *to) = 0;
  virtual int get_fixed_mapping(const char *db_name, const char *table_name) = 0;

  virtual const char *get_mapping_db_name() = 0;
  virtual const char *get_mapping_table_name() = 0;
};

class Metadata_Mapping : public Name_mapping {
 private:
  static char m_sql_group[SDB_CS_NAME_MAX_SIZE + 1];
  static bool m_enabled;
  static int m_mapping_unit_size;
  static int m_mapping_unit_count;
  static bool m_pefer_origin_name;

  Sdb_conn **m_sdb_conn;
  bool m_is_part_table;
  char m_cs_name[SDB_CS_NAME_MAX_SIZE + 1];
  char m_cl_name[SDB_CL_NAME_MAX_SIZE + 1];

 public:
  Metadata_Mapping() : m_sdb_conn(NULL), m_is_part_table(false) {
    m_cs_name[0] = '\0';
    m_cl_name[0] = '\0';
  }

  Metadata_Mapping(Sdb_conn **conn, bool is_part_table = false)
      : m_sdb_conn(conn), m_is_part_table(is_part_table) {
    m_cs_name[0] = '\0';
    m_cl_name[0] = '\0';
  }

 private:
  int calculate_mapping_slot(Sdb_conn *sdb_conn, const char *sql_group_cs_name,
                             const char *db_name, int &slot);

  int calculate_mapping_cs(Sdb_conn *sdb_conn, const char *db_name,
                           char *cs_name, const char *sql_group_name);

 public:
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
    m_mapping_unit_size = mapping_size;
  }

  static void set_mapping_group_number(int mapping_num) {
    m_mapping_unit_count = mapping_num;
  }

  static void set_perfer_origin_name(bool perfer_origin_name) {
    m_pefer_origin_name = perfer_origin_name;
  }

  int add_mapping(const char *db_name, const char *table_name);

  int delete_mapping(const char *db_name, const char *table_name);

  int get_mapping(const char *db_name, const char *table_name);

  int rename_mapping(const char *db_name, const char *from, const char *to);

  int get_fixed_mapping(const char *db_name, const char *table_name);

  int get_table_mapping(Sdb_conn *sdb_conn, const char *db_name,
                        const char *table_name,
                        enum_mapping_state *state = NULL);

  int add_table_mapping(Sdb_conn *sdb_conn, const char *db_name,
                        const char *table_name);

  int remove_table_mapping(Sdb_conn *sdb_conn, const char *db_name,
                           const char *table_name);

  int update_table_mapping(Sdb_conn *sdb_conn, const char *src_db_name,
                           const char *src_table_name, const char *dst_db_name,
                           const char *dst_table_name);

  static int get_mapping_cs_by_db(Sdb_conn *conn, const char *db_name,
                                  std::vector<String> &mapping_cs);

  int set_table_mapping_state(Sdb_conn *sdb_conn, const char *db_name,
                              const char *table_name, enum_mapping_state state);

  int get_sequence_mapping_cs(const char *db_name, const char *table_name);

  static int remove_table_mappings(Sdb_conn *sdb_conn, const char *db_name);

  const char *get_mapping_db_name() { return m_cs_name; }

  const char *get_mapping_table_name() { return m_cl_name; }
};
#endif
