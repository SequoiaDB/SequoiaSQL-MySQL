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

#ifndef HA_SDB_SEQ__H
#define HA_SDB_SEQ__H

#ifdef IS_MARIADB

#include "ha_sdb.h"
#include "sdb_seq.h"

class ha_sdb_seq : public ha_sdb {
 public:
  ha_sdb_seq(handlerton *hton, TABLE_SHARE *table_arg);

  int info(uint flag) { return 0; }

  int ensure_sequence(THD *thd);

  int create(const char *name, TABLE *form, HA_CREATE_INFO *create_info);

  int open(const char *name, int mode, uint test_if_locked);

  int write_row(uchar *buf);

  int update_row(const uchar *old_data, const uchar *new_data);

  int start_statement(THD *thd, uint table_count) { return 0; }

  int external_lock(THD *thd, int lock_type);

  int rnd_init(bool scan);

  int rnd_next(uchar *buf);

  int rnd_end() { return 0; }

  int reset();

  int close(void);

 private:
  int build_attribute_of_sequence(bson::BSONObj &options);

  int acquire_and_adjust_sequence_value(Sdb_seq *sdb_seq);

  int insert_into_sequence();

  int alter_sequence();

  int select_sequence();

 private:
  Sdb_seq *m_sequence;
  bool m_sequence_to_be_set;
  longlong m_acquire_up_bound;
  longlong m_acquire_low_bound;
  char m_sequence_name[SDB_CL_NAME_MAX_SIZE + 1];
};

#endif  // IS_MARIADB
#endif  // HA_SDB_SEQ__H
