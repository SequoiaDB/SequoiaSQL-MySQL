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

#ifndef SDB_IDX__H
#define SDB_IDX__H

#include <sql_class.h>
#include <client.hpp>
#include <boost/shared_ptr.hpp>
#include "sdb_cl.h"

const static uint16 NULL_BITS = 1;

struct Sdb_index_stat {
  uint *distinct_val_num;
  double *min_value_arr;
  double *max_value_arr;
  uint null_frac;
  ha_rows sample_records;
  uint version;

  int init(KEY *arg_key, uint arg_version);

  void fini();

  Sdb_index_stat() {
    distinct_val_num = NULL;
    min_value_arr = NULL;
    max_value_arr = NULL;
    null_frac = 0;
    sample_records = ~(ha_rows)0;
    version = 0;
  }

  ~Sdb_index_stat() { fini(); }
};

typedef boost::shared_ptr<Sdb_index_stat> Sdb_idx_stat_ptr;

inline int get_variable_key_length(const uchar *A) {
  return (int)(((uint16)(A[0])) + ((uint16)(A[1]) << 8));
}

int sdb_create_index(const KEY *key_info, Sdb_cl &cl,
                     bool shard_by_part_id = false);

int sdb_get_idx_order(KEY *key_info, bson::BSONObj &order, int order_direction,
                      bool secondary_sort_oid);

/*
  type: index scan type
    1: advance to the first record of same value.
    2: advance to the next different record from the value.
*/
int sdb_create_condition_from_key(TABLE *table, KEY *key_info,
                                  const key_range *start_key,
                                  const key_range *end_key,
                                  bool from_records_in_range, bool eq_range_arg,
                                  bson::BSONObj &start_cond,
                                  bson::BSONObj &end_cond);

int sdb_get_key_part_value(const KEY_PART_INFO *key_part, const uchar *key_ptr,
                           const char *op_str, bool ignore_text_key,
                           bson::BSONObjBuilder &obj_builder);

int sdb_get_key_direction(ha_rkey_function find_flag);

my_bool sdb_is_same_index(const KEY *a, const KEY *b);

void sdb_init_min_max_value_arr(KEY *key_info, double *min_value_arr,
                                double *max_value_arr);

int sdb_get_min_max_from_bson(KEY *key_info, bson::BSONElement &min_elem,
                              bson::BSONElement &max_elem,
                              double *min_value_arr, double *max_value_arr);

ha_rows sdb_estimate_match_count(Sdb_idx_stat_ptr stat_ptr, KEY *key_info,
                                 ha_rows total_records,
                                 const key_range *start_key,
                                 const key_range *end_key);

#endif
