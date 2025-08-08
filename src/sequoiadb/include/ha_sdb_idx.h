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
#ifndef SDB_IDX__H
#define SDB_IDX__H

#include <sql_class.h>
#include <client.hpp>
#include <boost/shared_ptr.hpp>
#include "sdb_cl.h"

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
