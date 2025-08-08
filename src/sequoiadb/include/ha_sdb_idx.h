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
#include "ha_sdb_conf.h"

const static uint16 NULL_BITS = 1;

struct Sdb_index_stat {
  uint *distinct_val_num;
  double *min_value_arr;
  double *max_value_arr;
  uint null_frac;
  ha_rows sample_records;
  // static total records in index statistics information
  ha_rows static_total_records;
  uint version;
  sdb_index_stat_level level;

  int init(KEY *arg_key, uint arg_version);

  void fini();

  Sdb_index_stat() {
    distinct_val_num = NULL;
    min_value_arr = NULL;
    max_value_arr = NULL;
    null_frac = 0;
    sample_records = ~(ha_rows)0;
    static_total_records = ~(ha_rows)0;
    version = 0;
    level = SDB_STATS_LVL_BASE;
  }

  virtual ~Sdb_index_stat() { fini(); }
};

/*
  value_array: Every value is in fixed length. The fixed length is value_len.
               Specially, the long variable value is only store the offset of
               str_buffer, and the value is stored on str_buffer.
  str_buffer: The buffer for variable value(string). The values are stored
              compactly. Format is like '<str_len> | <str_content>'.
  frac_array: The fracation array.

  Example:
  MCV in JSON:
    Values: [ { i: 1, s: "abc" }, { i: 3, s: "123456" }, ... ]
    Frac: [ 2000, 1000, ... ]

  Result:
    value_array: [
      [ 0x00000001, // i:1
        0x00000000 ], // "abc" offset in str_buffer is 0
      [ 0x00000003, // i:3
        0x00000005 ], // "12345" offset in str_buffer is 5
      ...
    ]
    str_buffer: [
      [ 0x0003, // "abc" length is 3
        'a', 'b', 'c' ],
      [ 0x0006, // "123456" length is 6
        '1', '2', '3', '4', '5', '6' ],
      ...
    ]
    frac_array: [ 2000, 1000, ... ]
*/
struct Sdb_index_stat_mcv : public Sdb_index_stat {
  uint value_len;
  uchar *value_array;
  uint16 *frac_array;
  uint array_elem_count;
  uchar *str_buffer;
  uint str_buffer_len;
  uint str_buffer_capacity;
  uint16 total_frac;

  void fini();

  Sdb_index_stat_mcv() {
    value_len = 0;
    value_array = NULL;
    frac_array = NULL;
    array_elem_count = 0;
    str_buffer = NULL;
    str_buffer_len = 0;
    str_buffer_capacity = 0;
    level = SDB_STATS_LVL_MCV;
  }

  virtual ~Sdb_index_stat_mcv() { fini(); };
};

int sdb_fill_mcv_stat(const KEY *key_info, const bson::BSONObj &frac_obj,
                      const bson::BSONObj &values_obj, Sdb_index_stat_mcv &s);

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
