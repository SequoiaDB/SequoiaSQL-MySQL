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
#include "sdb_cl.h"

int sdb_create_index(const KEY *key_info, Sdb_cl &cl);

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

#endif
