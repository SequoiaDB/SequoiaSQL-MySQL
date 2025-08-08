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
#ifndef SDB_CONDITION__H
#define SDB_CONDITION__H

#include "ha_sdb_item.h"
#include "my_bitmap.h"

enum SDB_COND_STATUS {
  SDB_COND_UNCALLED = -1,
  SDB_COND_SUPPORTED = 1,
  SDB_COND_PART_SUPPORTED,
  SDB_COND_BEFORE_SUPPORTED,
  SDB_COND_UNSUPPORTED,

  SDB_COND_UNKNOWN = 65535
};

struct ha_sdb_update_arg {
  /*
    Variables description:
    For example `set a = b + 1`.
    my_field: `a`, which is to be updated;
    other_field: `b`, which is any field except my_field;
    const_value: 1, like "abc", 100 ...;
    value_field: it is the field to be pushed down as { $field: "xxx" }
    optimizer_update: can this update operation be optimized or not.
  */
  Field *my_field;
  Item_field *value_field;
  uint my_field_count;
  uint other_field_count;
  uint const_value_count;
  bool *optimizer_update;

  ha_sdb_update_arg()
      : my_field(NULL),
        value_field(NULL),
        my_field_count(0),
        other_field_count(0),
        const_value_count(0),
        optimizer_update(NULL) {}
};

class ha_sdb_cond_ctx : public Sql_alloc {
 public:
  /*PUSHED_COND: for pushed condition.
    WHERE_COND:  for where condition.
  */
  enum Ctx_type { INVALID_TYPE = 0, PUSHED_COND, WHERE_COND };
  ha_sdb_cond_ctx(TABLE *cur_table, THD *ha_thd,
                  my_bitmap_map *pushed_cond_buff,
                  my_bitmap_map *where_cond_buff);

  ~ha_sdb_cond_ctx();

  void init(TABLE *cur_table, THD *ha_thd, my_bitmap_map *pushed_cond_buff,
            my_bitmap_map *where_cond_buff);

  void reset();

  void push(Item *item);

  void pop();

  void pop_all();

  void clear();

  Sdb_item *create_sdb_item(Item_func *cond_item);

  int to_bson(bson::BSONObj &obj);

  void update_stat(int rc);

  bool keep_on();

 public:
  Sdb_item *cur_item;
  List<Sdb_item> item_list;
  SDB_COND_STATUS status;
  THD *thd;
  enum Ctx_type type;
  TABLE *table;
  MY_BITMAP where_cond_set;
  MY_BITMAP pushed_cond_set;
  bool has_null_func;
  bool sub_sel;
};

void sdb_parse_condtion(const Item *cond_item, ha_sdb_cond_ctx *sdb_cond);
void sdb_traverse_update(const Item *update_item, void *arg);

#endif
