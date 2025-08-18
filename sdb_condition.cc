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
#ifndef MYSQL_SERVER
#define MYSQL_SERVER
#endif

#include "sdb_condition.h"
#include "sdb_errcode.h"

Sdb_cond_ctx::Sdb_cond_ctx() {
  cur_item = NULL;
  status = SDB_COND_SUPPORTED;
}

Sdb_cond_ctx::~Sdb_cond_ctx() {
  clear();
}

void Sdb_cond_ctx::clear() {
  Sdb_item *item_tmp = NULL;

  if (cur_item != NULL) {
    delete cur_item;
    cur_item = NULL;
  }

  while ((item_tmp = item_list.pop()) != NULL) {
    delete item_tmp;
  }
}

void Sdb_cond_ctx::pop_all() {
  Sdb_item *item_tmp = NULL;

  if (SDB_COND_UNSUPPORTED == status) {
    clear();
    goto done;
  }

  while (!item_list.is_empty()) {
    if (NULL == cur_item) {
      cur_item = item_list.pop();
      continue;
    }

    if (!cur_item->finished()) {
      if (Item_func::COND_AND_FUNC == cur_item->type()) {
        cur_item->push_item((Item *)NULL);
      } else {
        delete cur_item;
        cur_item = NULL;
        update_stat(SDB_ERR_COND_INCOMPLETED);
        continue;
      }
    }

    item_tmp = cur_item;
    cur_item = item_list.pop();
    if (0 != cur_item->push_sdb_item(item_tmp)) {
      delete item_tmp;
    }
  }

done:
  return;
}

void Sdb_cond_ctx::pop() {
  int rc = SDB_ERR_OK;
  Sdb_item *item_tmp = NULL;

  if (!keep_on() || item_list.is_empty()) {
    goto done;
  }

  while (!item_list.is_empty()) {
    if (NULL == cur_item) {
      cur_item = item_list.pop();
      continue;
    }

    item_tmp = cur_item;
    cur_item = item_list.pop();
    rc = cur_item->push_sdb_item(item_tmp);
    if (0 != rc) {
      delete item_tmp;
      goto error;
    }

    if (cur_item->finished()) {
      pop();
    }
    break;
  }

done:
  return;
error:
  update_stat(rc);
  goto done;
}

void Sdb_cond_ctx::update_stat(int rc) {
  if (SDB_ERR_OK == rc) {
    goto done;
  }

  if (SDB_COND_UNSUPPORTED == status || SDB_COND_BEFORE_SUPPORTED == status) {
    goto done;
  }

  if (NULL == cur_item || Item_func::COND_OR_FUNC == cur_item->type()) {
    status = SDB_COND_UNSUPPORTED;
    goto done;
  }

  if (Item_func::COND_AND_FUNC == cur_item->type() &&
      SDB_ERR_COND_PART_UNSUPPORTED == rc) {
    status = SDB_COND_PART_SUPPORTED;
  } else {
    status = SDB_COND_BEFORE_SUPPORTED;
  }

done:
  return;
}

bool Sdb_cond_ctx::keep_on() {
  if (SDB_COND_UNSUPPORTED == status || SDB_COND_BEFORE_SUPPORTED == status) {
    return FALSE;
  }
  return true;
}

void Sdb_cond_ctx::push(Item *item) {
  int rc = SDB_ERR_OK;
  Sdb_item *item_tmp = NULL;

  // get the real item
  // see aslo Item_ref
  Item *cond_item = (NULL == item) ? NULL : item->real_item();

  if (!keep_on()) {
    goto done;
  }

  if (NULL != cur_item) {
    if (NULL == cond_item || (Item::FUNC_ITEM != cond_item->type() &&
                              Item::COND_ITEM != cond_item->type())) {
      rc = cur_item->push_item(cond_item);
      if (0 != rc) {
        goto error;
      }

      if (cur_item->finished()) {
        // finish the current item
        pop();
        if (!keep_on()) {
          // Occur unsupported scene while finish the current item.
          // the status will be set in pop(), so keep rc=0 and skip
          // update_stat()
          goto error;
        }
      }

      goto done;
    }
  }

  if (Item::FUNC_ITEM != cond_item->type() &&
      Item::COND_ITEM != cond_item->type()) {
    // 1. When execute `select * from tb where a`, the first
    //    item will be FIELD_ITEM. This condition has not been handled yet.
    // 2. Subselect always can't be pushed down.
    //
    // This assert is to find out others condition that never be considered.
    DBUG_ASSERT(Item::FIELD_ITEM == cond_item->type() ||
                Item::SUBSELECT_ITEM == cond_item->type());
    rc = SDB_ERR_COND_UNSUPPORTED;
    goto error;
  }
  item_tmp = create_sdb_item((Item_func *)cond_item);
  if (NULL == item_tmp) {
    rc = SDB_ERR_COND_UNSUPPORTED;
    goto error;
  }
  if (cur_item != NULL) {
    item_list.push_front(cur_item);  // netsted func
  }
  cur_item = item_tmp;
  if (cur_item->finished()) {
    // func has no parameter
    pop();
    if (!keep_on()) {
      goto error;
    }
  }

done:
  return;
error:
  update_stat(rc);
  goto done;
}

Sdb_item *Sdb_cond_ctx::create_sdb_item(Item_func *cond_item) {
  Sdb_item *item = NULL;
  switch (cond_item->functype()) {
    case Item_func::COND_AND_FUNC: {
      item = new Sdb_and_item();
      break;
    }
    case Item_func::COND_OR_FUNC: {
      item = new Sdb_or_item();
      break;
    }
    case Item_func::EQ_FUNC:
    case Item_func::EQUAL_FUNC: {
      item = new Sdb_func_eq(cond_item);
      break;
    }
    case Item_func::NE_FUNC: {
      item = new Sdb_func_ne();
      break;
    }
    case Item_func::LT_FUNC: {
      item = new Sdb_func_lt();
      break;
    }
    case Item_func::LE_FUNC: {
      item = new Sdb_func_le();
      break;
    }
    case Item_func::GT_FUNC: {
      item = new Sdb_func_gt();
      break;
    }
    case Item_func::GE_FUNC: {
      item = new Sdb_func_ge();
      break;
    }
    case Item_func::BETWEEN: {
      item = new Sdb_func_between(((Item_func_between *)cond_item)->negated);
      break;
    }
    case Item_func::ISNULL_FUNC: {
      item = new Sdb_func_isnull();
      break;
    }
    case Item_func::ISNOTNULL_FUNC: {
      item = new Sdb_func_isnotnull();
      break;
    }
    case Item_func::IN_FUNC: {
      Item_func_in *item_func = (Item_func_in *)cond_item;
      item = new Sdb_func_in(item_func->negated, item_func->arg_count);
      break;
    }
    case Item_func::LIKE_FUNC: {
      item = new Sdb_func_like((Item_func_like *)cond_item);
      break;
    }
    default: {
      item = new Sdb_func_unkown(cond_item);
      break;
    }
  }
  return item;
}

int Sdb_cond_ctx::to_bson(bson::BSONObj &obj) {
  static bson::BSONObj empty_obj;
  int rc = 0;
  if (NULL != cur_item) {
    rc = cur_item->to_bson(obj);
    if (0 == rc) {
      goto done;
    }
    update_stat(rc);
  }
  obj = empty_obj;

done:
  return rc;
}

static void sdb_traverse_cond(const Item *cond_item, void *arg) {
  Sdb_cond_ctx *sdb_ctx = (Sdb_cond_ctx *)arg;

  if (SDB_COND_UNSUPPORTED == sdb_ctx->status ||
      SDB_COND_BEFORE_SUPPORTED == sdb_ctx->status) {
    // skip all while occured unsupported-condition
    goto done;
  }

  sdb_ctx->push((Item *)cond_item);
done:
  return;
}

void sdb_parse_condtion(const Item *cond_item, Sdb_cond_ctx *sdb_ctx) {
  ((Item *)cond_item)
      ->traverse_cond(&sdb_traverse_cond, (void *)sdb_ctx, Item::PREFIX);
  sdb_ctx->pop_all();
}
