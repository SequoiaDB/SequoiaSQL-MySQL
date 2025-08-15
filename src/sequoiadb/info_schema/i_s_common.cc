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

#include "i_s_common.h"

struct st_mysql_information_schema i_s_info = {
    MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};

uchar *i_s_pair_get_key(i_s_name_id_pair *pair, size_t *length,
                        my_bool not_used MY_ATTRIBUTE((unused))) {
  *length = pair->name_len;
  return (uchar *)pair->name;
}

int i_s_elem_to_string(bson::BSONElement &elem, std::string &str) {
  int rc = 0;
  try {
    if (bson::String == elem.type()) {
      str = elem.String();
    } else {
      str = elem.toString(false, true);
    }
  } catch (std::exception &e) {
    rc = 1;
    goto error;
  }
done:
  return rc;
error:
  goto done;
}

int i_s_store_elem_to_field(HASH &name_id_pair_hash,
                            TABLE *table,
                            bson::BSONElement &elem) {
  int rc = 0;
  Field *field = NULL;
  const char *name = elem.fieldName();
  i_s_name_id_pair *pair = NULL;

  pair = (i_s_name_id_pair *)my_hash_search(&name_id_pair_hash,
                                            (uchar *)name, (uint)strlen(name));
  if (NULL == pair) {
    // May it be field in newer version. Just ignore it.
    rc = 0;
    goto done;
  }
  field = table->field[pair->id];

  if (!bitmap_is_set(field->table->read_set, field->field_index)) {
    rc = 0;
    goto done;
  }

  field->set_notnull();

  switch (field->type()) {
    case MYSQL_TYPE_VARCHAR: {
      std::string str;
      rc = i_s_elem_to_string(elem, str);
      if (rc) {
        goto error;
      }
      field->store(str.c_str(), str.length(), system_charset_info);
      break;
    }
    case MYSQL_TYPE_LONG: {
      field->store(elem.numberInt());
      break;
    }
    case MYSQL_TYPE_TINY: {
      field->store(elem.booleanSafe() ? 1 : 0);
      break;
    }
    case MYSQL_TYPE_NULL: {
      // This field is not used. Skip it.
      break;
    }
    default: {
      DBUG_ASSERT(0);
      rc = 1;
      goto error;
    }
  }
done:
  return rc;
error:
  goto done;
}

