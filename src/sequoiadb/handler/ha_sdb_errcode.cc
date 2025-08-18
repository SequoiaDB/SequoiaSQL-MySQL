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
#include "my_global.h"
#include "my_base.h"
#include "ha_sdb_errcode.h"

void convert_sdb_code(int &rc) {
  if (rc < 0) {
    rc = SDB_ERR_INNER_CODE_BEGIN - rc;
    goto done;
  }

  switch (rc) {
    case SDB_ERR_NOT_ALLOWED: {
      rc = HA_ERR_NOT_ALLOWED_COMMAND;
      break;
    }
    case SDB_ERR_OOM: {
      rc = HA_ERR_OUT_OF_MEM;
      break;
    }
    case SDB_ERR_BUILD_BSON: {
      rc = HA_ERR_INTERNAL_ERROR;
      break;
    }
  }
done:
  return;
}

int get_sdb_code(int rc) {
  if (rc > SDB_ERR_INNER_CODE_BEGIN && rc < SDB_ERR_INNER_CODE_END) {
    return SDB_ERR_INNER_CODE_BEGIN - rc;
  }
  return rc;
}
