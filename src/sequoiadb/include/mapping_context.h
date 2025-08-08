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

   Source File Name = mapping_context.h

   Descriptive Name = N/A

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          08/08/2025  SequoiaDB  Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef MAPPING_CONTEXT__H
#define MAPPING_CONTEXT__H

enum enum_mapping_state {
  NM_STATE_NONE = 0,
  NM_STATE_CREATING,
  NM_STATE_CREATED
};

class Mapping_context {
 public:
  virtual ~Mapping_context() {}
  virtual void set_db(const char *db_name) = 0;
  virtual void set_table(const char *table_name) = 0;
  virtual const char *get_db() = 0;
  virtual const char *get_table() = 0;
  virtual const char *get_mapping_cs() = 0;
  virtual void set_mapping_cs(const char *cs_name) = 0;
  virtual const char *get_mapping_cl() = 0;
  virtual void set_mapping_cl(const char *cl_name) = 0;
  virtual enum_mapping_state get_mapping_state() = 0;
  virtual void set_mapping_state(enum_mapping_state state) = 0;
  virtual bool is_part_table() = 0;
  virtual void set_part_table(bool is_part_table) = 0;
  virtual bool is_specified_mapping() = 0;
  virtual void set_specified_mapping_flag(bool specified) = 0;
};
#endif
