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

#ifndef SDB_DEF__H
#define SDB_DEF__H

#include <client.hpp>

#define SDB_CS_NAME_MAX_SIZE 127
#define SDB_CL_NAME_MAX_SIZE 127
#define SDB_CL_FULL_NAME_MAX_SIZE (SDB_CS_NAME_MAX_SIZE + SDB_CL_NAME_MAX_SIZE)

#define SDB_IDX_FIELD_SIZE_MAX 1024
#define SDB_MATCH_FIELD_SIZE_MAX 1024

#define SDB_CHARSET my_charset_utf8mb4_bin

#define SOURCE_THREAD_ID "Source"
#define PREFIX_THREAD_ID "MySQL-"
#define PREFIX_THREAD_ID_LEN 6
#define TRANSAUTOROLLBACK "TransAutoRollback"
#define TRANSAUTOCOMMIT "TransAutoCommit"

#define SDB_FIELD_NAME_AUTOINCREMENT "AutoIncrement"
#define SDB_FIELD_NAME_FIELD "Field"
#define SDB_FIELD_NAME "Name"
#define SDB_FIELD_SEQUENCE_NAME "SequenceName"
#define SDB_FIELD_CURRENT_VALUE "CurrentValue"
#define SDB_FIELD_INCREMENT "Increment"
#define SDB_FIELD_START_VALUE "StartValue"
#define SDB_FIELD_ACQUIRE_SIZE "AcquireSize"
#define SDB_FIELD_CACHE_SIZE "CacheSize"
#define SDB_FIELD_MAX_VALUE "MaxValue"
#define SDB_FIELD_LAST_GEN_ID "LastGenerateID"

#define SDB_FIELD_SHARDING_KEY "ShardingKey"
#define SDB_FIELD_SHARDING_TYPE "ShardingType"
#define SDB_FIELD_PARTITION "Partition"
#define SDB_FIELD_REPLSIZE "ReplSize"
#define SDB_FIELD_COMPRESSED "Compressed"
#define SDB_FIELD_COMPRESSION_TYPE "CompressionType"
#define SDB_FIELD_ISMAINCL "IsMainCL"
#define SDB_FIELD_AUTO_SPLIT "AutoSplit"
#define SDB_FIELD_GROUP "Group"
#define SDB_FIELD_AUTOINDEXID "AutoIndexId"
#define SDB_FIELD_ENSURE_SHARDING_IDX "EnsureShardingIndex"
#define SDB_FIELD_STRICT_DATA_MODE "StrictDataMode"
#define SDB_FIELD_AUTOINCREMENT SDB_FIELD_NAME_AUTOINCREMENT

const static bson::BSONObj SDB_EMPTY_BSON;

#endif
