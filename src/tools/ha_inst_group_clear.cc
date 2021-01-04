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

#include <argp.h>
#include <fstream>
#include <string>
#include <memory>
#include <boost/unordered_set.hpp>
#include <iostream>
#include <termios.h>
#include <unistd.h>
#include <stdio.h>
#include <stdexcept>
#include <exception>
#include <client.hpp>

#include "ha_tool_utils.h"

using namespace std;
static char doc[] = HA_TOOL_HELP_DOC_CLEAR_INST_GROUP;
const char *argp_program_bug_address = 0;
const char *argp_program_version = 0;
static char args_doc[] = HA_TOOL_HELP_DOC_INST_GROUP_NAME;

// the last parameter indicates the order of the help information
static struct argp_option my_argp_options[] = {
    {"host", HA_KEY_HOST, "HOST", 0, HA_TOOL_HELP_HOST, 0},
    {"user", 'u', "USER", 0, HA_TOOL_HELP_USER, 1},
    {"password", 'p', "PASSWORD", OPTION_ARG_OPTIONAL, HA_TOOL_HELP_PASSWD, 2},
    {"force", HA_KEY_FORCE, 0, 0, HA_TOOL_HELP_FORCE, 3},
    {"token", 't', "TOKEN", OPTION_ARG_OPTIONAL, HA_TOOL_HELP_TOKEN, 4},
    {"file", HA_KEY_FILE, "FILE", 0, HA_TOOL_HELP_FILE, 5},
    {"inst_id", HA_KEY_INST_ID, "INST_ID", 0, HA_TOOL_HELP_INST_ID, 6},
    {"inst_host", HA_KEY_INST_HOST, "INST_HOST", 0, HA_TOOL_HELP_INST_HOST, 7},
    {"verbose", HA_KEY_VERBOSE, 0, 0, HA_TOOL_HELP_VERBOSE, 8},
    {NULL}};

static char *help_filter(int key, const char *text, void *input) {
  if (ARGP_KEY_HELP_DUP_ARGS_NOTE == key) {
    return NULL;
  }
  return (char *)text;
}

static error_t parse_option(int key, char *arg, struct argp_state *state);
static struct argp my_argp = {
    my_argp_options, parse_option, args_doc, doc, 0, help_filter, 0};

static error_t parse_option(int key, char *arg, struct argp_state *state) {
  st_args *args = (st_args *)state->input;

  if (state && state->root_argp && 0 == state->root_argp->help_filter) {
    struct argp *root_argp_tmp = (struct argp *)state->root_argp;
    root_argp_tmp->help_filter = help_filter;
  }
  switch (key) {
    case HA_KEY_HOST:
      args->host = arg;
      break;
    case 'u':
      args->is_user_set = true;
      args->user = arg;
      break;
    case 'p':
      args->is_password_set = true;
      args->password = arg ? arg : "";
      break;
    case HA_KEY_FORCE:
      args->force = true;
      break;
    case 't':
      args->token = arg ? arg : "";
      break;
    case HA_KEY_FILE:
      args->file_name = arg;
      break;
    case HA_KEY_INST_ID:
      try {
        args->inst_id = atoi(arg);
        args->is_inst_id_set = true;
      } catch (std::exception &e) {
        cerr << "Error: invalid argument 'inst_id' value: " << arg
             << ", it must be an integer" << endl;
        return EINVAL;
      }
      break;
    case HA_KEY_INST_HOST:
      args->inst_host = arg;
      break;
    case HA_KEY_VERBOSE:
      args->verbose = true;
      break;
    case ARGP_KEY_NO_ARGS:
      argp_usage(state);
      break;
    case ARGP_KEY_ARG:
      args->inst_group_name = arg;
      if (args->inst_group_name.empty()) {
        cerr << "Error: 'inst_group_name' can't be empty." << endl;
        argp_usage(state);
      }
      state->next = state->argc;
      break;
    default:
      return ARGP_ERR_UNKNOWN;
  }
  return 0;
}

static int clear_sql_instance(ha_tool_args &cmd_args, sdbclient::sdb &conn,
                              const std::string &orig_name) {
  bson::BSONObj result, cond;
  sdbclient::sdbCollectionSpace global_info_cs, inst_group_cs;
  sdbclient::sdbCollection registry_cl, inst_state_cl, inst_obj_state_cl;
  sdbclient::sdbCursor cursor;
  int rc = 0;
  bool clear_inst_obj_state = true;
  bool clear_inst_state = true;
  bool clear_registry_info = true;

  // get collection space 'HASysGlobalInfo' handle
  rc = conn.getCollectionSpace(HA_GLOBAL_INFO, global_info_cs);
  if (SDB_DMS_CS_NOTEXIST == rc) {
    cout << "Error: global configuration database doesn't exist" << endl;
    return rc;
  }
  HA_TOOL_RC_CHECK(rc, rc,
                   "Error: failed to get global configuration database '%s', "
                   "sequoiadb error: %s",
                   HA_GLOBAL_INFO, ha_sdb_error_string(conn, rc));

  // get instance group collection space handle
  rc = conn.getCollectionSpace(cmd_args.inst_group_name.c_str(), inst_group_cs);
  if (SDB_DMS_CS_NOTEXIST == rc) {
    cout << "Error: instance group '" << orig_name
         << "' configuration database doesn't exist" << endl;
    return rc;
  }
  HA_TOOL_RC_CHECK(rc, rc,
                   "Error: failed to get instance group configuration "
                   "database '%s', sequoiadb error: %s",
                   cmd_args.inst_group_name.c_str(),
                   ha_sdb_error_string(conn, rc));

  // get 'HARegistry' collection handler
  rc = global_info_cs.getCollection(HA_REGISTRY_CL, registry_cl);
  if (SDB_DMS_NOTEXIST == rc) {
    cout << "Info: no initialized SQL instances in current cluster" << endl;
    return 0;
  }
  HA_TOOL_RC_CHECK(rc, rc,
                   "Error: failed to get global configuration table "
                   "'%s.%s', sequoiadb error: %s",
                   HA_GLOBAL_INFO, HA_REGISTRY_CL,
                   ha_sdb_error_string(conn, rc));

  // get 'HAInstanceState' collection handler
  rc = inst_group_cs.getCollection(HA_INSTANCE_STATE_CL, inst_state_cl);
  if (SDB_DMS_NOTEXIST == rc) {
    clear_inst_state = false;
    cout << "Info: can't find instance state table, there is no "
            "need to clear instance state"
         << endl;
  }

  // get 'HAInstanceObjectState' collection handler
  rc = inst_group_cs.getCollection(HA_INSTANCE_OBJECT_STATE_CL,
                                   inst_obj_state_cl);
  if (SDB_DMS_NOTEXIST == rc) {
    clear_inst_obj_state = false;
    cout << "Info: can't find instance group instance object state table, "
            "there is no need to clear instance object state"
         << endl;
  }

  // instance info can be deleted by 'instance id' or
  // 'inst_hostname:inst_port'
  if (cmd_args.is_inst_id_set) {
    cond =
        BSON(HA_FIELD_INSTANCE_ID << cmd_args.inst_id
                                  << HA_FIELD_INSTANCE_GROUP_NAME << orig_name);
  } else {
    cond = BSON(HA_FIELD_HOST_NAME
                << cmd_args.inst_hostname << HA_FIELD_PORT << cmd_args.inst_port
                << HA_FIELD_INSTANCE_GROUP_NAME << orig_name);
  }

  // start transaction to clear instance configuration
  rc = conn.transactionBegin();
  if (0 == rc) {
    rc = registry_cl.query(cursor, cond);
  }
  if (0 == rc) {
    rc = cursor.next(result, false);
  }
  if (SDB_DMS_EOC == rc) {
    cout << "Error: can't find such instance in instance group '" << orig_name
         << "'" << endl;
    return rc;
  }
  if (rc) {
    cerr << "Error: unable to query instance configuration from '"
         << HA_GLOBAL_INFO << "." << HA_REGISTRY_CL
         << "', sequoiadb error: " << ha_sdb_error_string(conn, rc) << endl;
    conn.transactionRollback();
    return rc;
  }

  if (!cmd_args.is_inst_id_set) {
    cmd_args.inst_id = result.getIntField(HA_FIELD_INSTANCE_ID);
  }
  // clear information
  cond = BSON(HA_FIELD_INSTANCE_ID << cmd_args.inst_id);
  // clear config in 'HARegistry'
  if (clear_registry_info) {
    rc = registry_cl.del(cond);
  }

  // clear instance state info
  if (0 == rc && clear_inst_state) {
    rc = inst_state_cl.del(cond);
  }

  // clear instance object state info
  if (0 == rc && clear_inst_obj_state) {
    rc = inst_obj_state_cl.del(cond);
  }

  if (rc) {
    cerr << "Error: failed clear instance configuration, sequoiadb error: "
         << ha_sdb_error_string(conn, rc) << endl;
    conn.transactionRollback();
  } else {
    rc = conn.transactionCommit();
  }

  if (cmd_args.is_inst_id_set) {
    cout << "Info: completed cleanup of instance '" << cmd_args.inst_id << "'"
         << endl;
  } else {
    cout << "Info: completed cleanup of instance '" << cmd_args.inst_host << "'"
         << endl;
  }
  return rc;
}

static int clear_sql_inst_group(ha_tool_args &cmd_args, sdbclient::sdb &conn,
                                string orig_name) {
  int rc = 0;
  bson::BSONObj cond;
  sdbclient::sdbCollectionSpace global_info_cs;
  sdbclient::sdbCollection registry_cl;
  sdbclient::sdbCollectionSpace inst_group_cs;

  rc = conn.getCollectionSpace(cmd_args.inst_group_name.c_str(), inst_group_cs);
  if (SDB_DMS_CS_NOTEXIST == rc) {
    cerr << "Error: instance group '" << orig_name << "' doesn't exist" << endl;
    return rc;
  } else if (0 != rc) {
    cerr << "Error: failed to get insance group '" << cmd_args.inst_group_name
         << "' configuration database, sequoiadb error: "
         << ha_sdb_error_string(conn, rc) << endl;
    return rc;
  }

  // drop instance group configuration database
  rc = conn.dropCollectionSpace(cmd_args.inst_group_name.c_str());
  // ignore 'SDB_DMS_CS_NOTEXIST' error
  if (SDB_DMS_CS_NOTEXIST == rc) {
    rc = 0;
  }
  HA_TOOL_RC_CHECK(rc, rc,
                   "Error: failed to clear instance group '%s', "
                   "sequoiadb error: %s",
                   cmd_args.inst_group_name.c_str(),
                   ha_sdb_error_string(conn, rc));

  // delete instance group global configuration from 'HARegistry'
  rc = conn.getCollectionSpace(HA_GLOBAL_INFO, global_info_cs);
  if (SDB_DMS_CS_NOTEXIST == rc) {
    cout << "Error: global configuration database doesn't exist" << endl;
    return rc;
  }
  HA_TOOL_RC_CHECK(rc, rc,
                   "Error: failed to get global configuration database '%s', "
                   "sequoiadb error: %s",
                   HA_GLOBAL_INFO, ha_sdb_error_string(conn, rc));

  // if 'HARegistry' does not exist, do not report error
  rc = global_info_cs.getCollection(HA_REGISTRY_CL, registry_cl);
  if (SDB_DMS_NOTEXIST == rc) {
    cout << "Info: completed cleanup of instance group '" << orig_name << "'"
         << endl;
    return SDB_HA_OK;
  }
  HA_TOOL_RC_CHECK(rc, rc,
                   "Error: failed to get global configuration table '%s.%s', "
                   "sequoiadb error: %s",
                   HA_GLOBAL_INFO, HA_REGISTRY_CL,
                   ha_sdb_error_string(conn, rc));

  cond = BSON(HA_FIELD_INSTANCE_GROUP_NAME << orig_name.c_str());
  rc = registry_cl.del(cond);
  HA_TOOL_RC_CHECK(rc, rc,
                   "Error: failed to delete instance group "
                   "global configuration from '%s.%s', sequoiadb error: %s",
                   HA_GLOBAL_INFO, HA_REGISTRY_CL,
                   ha_sdb_error_string(conn, rc));
  cout << "Info: completed cleanup of instance group '" << orig_name << "'"
       << endl;
  return rc;
}

int main(int argc, char *argv[]) {
  int rc = 0;
  string orig_name;
  sdbclient::sdb conn;
  ha_tool_args cmd_args;
  bool no_passwd_login = false;
  bool clear_instance = false;
  string instance;

  try {
    ha_init_default_args(cmd_args);
    rc = argp_parse(&my_argp, argc, argv, 0, 0, &cmd_args);
    HA_TOOL_RC_CHECK(rc, rc, "Error: command-line argument parsing error: %s",
                     strerror(rc));

    orig_name = cmd_args.inst_group_name;
    rc = ha_init_sequoiadb_connection(conn, cmd_args);
    HA_TOOL_RC_CHECK(rc, rc,
                     "Error: failed to initialize sequoiadb connection");

    clear_instance = (cmd_args.is_inst_id_set || !cmd_args.inst_host.empty());
    // get instance identifier
    if (clear_instance) {
      string choose;
      if (cmd_args.is_inst_id_set) {
        std::ostringstream os;
        os << cmd_args.inst_id;
        instance = os.str();
      } else {
        std::ostringstream os;
        // parse instance host service address
        rc = ha_parse_host(cmd_args.inst_host, cmd_args.inst_hostname,
                           cmd_args.inst_port);
        HA_TOOL_RC_CHECK(rc, rc,
                         "Error: 'inst_host' is not in the correct format");
        os << cmd_args.inst_port;
        instance = cmd_args.inst_hostname + ":" + os.str();
      }
    }

    // prompt user to check if they really want to delete instance group config
    if (!cmd_args.force) {
      string choose;
      boost::unordered_set<std::string> valid_words;
      valid_words.insert("yes");
      valid_words.insert("Y");
      valid_words.insert("y");

      if (clear_instance) {
        cout << "Do you really want to clear instance '" << instance
             << "' [y/N]? ";
      } else {
        cout << "Do you really want to clear instance group '" << orig_name
             << "' [y/N]? ";
      }
      getline(cin, choose);
      if (1 != valid_words.count(choose)) {
        return 0;
      }
    }

    if (clear_instance) {
      rc = clear_sql_instance(cmd_args, conn, orig_name);
    } else {
      rc = clear_sql_inst_group(cmd_args, conn, orig_name);
    }
  } catch (std::exception &e) {
    cerr << "Error: unexpected error: " << e.what() << endl;
    return SDB_HA_EXCEPTION;
  }
  return rc;
}
