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
    {"verbose", HA_KEY_VERBOSE, 0, 0, HA_TOOL_HELP_VERBOSE, 6},
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

int main(int argc, char *argv[]) {
  int rc = 0;
  string orig_name;
  sdbclient::sdb conn;
  bson::BSONObj result, cond;
  sdbclient::sdbCollectionSpace global_info_cs;
  sdbclient::sdbCollection config_cl;
  ha_tool_args cmd_args;
  bool no_passwd_login = false;
  const char *sdb_err = "";

  try {
    ha_init_default_args(cmd_args);
    rc = argp_parse(&my_argp, argc, argv, 0, 0, &cmd_args);
    HA_TOOL_RC_CHECK(rc, rc, "Error: command-line argument parsing error: %s",
                     strerror(rc));

    orig_name = cmd_args.inst_group_name;
    rc = ha_init_sequoiadb_connection(conn, cmd_args);
    HA_TOOL_RC_CHECK(rc, rc,
                     "Error: failed to initialize sequoiadb connection");

    // prompt user to check if they really want to delete instance group config
    if (!cmd_args.force) {
      string choose;
      boost::unordered_set<std::string> valid_words;
      valid_words.insert("yes");
      valid_words.insert("Y");
      valid_words.insert("y");
      cout << "Do you really want to clear instance group '" << orig_name
           << "' [Y/N]? ";
      getline(cin, choose);
      if (1 != valid_words.count(choose)) {
        return 0;
      }
    }

    // drop instance group collection space
    rc = conn.dropCollectionSpace(cmd_args.inst_group_name.c_str());
    // ignore 'SDB_DMS_CS_NOTEXIST' error
    if (SDB_DMS_CS_NOTEXIST == rc) {
      rc = 0;
    } else if (rc) {
      sdb_err = ha_sdb_error_string(conn, rc);
    } else {
      sdb_err = "";
    }
    HA_TOOL_RC_CHECK(rc, rc,
                     "Error: failed to clear instance group '%s', "
                     "sequoiadb error: %s",
                     cmd_args.inst_group_name.c_str(), sdb_err);

    // delete instance group global configuration from 'HAConfig'
    rc = conn.getCollectionSpace(HA_GLOBAL_INFO, global_info_cs);
    sdb_err = rc ? ha_sdb_error_string(conn, rc) : "";
    if (SDB_DMS_CS_NOTEXIST == rc) {
      cout << "Warning: the global configuration database doesn't exist"
           << endl;
      return SDB_HA_OK;
    }
    HA_TOOL_RC_CHECK(rc, rc,
                     "Error: failed to get global configuration database '%s', "
                     "sequoiadb error: %s",
                     HA_GLOBAL_INFO, sdb_err);

    // if 'HAConfig' does not exist, do not report error
    rc = global_info_cs.getCollection(HA_CONFIG_CL, config_cl);
    if (SDB_DMS_NOTEXIST == rc) {
      return SDB_HA_OK;
    }
    sdb_err = rc ? ha_sdb_error_string(conn, rc) : "";
    HA_TOOL_RC_CHECK(rc, rc,
                     "Error: failed to get global configuration table '%s.%s', "
                     "sequoiadb error: %s",
                     HA_GLOBAL_INFO, HA_CONFIG_CL, sdb_err);

    cond = BSON(HA_FIELD_INSTANCE_GROUP_NAME << orig_name.c_str());
    rc = config_cl.del(cond);
    sdb_err = rc ? ha_sdb_error_string(conn, rc) : "";
    HA_TOOL_RC_CHECK(rc, rc,
                     "Error: failed to delete instance group "
                     "global configuration from '%s.%s', sequoiadb error: %s",
                     HA_GLOBAL_INFO, HA_CONFIG_CL, sdb_err);
  } catch (std::exception &e) {
    cerr << "Error: unexpected error: " << e.what() << endl;
    return SDB_HA_EXCEPTION;
  }
  return 0;
}
