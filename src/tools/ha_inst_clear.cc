#include <argp.h>
#include <fstream>
#include <string>
#include <memory>
#include <unordered_set>
#include <iostream>
#include <termios.h>
#include <unistd.h>
#include <stdio.h>
#include <stdexcept>
#include <exception>
#include <client.hpp>

#include "ha_tool_utils.h"

using namespace std;
static char doc[] = "Clear instance information from sequoiadb.\v";
const char *argp_program_bug_address = 0;
const char *argp_program_version = 0;
static char args_doc[] = "inst_group_name";

static struct argp_option my_argp_options[] = {
    {"host", HA_KEY_HOST, "HOST", 0,
     "Sequoiadb coord address(hostname:port), default: localhost:11810", 0},
    {"user", 'u', "USER", 0, "User connecting to sequoiadb, default: \"\"", 1},
    {"password", 'p', "PASSWORD", OPTION_ARG_OPTIONAL,
     "Password used to connect to sequoiadb, default: \"\".", 2},
    {"force", HA_KEY_FORCE, 0, 0,
     "Force to delete instance info without confirm", 3},
    {"token", 't', "TOKEN", OPTION_ARG_OPTIONAL,
     "Token used to decrypt password in cipherfile, default: \"\"", 4},
    {"file", HA_KEY_FILE, "FILE", 0,
     "Cipherfile path, default: ~/sequoiadb/passwd", 5},
    {"inst_id", HA_KEY_INST_ID, "INST_ID", 0,
     "Instance unique ID, if this argument is set, delete instance "
     "configuration information "
     "by instance id",
     6},
    {"inst_host", HA_KEY_INST_HOST, "INST_HOST", 0,
     "Instance address, used to delete instance configuration information, "
     "works when 'inst_id' is not set",
     7},
    {"verbose", HA_KEY_VERBOSE, 0, 0, "Print more information", 8},
    {NULL}};

static char *help_filter(int key, const char *text, void *input) {
  // printf("key: %d, text: %s", key, text);
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
        args->inst_id = std::stoi(arg);
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
  sdbclient::sdbCollection config_cl, gstate_cl, istate_cl;
  const char *sdb_err = "";
  sdbclient::sdbCursor cursor;
  int rc = 0;
  bool clear_inst_state = true, clear_global_state = true, clear_config = true;

  // get collection space 'HASysGlobalInfo' handle
  rc = conn.getCollectionSpace(HA_GLOBAL_INFO, global_info_cs);
  sdb_err = rc ? ha_sdb_error_string(conn, rc) : "";
  HA_TOOL_RC_CHECK(rc, rc,
                   "Error: failed to get collection space '%s', "
                   "sequoiadb error: %s",
                   HA_GLOBAL_INFO, sdb_err);

  // get instance group collection space handle
  rc = conn.getCollectionSpace(cmd_args.inst_group_name.c_str(), inst_group_cs);
  sdb_err = rc ? ha_sdb_error_string(conn, rc) : "";
  HA_TOOL_RC_CHECK(rc, rc,
                   "Error: failed to get collection space '%s', "
                   "sequoiadb error: %s",
                   cmd_args.inst_group_name.c_str(), sdb_err);

  // get 'HAConfig' collection handler
  rc = global_info_cs.getCollection(HA_CONFIG_CL, config_cl);
  if (SDB_DMS_NOTEXIST == rc) {
    cout << "Info: no SQL instances in current sequoiadb cluster" << endl;
    return HA_ERR_OK;
  }
  sdb_err = rc ? ha_sdb_error_string(conn, rc) : "";
  HA_TOOL_RC_CHECK(rc, rc,
                   "Error: failed to get collection '%s.%s', "
                   "sequoiadb error: %s",
                   HA_GLOBAL_INFO, HA_CONFIG_CL, sdb_err);

  // get 'HAGlobalState' collection handler
  rc = inst_group_cs.getCollection(HA_GLOBAL_STATE_CL, gstate_cl);
  if (SDB_DMS_NOTEXIST == rc) {
    clear_global_state = false;
    cout << "Info: can't find instance group global state table, there is no "
            "need to clear instance global state"
         << endl;
  }

  // get 'HAInstanceState' collection handler
  rc = inst_group_cs.getCollection(HA_INSTANCE_STATE_CL, istate_cl);
  if (SDB_DMS_NOTEXIST == rc) {
    clear_inst_state = false;
    cout << "Info: can't find instance group instance state table, there is "
            "no need to clear instance state"
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
    rc = config_cl.query(cursor, cond);
  }
  if (0 == rc) {
    rc = cursor.next(result, false);
  }
  if (SDB_DMS_EOC == rc) {
    cout << "Info: no such instance in instance group '" << orig_name << "'"
         << endl;
    // if 'inst_id' is not set, can't clear global and instance state
    if (!cmd_args.is_inst_id_set) {
      conn.transactionRollback();
      return 0;
    }
    // continue clear global state and instance state
    rc = 0;
    clear_config = false;
  }
  sdb_err = rc ? ha_sdb_error_string(conn, rc) : "";
  if (rc) {
    cerr << "Error: unable to query instance configuration from '"
         << HA_GLOBAL_INFO << "." << HA_CONFIG_CL
         << "', sequoiadb error: " << sdb_err << endl;
    conn.transactionRollback();
    return rc;
  }

  if (!cmd_args.is_inst_id_set) {
    cmd_args.inst_id = result.getIntField(HA_FIELD_INSTANCE_ID);
  }
  // clear information
  cond = BSON(HA_FIELD_INSTANCE_ID << cmd_args.inst_id);
  // clear config in 'HAConfig'
  if (clear_config) {
    rc = config_cl.del(cond);
  }

  // clear global state info
  if (0 == rc && clear_global_state) {
    rc = gstate_cl.del(cond);
  }

  // clear instance state info
  if (0 == rc && clear_inst_state) {
    rc = istate_cl.del(cond);
  }

  if (rc) {
    sdb_err = ha_sdb_error_string(conn, rc);
    cerr << "Error: failed clear instance configuration, sequoiadb error: "
         << sdb_err << endl;
    conn.transactionRollback();
  } else {
    rc = conn.transactionCommit();
  }
  return rc;
}

int main(int argc, char *argv[]) {
  int rc = 0;
  string orig_name;
  sdbclient::sdb conn;
  bson::BSONObj result, cond;
  sdbclient::sdbCollectionSpace global_info_cs, inst_group_cs;
  sdbclient::sdbCollection config_cl, gstate_cl, istate_cl;
  ha_tool_args cmd_args;
  bool no_passwd_login = false;
  const char *sdb_err = "";
  sdbclient::sdbCursor cursor;

  try {
    ha_init_default_args(cmd_args);
    rc = argp_parse(&my_argp, argc, argv, 0, 0, &cmd_args);
    HA_TOOL_RC_CHECK(rc, rc, "Error: command-line argument parsing error: %s",
                     strerror(rc));

    orig_name = cmd_args.inst_group_name;

    rc = ha_init_sequoiadb_connection(conn, cmd_args);
    HA_TOOL_RC_CHECK(rc, rc,
                     "Error: failed to initialize sequoiadb connection");

    // if 'inst_id' is not set and one of 'hostname', 'port' is not set
    if (!cmd_args.is_inst_id_set && cmd_args.inst_host.empty()) {
      cerr << "Error: please specify 'inst_id' or 'inst_host'" << endl;
      return HA_ERR_INVALID_PARAMETER;
    }

    if (!cmd_args.is_inst_id_set) {
      // parse instance host service address
      rc = ha_parse_host(cmd_args.inst_host, cmd_args.inst_hostname,
                         cmd_args.inst_port);
      HA_TOOL_RC_CHECK(rc, rc,
                       "Error: 'inst_host' is not in the correct format");
    }

    // prompt user to check if they really want to delete instance config
    if (!cmd_args.force) {
      string choose;
      std::string instance;
      if (cmd_args.is_inst_id_set) {
        instance = to_string(cmd_args.inst_id);
      } else {
        instance = cmd_args.inst_hostname + ":" + to_string(cmd_args.inst_port);
      }
      std::unordered_set<std::string> valid_words({"yes", "Y", "y"});
      cout << "Do you really want to clear instance '" << instance
           << "' [Y/N]? ";
      getline(cin, choose);
      if (1 != valid_words.count(choose)) {
        return 0;
      }
    }
    // clear instance information including 'config info', 'global state',
    // 'instance state'
    rc = clear_sql_instance(cmd_args, conn, orig_name);
    HA_TOOL_RC_CHECK(rc, rc, "Error: failed to clear SQL instance");
  } catch (std::exception &e) {
    cerr << "Error: unexpected error: " << e.what() << endl;
    return HA_ERR_EXCEPTION;
  }
  return 0;
}
