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
static char doc[] = "Clear instance group information from sequoiadb.\v";
const char *argp_program_bug_address = 0;
const char *argp_program_version = 0;
static char args_doc[] = "inst_group_name";

static struct argp_option my_argp_options[] = {
    {"host", HA_KEY_HOST, "HOST", 0,
     "Sequoiadb coord address(hostname:port), default: localhost:11810", 0},
    {"user", 'u', "USER", 0, "User for logging sequoiadb, default: \"\"", 1},
    {"password", 'p', "PASSWORD", OPTION_ARG_OPTIONAL,
     "Password used to connect to sequoiadb, default: \"\"", 2},
    {"force", HA_KEY_FORCE, 0, 0,
     "Force to delete instance group info without confirm", 3},
    {"token", 't', "TOKEN", OPTION_ARG_OPTIONAL,
     "Token used to decrypt password in cipherfile, default: \"\"", 4},
    {"file", HA_KEY_FILE, "FILE", 0,
     "Cipherfile path, default: ~/sequoiadb/passwd", 5},
    {"verbose", HA_KEY_VERBOSE, 0, 0, "Print more information", 6},
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
      std::unordered_set<std::string> valid_words({"yes", "Y", "y"});
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
    HA_TOOL_RC_CHECK(rc, rc,
                     "Error: failed to get global configuration database '%s', "
                     "sequoiadb error: %s",
                     HA_GLOBAL_INFO, sdb_err);

    // if 'HAConfig' does not exist, do not report error
    rc = global_info_cs.getCollection(HA_CONFIG_CL, config_cl);
    if (SDB_DMS_NOTEXIST == rc) {
      return HA_ERR_OK;
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
    return HA_ERR_EXCEPTION;
  }
  return 0;
}
