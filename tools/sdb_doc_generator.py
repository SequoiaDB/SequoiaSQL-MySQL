#! /usr/bin/python
# -*- coding: utf-8 -*-

# Copyright (c) 2018, SequoiaDB and/or its affiliates. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

import os
import sys
import getopt
import re

ERR_OK = 0
ERR_INVALID_ARG = 1
ERR_PARSE = 2

OUT_FILE_NAME = 'sdb_doc'

DFT_ARG_LANG = 'cn'
DFT_ARG_OUT = '.'
DFT_ARG_FORMAT = 'all'

MY_CNF_DEFAULT = \
"[client]\n\
default-character-set=utf8mb4\n\
\n\
[mysqld]\n\
sql_mode=STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION\n\
character_set_server=utf8mb4\n\
collation_server=utf8mb4_bin\n\
default_storage_engine=SequoiaDB\n\n"



def enum(*args):
    enums = dict(zip(args, range(len(args))))
    return type('Enum', (), enums)

FormatType = enum('MARKDOWN', 'CNF', 'ALL')
Language = enum('ENGLISH', 'CHINESE')



class DocTuple:
    def __init__(self):
        self.name = ""
        self.type = ""
        self.default = ""
        self.online = ""
        self.scope = ""
        self.desp_cn = ""
        self.desp_en = ""

    @staticmethod
    def get_md_header(language):
        if language == Language.CHINESE:
            header = '|参数名|类型|默认值|动态生效|作用范围|说明|'
        elif language == Language.ENGLISH:
            header = '|Parameter|Type|Default|Online|Scope|Description|'
        else:
            print("ERROR: Unknown language:" + fmt)
            return None
        header += '\n|---|---|---|---|---|---|\n'
        return header
    
    def toString(self, fmt = FormatType.MARKDOWN, language = Language.ENGLISH):
        if fmt == FormatType.MARKDOWN:
            if language == Language.CHINESE:
              descript = self.desp_cn
            elif language == language.ENGLISH:
              descript = self.desp_en
            return "|" + self.name + "|" + self.type + "|" + self.default + \
                    "|" + self.online + "|" + self.scope + "|" + descript + "|\n"
        elif fmt == FormatType.CNF:
            default_val = self.default
            last = len(default_val) - 1
            if (self.default[0] == '"' and self.default[last] == '"') or \
                (self.default[0] == "'" and self.default[last] == "'"):
                default_val = default_val[1:last]
            res = "# " + self.desp_en + "\n"
            res += "# " + self.name + "=" + default_val + "\n\n"
            return res
        else:
            print("ERROR: Unknown format:" + fmt)
            return None



class DocExtractor:
    def __init__(self, language):
        self.language = language
        self.tuples = []

    def is_alpha(self, character):
        return ('A' <= character and character <= 'Z') or \
                ('a' <= character and character <= 'z')

    def get_tuple(self, declare):
        # Declare format: 
        # static MYSQL_XXXVAR_XXX(name, varname, opt, 
        #     "<English Description>" 
        #     "(Default: <Default Value>)." 
        #     /*<Chinese Description>*/, 
        #     check, update, def); 
        
        # Get `scope`
        t = DocTuple()
        bgn = 0
        while declare[bgn] != '_':
            bgn += 1
        bgn += 1
        end = bgn
        while declare[end] != '_':
            end += 1
        scope_type = declare[bgn:end]
        if scope_type == 'SYSVAR':
            t.scope = 'Global'
        elif scope_type == 'THDVAR':
            t.scope = 'Global, Session'
        else:
            print("ERROR: Invalid scope declare:" + scope_type)
            return None

        # Get `type`
        bgn = end + 1
        end = bgn + 1
        while self.is_alpha(declare[end]):
            end += 1
        data_type = declare[bgn:end]
        if data_type == 'BOOL':
            t.type = 'bool'
        elif data_type == 'STR':
            t.type = 'string'
        elif data_type == 'INT':
            t.type = 'int'
        elif data_type == 'UINT':
            t.type = 'unsigned int'
        # Linux64, gcc, long is 64bit
        elif data_type == 'LONG' or data_type == 'LONGLONG':
            t.type = 'long'
        elif data_type == 'ULONG' or data_type == 'ULONGLONG':
            t.type = 'unsigned long'
        elif data_type == 'ENUM':
            t.type = 'enum'
        elif data_type == 'SET':
            t.type = 'set'
        elif data_type == 'DOUBLE':
            t.type = 'double'
        else:
            print("ERROR: Unknown type declare:" + data_type)
            return None

        # Get `name`
        bgn = end + 1
        while not self.is_alpha(declare[bgn]):
            bgn += 1
        end = bgn 
        while declare[end] != ',':
            end += 1
        name = declare[bgn:end]
        t.name = "sequoiadb_" + name

        # Get `online`
        bgn = end + 1
        # Skip parameter varname, which is only belong to SYSVAR
        if (scope_type == 'SYSVAR'): 
            while declare[bgn] != ',':
                bgn += 1
        while not self.is_alpha(declare[bgn]):
            bgn += 1
        end = bgn + 1
        while declare[end] != ',':
            end += 1
        opt = declare[bgn:end]
        if opt.find("PLUGIN_VAR_READONLY") > 0:
            t.online = "No"
        else:
            t.online = "Yes"

        # Get `default`
        comment = ""
        bgn = end + 1
        while True:
            while declare[bgn] != '"':
                bgn += 1
            bgn += 1
            end = bgn
            while declare[end] != '"' or declare[end - 1] == '\\':
                end += 1
            comment += declare[bgn:end]

            # find next adjacent string
            bgn = end + 1
            while declare[bgn] == ' ' or declare[bgn] == '\n':
                bgn += 1
            if declare[bgn] != '"':
                break

        comment = comment.replace('\\"', '"')
        default_declare = re.search("\(\s?Default\s?:.*\)", comment)
        if default_declare:
            default_val = default_declare.group(0)
            val_bgn = 0
            while default_val[val_bgn] != ':':
                val_bgn += 1
            val_bgn += 1
            while default_val[val_bgn] == ' ':
                val_bgn += 1
            val_end = len(default_val) - 2
            while default_val[val_end] == ' ':
                val_end -= 1
            val_end += 1
            t.default = default_val[val_bgn:val_end]
        else:
            print("WARN: No default value in " + t.name)
            t.default = '-'

        # Get `desp_en`
        if default_declare:
            desp_end = comment.find(default_declare.group(0))
        else:
            desp_end = len(comment)
        t.desp_en = comment[0:desp_end]

        # Get `desp_cn`
        while declare[bgn] != '/' or declare[bgn + 1] != '*':
            bgn += 1
        bgn += 2
        end = bgn
        while declare[end] != '*' or declare[end + 1] != '/':
            end += 1
        t.desp_cn = declare[bgn:end]

        return t

    def get_tuples(self, conf_src_path):
        if len(self.tuples) > 0:
            return self.tuples
        with open(conf_src_path, 'r') as f:
            skip_next = False
            while True:
                line = f.readline()
                if not line:
                    break
                if re.match(r'^//\s?SDB_DOC_OPT\s?=\s?IGNORE.*$', line):
                    skip_next = True
                if re.match(r'^static MYSQL_...VAR_.*\(.*$', line):
                    declare = line
                    while not re.match(r'^.*\);$', line):
                        line = f.readline()
                        if not line:
                            break 
                        declare += line
                    if not skip_next:
                        t = self.get_tuple(declare)
                        if not t:
                            return None
                        self.tuples.append(t)
                    else:
                        skip_next = False
        return self.tuples;



class DocExporter:
    def __init__(self, language, fmt):
        self.language = language
        self.fmt = fmt

    def get_file_path(self, out_dir, fmt):
        if fmt == FormatType.MARKDOWN:
            suffix = '.md'
        elif fmt == FormatType.CNF:
            suffix = '.cnf'

        if not os.path.isdir(out_dir):
            print("WARN: Specified output directory is invalid. Using current directory.")
            out_dir = DFT_ARG_OUT

        return out_dir + '/' + OUT_FILE_NAME + suffix

    def export(self, tuples, out_dir):
        if self.fmt == FormatType.MARKDOWN or self.fmt == FormatType.ALL:
            path = self.get_file_path(out_dir, FormatType.MARKDOWN) 
            with open(path, 'w') as f:
                f.write(DocTuple.get_md_header(self.language))
                for t in tuples:
                    f.write(t.toString(FormatType.MARKDOWN, self.language)) 

        if self.fmt == FormatType.CNF or self.fmt == FormatType.ALL:
            path = self.get_file_path(out_dir, FormatType.CNF) 
            with open(path, 'w') as f:
                f.write(MY_CNF_DEFAULT)
                for t in tuples:
                    f.write(t.toString(FormatType.CNF))



def print_help():
    print('Usage: python sdb_doc_generator.py [OPTIONS]')
    print('This tools generates the document from the source code')
    print('')
    print('Options:')
    print('  -l, --language         Document language. Options: cn(Chinese), en(English). ')
    print('                         Default: cn.')
    print('  -o, --out              Output directory. Use current directory by default.')
    print('  -f, --format           Document format. Options: md(MarkDown), cnf(my.cnf ')
    print('                         format), all(All above). Default: all.')
    print('')
    print('Exit status:')
    print(' 0  if OK,')
    print(' 1  if invalid arguments,')
    print(' 2  if failed to parse document,')



def main(argv):
    arg_language = DFT_ARG_LANG
    arg_out = DFT_ARG_OUT
    arg_format = DFT_ARG_FORMAT

    # Get directory of this script.
    this_path = os.path.abspath(argv[0])
    this_dir, this_file_name = os.path.split(this_path);
    argv = argv[1:]

    try:
        opts, args = getopt.getopt(argv, "hl:o:f:",["help","language=","out=","format="])
    except getopt.GetoptError:
        print("ERROR: Unknown Options.")
        print_help()
        sys.exit(ERR_INVALID_ARG)

    for opt, arg in opts:
        if opt in ("-l", "--language"):
            arg_language = arg
        elif opt in ("-o", "--out"):
            arg_out = arg
        elif opt in ("-f", "--format"):
            arg_format = arg
        elif opt in ("-h", "--help"):
            print_help()
            sys.exit()
        else:
            print_help()
            sys.exit(ERR_INVALID_ARG)

    if arg_language == 'cn':
        language_type = Language.CHINESE
    elif arg_language == 'en':
        language_type = Language.ENGLISH
    else:
        print("ERROR: Invalid language option. Please use 'cn' or 'en'")
        sys.exit(ERR_INVALID_ARG)

    if arg_format == 'all':
        format_type = FormatType.ALL
    elif arg_format == 'md':
        format_type = FormatType.MARKDOWN
    elif arg_format == 'cnf':
        format_type = FormatType.CNF
    else:
        print("ERROR: Invalid format option. Please use 'md', 'cnf' or 'all'")
        sys.exit(ERR_INVALID_ARG)

    conf_src_path = this_dir + "/../sdb_conf.cc"

    extractor = DocExtractor(language_type)
    tuples = extractor.get_tuples(conf_src_path)
    if not tuples:
        print("ERROR: Failed to parse document")
        sys.exit(ERR_PARSE)

    exporter = DocExporter(language_type, format_type)
    exporter.export(tuples, arg_out)



if __name__ == '__main__':
    main(sys.argv)
