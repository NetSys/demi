# Copyright 2011-2013 Andreas Wundsam
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at:
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import getpass
import os
import re
import socket
import sys
import json
import time
from util import timestamp_string, find, backtick, system
import subprocess



def dump_metadata(metadata_file, additional_metadata=None):
  with open(metadata_file, "w") as t:
    metadata = { 'timestamp' : timestamp_string(),
               'argv' : sys.argv,
               'user' : getpass.getuser(),
               'cwd' : os.getcwd(),
               'host' : {
                  'name' : socket.gethostname(),
                  'uptime' : backtick("uptime"),
                  'free' : backtick("exec 2>/dev/null free"),
                  'num_cores' : backtick("cat 2>/dev/null /proc/cpuinfo  | grep '^processor[[:space:]]' | wc -l"),
                  'cpu_info' : backtick("cat 2>/dev/null /proc/cpuinfo | grep 'model name[[:space:]]' | uniq | sed 's/.*://' | perl -pi -e 's/\s+/ /g'")
                },
               'sys' : {
                 'lsb_release' : backtick("exec 2>/dev/null lsb_release --description --short"),
                 'uname' : backtick("uname -a")
               },
               'modules' : {
                 module : { 'commit' : backtick("git rev-parse HEAD", cwd=path),
                            'branch' : backtick("git rev-parse --abbrev-ref HEAD", cwd=path),
                            'status' : backtick("git status", cwd=path),
                            'diff'   : backtick("git diff", cwd=path)
                          } for module, path in [("sts2", "./")]
               },
               'additional_metadata': additional_metadata,
             }
    t.write(json.dumps(metadata, sort_keys=True, indent=2, separators=(',', ": ")) + "\n")

def walk_dirs_up(path):
  while path != "" and path != "/":
    yield path
    path = os.path.dirname(path)

def find_git_dir(results_dir):
  return find(lambda f: os.path.exists(os.path.join(f, ".git" )), walk_dirs_up(results_dir))

def git_has_uncommitted_files(d):
  return system("git diff-files --quiet --ignore-submodules --", cwd=d) > 0 \
    or system("git diff-index --cached --quiet HEAD --ignore-submodules --", cwd=d) > 0

def publish_prepare(exp_name, results_dir):
  for module, path in sts_modules:
    if git_has_uncommitted_files(path):
      raise Exception("Cannot publish: uncommitted changes in sts module %s" % module)

  res_git_dir = find_git_dir(results_dir)
  if not res_git_dir:
    raise Exception("Cannot publish - no git dir found in results tree")

def publish_results(exp_name, results_dir):
    import logging
    log = logging.getLogger("sts.exp_lifecycle")
    res_git_dir = find_git_dir(results_dir)
    rel_results_dir = os.path.relpath(results_dir, res_git_dir)
    log.info("Publishing results to git dir "+res_git_dir)
    system("git add %s" % rel_results_dir, cwd=res_git_dir)
    system("git commit -m '%s'" % exp_name, cwd=res_git_dir)
    system("git pull --rebase", cwd=res_git_dir)
    system("git push", cwd=res_git_dir)
