#!/usr/bin/env python
# Copyright 2011-2013 Andreas Wundsam
# Copyright 2011-2013 Colin Scott
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

import lifecycle as exp_lifecycle
from util import timestamp_string, mkdir_p, rm_rf, create_clean_dir, find

import os
import shutil
import re
import logging
import errno
import sys
import argparse

def setup_experiment(args, config):
  # Grab parameters
  if args.exp_name:
    config.exp_name = args.exp_name
  # elif not hasattr(config, 'exp_name'):
  #   config.exp_name = exp_lifecycle.guess_config_name(config)

  if not hasattr(config, 'results_dir'):
    config.results_dir = "experiments/%s" % config.exp_name

  if args.timestamp_results is not None:
    # Note that argparse returns a list
    config.timestamp_results = args.timestamp_results

  if hasattr(config, 'timestamp_results') and config.timestamp_results:
    now = timestamp_string()
    config.results_dir += "_" + str(now)

  # Set up results directory
  mkdir_p("./experiments")
  create_clean_dir(config.results_dir)

  # Make sure that there are no uncommited changes
  if args.publish:
    exp_lifecycle.publish_prepare(config.exp_name, config.results_dir)

  # Record machine information for this experiment
  additional_metadata = None
  if hasattr(config, "get_additional_metadata"):
    additional_metadata = config.get_additional_metadata()

  exp_lifecycle.dump_metadata("%s/metadata" % config.results_dir,
                              additional_metadata=additional_metadata)

  # # Copy over config file
  # config_file = re.sub(r'\.pyc$', '.py', config.__file__)
  # if os.path.exists(config_file):
  #   canonical_config_file = config.results_dir + "/orig_config.py"
  #   if os.path.abspath(config_file) != os.path.abspath(canonical_config_file):
  #     shutil.copy(config_file, canonical_config_file)

def top_level_prefix():
  # TODO(cs): use a string builder
  prefix = "."

  def areWeThereYet(prefix):
    return "interposition" in os.listdir(prefix)

  while (not areWeThereYet(prefix)):
    prefix = prefix + "/.."

  return prefix + "/"


if __name__ == '__main__':
  description = """
  Create an experiment directory, and fill it with a metadata file.
  """

  parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                   description=description)

  parser.add_argument('-v', '--verbose', action="count", default=0,
                      help='''increase verbosity''')

  parser.add_argument('-n', '--exp-name', dest="exp_name",
                      default=None, required=True,
                      help='''experiment name (determines result directory name)''')

  parser.add_argument('-t', '--timestamp-results', dest="timestamp_results",
                      default=False, action="store_true",
                      help='''whether to append a timestamp to the result directory name''')

  parser.add_argument('-p', '--publish', action="store_true", default=False,
                      help='''automatically publish experiment results to git''')

  args = parser.parse_args()
  class Config(object):
    pass
  config = Config()

  os.chdir(top_level_prefix())

  setup_experiment(args, config)
  print os.path.abspath(config.results_dir)
