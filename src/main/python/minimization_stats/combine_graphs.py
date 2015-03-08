#!/usr/bin/env python2.7
#
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

from generate_graph import write_data_file, load_json, DataInfo, write_gpi_template, invoke_gnuplot
import string
import argparse
import json
import os
import sys

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description=(
  '''Specify any number of minimization_stats.json files to be combined into '''
  '''a single graph. Usage:\n'''
  ''' $0 <path to 1st json> <title for 1st json> <path to 2nd json> '''
  ''' <title for 2nd json> ...'''
  ))
  # Need at least one input file to know where to infer where to put the output.
  parser.add_argument('input1', metavar="INPUT1",
                      help='''The first input json file''')
  parser.add_argument('title1', metavar="TITLE1",
                      help='''The title for input1's line''')
  args, unknown = parser.parse_known_args()
  if (len(unknown) % 2) != 0:
    print >> sys.stderr, "Uneven number of arguments. Need titles!"
    sys.exit(1)

  basename = os.path.basename(args.input1)
  dirname = os.path.dirname(os.path.dirname(args.input1))
  gpi_filename = string.replace(dirname + "/combined_" + basename, ".json", ".gpi")
  output_filename = string.replace(dirname + "/combined_" + basename, ".json", ".pdf")

  # Turn each adjacent pair of elements into a tuple
  # http://stackoverflow.com/questions/4628290/pairs-from-single-list
  pairs = zip(unknown[::2], unknown[1::2])

  graph_title = ""

  data_info_list = []
  for input_json, line_title in [(args.input1, args.title1)] + pairs:
    dat_filename = string.replace(input_json, ".json", ".dat")
    stats = load_json(input_json)
    write_data_file(dat_filename, stats)
    data_info_list.append(DataInfo(title=line_title, filename=dat_filename))

  write_gpi_template(gpi_filename, output_filename, data_info_list, graph_title)
  invoke_gnuplot(gpi_filename)

  print "Output placed in %s" % output_filename
