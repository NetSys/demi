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

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description="generate a plot")
  # TODO(cs): generalize to indefinite input files
  parser.add_argument('input1', metavar="INPUT1",
                      help='''The first input json file''')
  parser.add_argument('title1', metavar="TITLE1",
                      help='''The title for input1's line''')
  parser.add_argument('input2', metavar="INPUT2",
                      help='''The second input json file''')
  parser.add_argument('title2', metavar="TITLE2",
                      help='''The title for input2's line''')
  args = parser.parse_args()

  basename = os.path.basename(args.input1)
  dirname = os.path.dirname(args.input1)
  gpi_filename = string.replace(dirname + "combined_" + basename, ".json", ".gpi")
  output_filename = string.replace(dirname + "combined_" + basename, ".json", ".pdf")

  graph_title = ""

  data_info_list = []
  for input_json, line_title in [(args.input1, args.title1), (args.input2, args.title2)]:
    dat_filename = string.replace(input_json, ".json", ".dat")
    stats = load_json(input_json)
    write_data_file(dat_filename, stats)
    data_info_list.append(DataInfo(title=line_title, filename=dat_filename))
    if 'prune_duration_seconds' in stats:
      graph_title += "%s runtime=%.1fs" % (line_title, stats['prune_duration_seconds'])

  write_gpi_template(gpi_filename, output_filename, data_info_list, graph_title)
  invoke_gnuplot(gpi_filename)

  print "Output placed in %s" % output_filename
