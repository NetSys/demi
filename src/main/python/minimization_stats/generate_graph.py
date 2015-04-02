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

# TODO(cs): should use matplotlib instead of the template
from gpi_template import template
import string
import argparse
import json
import os
from collections import namedtuple

def write_data_file(dat_filename, stats):
  ''' Write out the datapoints '''
  sorted_keys = stats["iteration_size"].keys()
  sorted_keys.sort(lambda a,b: -1 if int(a) < int (b) else 1 if int(a) > int(b) else 0)
  with open(dat_filename, "w") as dat:
    for key in sorted_keys:
      dat.write(str(key) + " " + str(stats["iteration_size"][key]) + '\n')

def load_json(json_input):
  with open(json_input) as json_input_file:
    return json.load(json_input_file)

DataInfo = namedtuple('DataInfo', ['filename', 'title'])

def write_gpi_template(gpi_filename, output_filename, data_info_list, xmax=None, title=""):
  with open(gpi_filename, "w") as gpi:
    # Finish off the rest of the template
    gpi.write(template)
    if title != "":
      gpi.write('''set title "%s"\n''' % title)
    gpi.write('''set output "%s"\n''' % output_filename)
    if xmax != None:
      gpi.write('''set xrange [0:%d]\n''' % xmax)
    gpi.write('''plot ''')
    first_iteration = True
    for i, data_info in enumerate(data_info_list):
      gpi.write('''"%s" index 0:1 title "%s" with steps ls %d''' %
                (data_info.filename, data_info.title, i+1))
      if i != len(data_info_list) - 1:
        gpi.write(", \\\n")
      else:
        gpi.write("\n")

def invoke_gnuplot(gpi_filename):
  os.system("gnuplot %s" % gpi_filename)

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description="generate a plot")
  parser.add_argument('input', metavar="INPUT",
                      help='''The input json file''')
  parser.add_argument('-x', '--xmax', type=int,
                      help='''Truncate the x dimension''')

  args = parser.parse_args()

  stats = load_json(args.input)

  gpi_filename = string.replace(args.input, ".json", ".gpi")
  output_filename = string.replace(args.input, ".json", ".pdf")
  dat_filename = string.replace(args.input, ".json", ".dat")

  write_data_file(dat_filename, stats)
  data_info_list = [DataInfo(title="", filename=dat_filename)]
  xmax = args.xmax if hasattr(args, "xmax") else None
  write_gpi_template(gpi_filename, output_filename, data_info_list, xmax=xmax)
  invoke_gnuplot(gpi_filename)
