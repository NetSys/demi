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

# Returns:
# hashmap:
# {
#    { iteration -> iteration size }
#    { internal_iteration -> iteration size }
# }
def interpolate_datapoints(stats):
  # Two tasks:
  #  - String all of the inner json objects into a single hashmap of datapoints
  #  - If there is a gap in iteration sizes, fill it in with the previous
  #    value
  # TODO(cs): visually delineate the different stages of minimization
  return {
    "iteration_size": get_external_progress(stats),
    "internal_iteration_size": get_internal_progress(stats)
  }

def get_internal_progress(stats):
  ret = {}
  iteration = 0

  # Provenance isn't applied in all cases, so use original
  # provenance = stats[1]
  orig = stats[1]
  ret[iteration] = int(orig["minimized_deliveries"])
  iteration += 1

  for obj in stats[2:]:
    sorted_keys = sorted(map(int, obj["internal_iteration_size"].keys()))

    if (len(sorted_keys) == 0):
      # This wasn't an internal minimization run. The only (monotonic) progress that would
      # have been made would be at the very end, where the external event MCS
      # was verified, and there were absent expected events.
      # TODO(cs): probably off by one on the x-axis
      for i in range(obj["total_replays"]-1):
        ret[iteration] = ret[iteration-1]
        iteration += 1
      ret[iteration] = obj["minimized_deliveries"]
      iteration += 1
      continue

    # Smallest index so far for this object:
    bottom = 1
    for k in sorted_keys:
      while bottom < k:
        ret[iteration] = ret[iteration-1]
        iteration += 1
        bottom += 1
      ret[iteration] = int(obj["internal_iteration_size"][str(k)])
      iteration += 1
      bottom += 1

  return ret

def get_external_progress(stats):
  ret = {}
  iteration = 0

  # Get initial iteration_size from first DDMin run.
  # ["minimized_externals" does not include Start events]
  lowest_key = str(map(int, sorted(stats[2]["iteration_size"].keys()))[0])
  ret[iteration] = stats[2]["iteration_size"][lowest_key]
  iteration += 1

  for obj in stats[2:]:
    sorted_keys = sorted(map(int, obj["iteration_size"].keys()))

    if (len(sorted_keys) == 0):
      # This wasn't an external minimization run, so no external progress was
      # made
      for i in range(obj["total_replays"]):
        ret[iteration] = ret[iteration-1]
        iteration += 1
      continue

    # Smallest index so far for this object:
    bottom = 1
    for k in sorted_keys:
      while bottom < k:
        ret[iteration] = ret[iteration-1]
        iteration += 1
        bottom += 1
      ret[iteration] = int(obj["iteration_size"][str(k)])
      iteration += 1
      bottom += 1
  return ret

def write_data_file(dat_filename, stats):
  ''' Write out the datapoints. Return the maximum x-value '''
  xmax = 0
  datapoints = interpolate_datapoints(stats)
  for t in ["internal_iteration_size", "iteration_size"]:
    sorted_keys = datapoints[t].keys()
    sorted_keys.sort(lambda a,b: -1 if int(a) < int(b) else 1 if int(a) > int(b) else 0)
    if int(sorted_keys[-1]) > xmax:
      xmax = int(sorted_keys[-1])
    with open(dat_filename + "_" + t, "w") as dat:
      for key in sorted_keys:
        dat.write(str(key) + " " + str(datapoints[t][key]) + '\n')
  return xmax

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
    line_type_counter = 1
    for i, data_info in enumerate(data_info_list):
      first_t = True
      for t,title in [("internal_iteration_size", "Deliveries"),
                      ("iteration_size", "Externals")]:
        expanded_title = title if data_info.title == "" else data_info.title + "_" + title
        gpi.write('''"%s" index 0:1 title "%s" with steps ls %d''' %
                  (data_info.filename + "_" + t, expanded_title,
                   line_type_counter))
        line_type_counter += 1
        if first_t:
          gpi.write(", \\\n")
          first_t = False
        elif i != len(data_info_list) - 1:
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

  max_x_value = write_data_file(dat_filename, stats)
  data_info_list = [DataInfo(title="", filename=dat_filename)]
  xmax = args.xmax
  if (xmax == None):
    xmax = max_x_value
  write_gpi_template(gpi_filename, output_filename, data_info_list, xmax=xmax)
  invoke_gnuplot(gpi_filename)
