#!/usr/bin/env ruby

# Must be invoked from the top-level sts2-applications directory.
#
# Given the directory containing an unmodified execution trace, find all
# directories containing MCSes for that unmodified execution trace.
# Combine their minimization_stats.json data into a single graph.

if ARGV.length != 1
  $stderr.puts "Usage: #{$0} /path/to/experiment"
  exit(1)
end

basename = File.basename(ARGV[0])
dirname = File.dirname(ARGV[0])

args = []
Dir.chdir dirname do
  mcs_dirs = Dir.glob("#{basename}*").select do |name|
    name != basename and File.directory? name
  end

  mcs_dirs.each do |mcs_dir|
    title = mcs_dir.gsub(/#{basename}_/, '')
    path = File.absolute_path(mcs_dir) + "/minimization_stats.json"
    args << path
    args << title
  end
end

system("interposition/src/main/python/minimization_stats/combine_graphs.py #{args.join(' ')}")
