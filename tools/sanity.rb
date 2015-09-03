#!/usr/bin/env ruby

require 'set'

original = Set.new
pruned = Set.new
first_line = true

File.foreach(ARGV.shift) do |line|
  events = line.chomp.split(", ")
  if first_line
    original += events
    first_line = false
  else
    pruned += events
  end
end

(original - pruned).each do |not_pruned|
  puts not_pruned
end
