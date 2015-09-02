#!/usr/bin/env ruby

File.foreach(ARGV.shift) do |line|
  puts line
  puts
end
