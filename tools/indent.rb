#!/usr/bin/ruby

File.foreach(ARGV.shift) do |line|
  if line =~ /BEFORE/
    puts "  " + line
  elsif line =~ /AFTER/
    puts "  " + line
  elsif line =~ /schedule_new_message/ or line =~ /RAFT/
    puts
    puts line
    puts
  end
end
