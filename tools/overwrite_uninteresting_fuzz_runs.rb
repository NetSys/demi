#!/usr/bin/ruby

outName = "console.out"
out = File.new(outName, "w")

ARGF.each do |line|
  out.puts line
  if line =~ /Trying random interleaving/
    out.close
    File.delete(outName)
    out = File.new(outName, "w")
  end
end
