
template = """
# Note you need gnuplot 4.4 for the pdfcairo terminal.

set terminal pdf font "Helvetica, 12" linewidth 4

# Line style for axes
set style line 80 lt rgb "#808080"

# Line style for grid
set style line 81 lt 0  # dashed
set style line 81 lt rgb "#808080"  # grey

set grid back linestyle 81
set border 3 back linestyle 80 # Remove border on top and right.  These
             # borders are useless and make it harder
             # to see plotted lines near the border.
    # Also, put it in grey; no need for so much emphasis on a border.
set xtics nomirror
set ytics nomirror

#set log x
#set mxtics 10    # Makes logscale look good.

# Line styles: try to pick pleasing colors, rather
# than strictly primary colors or hard-to-see colors
# like gnuplot's default yellow.  Make the lines thick
# so they're easy to see in small plots in papers.
set style line 3 lt rgb "#00A000" lw 2 pt 6
set style line 2 lt rgb "#A00000" lw 2 pt 1
set style line 1 lt rgb "#5060D0" lw 2 pt 2
set style line 4 lt rgb "#F25900" lw 2 pt 9

set key top right

set ylabel "Number of Remaining Events"
set xlabel "Number of Schedules Executed"

set yrange [0:]

# Note that we're leaving out output, title, and plot
#set output "runtime_graph.pdf"
#set title "runtime seconds=?, replay duration=?"
#plot 'foo.dat' index 0:1 title "" w lp ls 1
"""
