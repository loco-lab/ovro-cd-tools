# count-ovro-files
This tool searches for casa `ms` files with names like YYYYMMDD_TTTTTT_??MHz.ms by recursively visiting directories until it reaches a maximum depth. It will not search inside of any ms file it finds.

The tool outputs the counts of files for each day as a function of subband.