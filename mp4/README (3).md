# MP4

## Getting started

The input directory is the MP3_FILE folder. If that doesn't exist, then create a directory called MP3_FILE in the root directory. Put csv or txt files separated by commas into MP3_FILE and append "input_file_" prefix to file names of all input files. 

ie. employees.txt becomes "input_file_employees.txt"

First, start the failure detector on VM1 by running

```
python3 introducer.py
```

Then, run the server file on all VM hosts using 

```
python3 server.py
```

Filter tasks can be run like this:

```
maple maple_filter.py 5 filtered MP3_FILE -reg Melissa -matchAttrib None
```
maple [executable] [numTasks] [intermed_prefix] [input_dir] [regex]

Juice Tasks can be run like this:

```
juice juice_filter.py 5 filtered result.txt 0 -hash hash
```

juice [executable] [numTasks] [intermed_prefix] [result file name] [delete_intermed_file] [partition_type]

delete intermed_file is set to 0 (don't delete) and 1 (delete) after maple juice finishes.
partition_type is set to -hash hash, or -range range

Join tasks can be run like this: (join value hardcoded like specified in instruction)

```
maple maple_join.py 5 joined MP3_FILE -matchAttrib None
```
maple [executable] [numTasks] [intermed_prefix] [input_dir]


```
juice juice_join.py 9 joined result222.txt 0 -hash hash
```
juice [executable] [numTasks] [intermed_prefix] [result file name] [delete_intermed_file] [partition_type]

