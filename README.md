# MPI-FilePartitioning
Partitioning large files using MPI

This experimental code-base is meant to enable parallel reading of a large file using MPI. The assumption is that a file is a collection of records which are separated by newline character or XML-like tags that denotes end of record. This user requirement is heavily influenced by what Hadoop Distributed File System (HDFS) provides by design: each process/thread should read a subset of the records completely i.e. a record should not get broken between two consecutive threads/processes while doing a parallel read operation.

There are two ways in which an MPI process (among N processes) can read N/P chunk of arbitrary text file of size N stored in disk:
1) Overlapping parallel reads by consecutive MPI processes.
2) Non-overlapping parallel reads, followed by a cyclic communication by MPI processes.

More details are in the following paper: 
MPI-Vector-IO: Parallel I/O and Partitioning for Geospatial Vector Data. International Conference on Parallel Processing (ICPP), 2018.
