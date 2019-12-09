#ifndef __FILEPARTITIONER_H_INCLUDED__
#define __FILEPARTITIONER_H_INCLUDED__

#include "config.h"
#include <list>
#include <string>
#include <tuple>

#include "FileSplits.h"

using namespace std;

class FilePartitioner 
{
	int numPartitions;
	int numLines; // lines in input file
    list<string> files;
	
	public:
	
	static const MPI_Offset OVERLAP= 1*1024*1024; // 10 MB;
    //static const MPI_Offset FILE_SPLIT_SIZE = 1024*1024*1024;

    //static const MPI_Offset BLOCK_SIZE = 54*1024*1024; // 64 MB
    //static const MPI_Offset FILE_SPLIT_SIZE = 256*1024*1024; // 256 MB 
    
	virtual pair<FileSplits*,FileSplits*> partition() = 0; 
	
	void setPartition(int num)
	{
    	numPartitions = num;
	}
	
	virtual int initialize(Config &args) = 0;
	virtual void finalize() = 0;
	virtual ~FilePartitioner()
	{
	}
};

#endif 