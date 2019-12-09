#ifndef __MPI_FILEPARTITIONBYCOMM_H_INCLUDED__
#define __MPI_FILEPARTITIONBYCOMM_H_INCLUDED__

//#include "FilePartitioner.h"
#include "MPI_File_Partitioner.h"

#include <assert.h>
#include <stdio.h>
#include <mpi.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <math.h>

class MPI_FilePartition_ByComm : public FilePartitioner
{
    //static const MPI_Offset overlap = 10*1024*1024; // 10 MB;
    //static const MPI_Offset FILE_SPLIT_SIZE = 1024*1024*1024; // 1 GB
    //static const MPI_Offset FILE_SPLIT_SIZE = 256*1024*1024; // 256 MB 
    	
    MPI_File mpi_layer1;
    MPI_File mpi_layer2;
    
    int rank;
    int MPI_Processes;
    
    MPI_Offset m_FileSplitSizeBytes;
    
    int handleLastIteration(MPI_File mpi_layer, MPI_Offset filesize);
    int fixBrokenLinesAcrossBoundaries(char *chunk, int size);           	
    
	public:
	MPI_FilePartition_ByComm()
	{

	}
	
	char* concat(const char *s1, const char *s2);
	
	MPI_File initializeLayer(Config &args);
	FileSplits* partitionLayer(MPI_File mpi_layer);
	
	//int initMPI(int argc, char **argv);
	
    int initialize(Config &args);
    pair<FileSplits*,FileSplits*> partition(); 
    
    void finalize();
    ~MPI_FilePartition_ByComm()
    {
    }
    
};
#endif 