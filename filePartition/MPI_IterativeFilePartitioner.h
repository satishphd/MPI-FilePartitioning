#ifndef __MPI_ITERFILEPARTITIONER_H_INCLUDED__
#define __MPI_ITERFILEPARTITIONER_H_INCLUDED__

//#include "FilePartitioner.h"
#include "MPI_File_Partitioner.h"

#include <assert.h>
#include <stdio.h>
#include <mpi.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <math.h>

class MPI_IterativeFilePartitioner : public FilePartitioner
{
    //static const MPI_Offset overlap=1000*2000;
    //static const MPI_Offset FILE_SPLIT_SIZE = 1024*1024*1024;
    
    MPI_File mpi_layer1;
    MPI_File mpi_layer2;
    
    int rank;
    int MPI_Processes;
    
    MPI_Offset m_FileSplitSizeBytes;
    
    int handleLastIteration(MPI_File mpi_layer, MPI_Offset filesize);
               	
	public:
	MPI_IterativeFilePartitioner()
	{

	}
	
	MPI_File initializeLayer(Config &args);
	FileSplits* partitionLayer(MPI_File mpi_layer);
	
	//int initMPI(int argc, char **argv);
	
    int initialize(Config &args);
    pair<FileSplits*,FileSplits*> partition(); 
    
    void finalize();
    ~MPI_IterativeFilePartitioner()
    {
    }
    
};
#endif 