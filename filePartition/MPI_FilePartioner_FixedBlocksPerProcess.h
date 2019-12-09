#ifndef __FIXEDBLOCKSPERPROCESS_H_INCLUDED__
#define __FIXEDBLOCKSPERPROCESS_H_INCLUDED__

#include "MPI_File_Partitioner.h"

#include <assert.h>
#include <stdio.h>
#include <mpi.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <math.h>

class MPI_FilePartioner_FixedBlocksPerProcess : public FilePartitioner
{

	MPI_File mpi_layer1;
    MPI_File mpi_layer2;
    
    int rank;
    int MPI_Processes;
    MPI_Offset m_BlockSizeBytes;
    
    int fixBrokenLinesAcrossBoundaries(char *chunk, int localsize);

    int handleLastIterByOverlap(MPI_File mpi_layer, MPI_Offset filesize);
    int handleLastIterByMessaging(MPI_File mpi_layer, MPI_Offset filesize);
    
   public:
	
	MPI_FilePartioner_FixedBlocksPerProcess()
	{
	}
	
	MPI_File initializeLayer(Config &args);
	FileSplits* partitionLayerByOverlap(MPI_File mpi_layer);
	FileSplits* partitionLayerByMessaging(MPI_File mpi_layer);
	
    int initialize(Config &args);

    pair<FileSplits*,FileSplits*> partition()
    {
	    pair<FileSplits*, FileSplits*> p(NULL, NULL);
    	return p;
    }
    
    pair<FileSplits*,FileSplits*> partitionByFileOverlap();
    pair<FileSplits*,FileSplits*> partitionByMessaging(); 
    
    void finalize();
    ~MPI_FilePartioner_FixedBlocksPerProcess()
    {
    }

};

#endif 
