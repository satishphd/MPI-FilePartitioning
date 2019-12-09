#include "filePartition/MPI_FilePartioner_FixedBlocksPerProcess.h"

class FileSplits;

void MPI_FilePartioner_FixedBlocksPerProcess :: finalize() 
{
    MPI_File_close(&mpi_layer1);
    //MPI_File_close(&mpi_layer2);
}

int MPI_FilePartioner_FixedBlocksPerProcess :: initialize(Config &args) 
{
    rank = args.rank;

    MPI_Processes = args.numProcesses;

    const char *layer1 = args.getLayer1()->at(2).c_str();
    
    //const char *layer2 = args.getLayer2()->at(2).c_str();
     
    // cout<<"MPI layer1 "<<layer1<<endl; 
	//     cout<<"MPI layer2 "<<layer2<<endl; 
    MPI_Info myinfo;
    MPI_Info_create(&myinfo);
    MPI_Info_set(myinfo, "access_style", "read_once,sequential"); 
    MPI_Info_set(myinfo, "collective_buffering", "true"); 
    MPI_Info_set(myinfo, "romio_cb_read", "enable");
 
    int ierr1 = MPI_File_open(MPI_COMM_WORLD, layer1, MPI_MODE_RDONLY, myinfo, &mpi_layer1);
    
    //int ierr2 = MPI_File_open(MPI_COMM_WORLD, layer2, MPI_MODE_RDONLY, myinfo, &mpi_layer2);
    
    //if (ierr1 || ierr2) 
    if (ierr1)
    {
        if (rank == 0) 
         cout<<" Couldn't open file 1\n"<<endl;
        
        MPI_Finalize();
        exit(2);
    }
    
    return 0;
}

MPI_File MPI_FilePartioner_FixedBlocksPerProcess :: initializeLayer(Config &args) 
{
    rank = args.rank;
    
    m_BlockSizeBytes = args.blockSize*1024*1024;
    
    MPI_Processes = args.numProcesses;

    const char *layer1 = args.getLayer1()->at(2).c_str();

    // cout<<"MPI layer1 "<<layer1<<endl; 

    MPI_Info myinfo;
    MPI_Info_create(&myinfo);
    MPI_Info_set(myinfo, "access_style", "read_once,sequential"); 
    MPI_Info_set(myinfo, "collective_buffering", "true"); 
    MPI_Info_set(myinfo, "romio_cb_read", "enable");
    
    /* you can add optimizations for write as well like "romio_cb_write" etc */
    
    int ierr1 = MPI_File_open(MPI_COMM_WORLD, layer1, MPI_MODE_RDONLY, myinfo, &mpi_layer1);
    //int ierr1 = MPI_File_open(MPI_COMM_WORLD, layer1, MPI_MODE_RDONLY, MPI_INFO_NULL, &mpi_layer1);
    
    if (ierr1)
    {
        if (rank == 0) 
         cout<<" Couldn't open file 1\n"<<endl;
        
        MPI_Finalize();
        exit(2);
    }
    
    return mpi_layer1;
}


pair<FileSplits*,FileSplits*> MPI_FilePartioner_FixedBlocksPerProcess :: partitionByFileOverlap()
{  
   FileSplits* splitLayer1 = partitionLayerByOverlap(mpi_layer1); 
   
   //FileSplits* splitLayer2 = partitionLayer(mpi_layer2); 

   //pair<FileSplits*, FileSplits*> p(splitLayer1, splitLayer2);
   pair<FileSplits*, FileSplits*> p(splitLayer1, NULL);

   return p;
}

pair<FileSplits*,FileSplits*> MPI_FilePartioner_FixedBlocksPerProcess :: partitionByMessaging()
{  
   FileSplits* splitLayer1 = partitionLayerByMessaging(mpi_layer1); 
   
   //FileSplits* splitLayer2 = partitionLayer(mpi_layer2); 

   //pair<FileSplits*, FileSplits*> p(splitLayer1, splitLayer2);
   pair<FileSplits*, FileSplits*> p(splitLayer1, NULL);

   return p;
}


FileSplits* MPI_FilePartioner_FixedBlocksPerProcess :: partitionLayerByMessaging(MPI_File mpi_layer) 
{
    MPI_Offset filesize;
    
    MPI_Offset start;
    MPI_Offset end;
    char *chunk;

    /* figure out who reads what */

    MPI_File_get_size(mpi_layer, &filesize);
    
    MPI_Offset globalStartOfset = 0;
    
    MPI_Offset fileChunkSize = MPI_Processes * m_BlockSizeBytes;
    
    
    double dIterations = filesize/(double)fileChunkSize;
    int numIterations = (int)ceil(dIterations);
    
    if(rank == 0) {
      printf("#Iterations %d file size %lld\n", numIterations, filesize);
      fflush(stdout);
    }
    
    // except last iteration
    for(int i = 0; i < (numIterations-1); i++)
    {     
        if(rank == 0 && i == 0)
        {
           start = 0;
        }
        else 
        {
          globalStartOfset = i * fileChunkSize;
    	  start = globalStartOfset + rank * m_BlockSizeBytes;
        }
        // what if iteration = 1,2,.. but rank is zero
          
        end   = start + m_BlockSizeBytes;

    	MPI_Offset actualPerProcessBytes =  end - start;

    	//cerr<<rank<<" 1st : "<<start<<" , "<<end<<endl;
    
    	/* allocate memory */
    	chunk = (char *)malloc((actualPerProcessBytes+1) * sizeof(char));

    	if(chunk == NULL) {
      		cerr<<"Error in malloc code 1 for chunk "<<endl;
      		return NULL;
   		}
   		
   		if(start < 0)
   		{
   		    printf("MPI-GIS error: File I/O Offset is negative \n");
   		    fflush(stdout);
   		    MPI_Abort(MPI_COMM_WORLD, 2);
   		}
        
        MPI_Status status;
    	/* everyone reads in their part */
    	MPI_File_read_at_all(mpi_layer, start, chunk, actualPerProcessBytes, MPI_CHAR, &status);
    	
    	int count;
    	MPI_Get_count(&status, MPI_CHAR, &count);
    	if(count != actualPerProcessBytes)
    	{
        	   printf("%d bytes read \n", count);
        	   fflush(stdout);
        }
    	chunk[actualPerProcessBytes] = '\0';
    	
    	printf("P[%d], iter->%d, start %lld, end %lld, bytes %lld : %d \n", rank, i, start, end, actualPerProcessBytes, count);
   		fflush(stdout);
		
		fixBrokenLinesAcrossBoundaries(chunk, actualPerProcessBytes+1);
    	
    	free(chunk);
    }
    
     // handle last iteration
    handleLastIterByMessaging(mpi_layer, filesize);
    
    return NULL;
}

int MPI_FilePartioner_FixedBlocksPerProcess ::fixBrokenLinesAcrossBoundaries(char *chunk, int localsize)
{
    
   MPI_Offset preTerminatorCharCount = localsize-1;
    
    //if(rank==0)
    //  printf("rank = %d localsize = %d locstart = %d locend = %d \n",rank, localsize, locstart, locend);
    
    // the loop assumes that it wont have to handle the last iteration
    int sendBufferSize = 0;
    while(preTerminatorCharCount >= 0 && chunk[preTerminatorCharCount] != '\n') 
    {
          preTerminatorCharCount--;
          sendBufferSize++;
    }
    preTerminatorCharCount--; // including "\n" character
    
    if(preTerminatorCharCount == 0)
    {
       cerr<<"Error: New line character \n not found "<<endl;
    }
    //char *send_buffer = new char[preTerminatorCharCount];
    //strncpy(send_buffer, preTerminatorCharCount, );
    
    if(rank == (MPI_Processes-1))
    {
        MPI_Send(chunk+preTerminatorCharCount, sendBufferSize, MPI_CHAR, 0, 111, MPI_COMM_WORLD);
    }
    else
    {
        //MPI_Send(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm)

		// printf("Rank#: %d Sending %d bytes \n",rank, sendBufferSize);
		// fflush(stdout);
        
        MPI_Send(chunk+preTerminatorCharCount, sendBufferSize, MPI_CHAR, rank+1, 111, MPI_COMM_WORLD);
    }
    
    MPI_Status status;
    int maxBufferSize = 10*1024*1024; // 11 MB
    char *recv_buffer = new char[maxBufferSize]; 
    
    if(rank == 0)
    {
       //MPI_Recv(&ch, 1, MPI_CHAR, (MPI_Processes-1) , 111, MPI_COMM_WORLD, &status);
       MPI_Recv(recv_buffer, maxBufferSize, MPI_CHAR, (MPI_Processes-1), 111, MPI_COMM_WORLD, &status);
    }
    else
    {
      
      //MPI_Recv(void *buf, int count, MPI_Datatype datatype, int source, int tag, MPI_Comm comm, MPI_Status *status)  
      MPI_Recv(recv_buffer, maxBufferSize, MPI_CHAR, rank-1, 111, MPI_COMM_WORLD, &status);
    }
    
    int receivedBytes;
    MPI_Get_count(&status, MPI_CHAR, &receivedBytes);
      
	//     printf("      Rank#: %d Received %d bytes \n",rank, receivedBytes);
	//     fflush(stdout);
      
    if(receivedBytes > 1)
	      recv_buffer[receivedBytes-1] = '\0';
      
      //int newSize = localsize + receivedBytes;
      //char *newBuffer = new char[newSize];
      
      //char *mergedString = concat(chunk, recv_buffer);
      
      //printf("RANK RANK %s \n\n\n", mergedString);
      //fflush(stdout);
    delete[] recv_buffer;
    return 0;
}

FileSplits* MPI_FilePartioner_FixedBlocksPerProcess :: partitionLayerByOverlap(MPI_File mpi_layer) 
{
    MPI_Offset filesize;
    
    MPI_Offset start;
    MPI_Offset end;
    char *chunk;

    /* figure out who reads what */

    MPI_File_get_size(mpi_layer, &filesize);
    
    MPI_Offset globalStartOfset = 0;
    
    MPI_Offset fileChunkSize = MPI_Processes * m_BlockSizeBytes;
    
    
    double dIterations = filesize/(double)fileChunkSize;
    int numIterations = (int)ceil(dIterations);
    
    if(rank == 0) {
      printf("#Exact Iterations:%f ceiling: %d file size %lld\n", dIterations, numIterations, filesize);
      fflush(stdout);
    }
    
    // except last iteration
    for(int i = 0; i < (numIterations-1); i++)
    {     
        if(rank == 0 && i == 0)
        {
           start = 0;
        }
        else 
        {
          globalStartOfset = i * fileChunkSize;
    	  start = globalStartOfset + rank * m_BlockSizeBytes;
        }
        // what if iteration = 1,2,.. but rank is zero
        
    	/* add overlap to the end of everyone's chunk... */         
        end   = start + m_BlockSizeBytes + OVERLAP;

    	MPI_Offset actualPerProcessBytes =  end - start;

    	//cerr<<rank<<" 1st : "<<start<<" , "<<end<<endl;
    
    	/* allocate memory */
    	chunk = (char *)malloc((actualPerProcessBytes+1) * sizeof(char));

    	if(chunk == NULL) {
      		cerr<<"Error in malloc code 1 for chunk "<<endl;
      		return NULL;
   		}
   		
   		if(start < 0)
   		{
   		    printf("MPI-GIS error: File I/O Offset is negative \n");
   		    fflush(stdout);
   		    MPI_Abort(MPI_COMM_WORLD, 2);
   		}
        
        MPI_Status status;
    	/* everyone reads in their part */
    	MPI_File_read_at_all(mpi_layer, start, chunk, actualPerProcessBytes, MPI_CHAR, &status);
    	
    	int count;
    	MPI_Get_count(&status, MPI_CHAR, &count);
    	if(count != actualPerProcessBytes)
    	{
        	   printf("%d bytes read \n", count);
        	   fflush(stdout);
        }
    	chunk[actualPerProcessBytes] = '\0';
    	
    	printf("P[%d], iter->%d, start %lld, end %lld, bytes %lld : %d \n", rank, i, start, end, actualPerProcessBytes, count);
   		fflush(stdout);

    	free(chunk);
    }
    // handle last iteration
    handleLastIterByOverlap(mpi_layer, filesize);

    return NULL;
 }
 
 int MPI_FilePartioner_FixedBlocksPerProcess :: handleLastIterByOverlap(MPI_File mpi_layer, MPI_Offset filesize)
 {
		MPI_Offset remainingFileSize = filesize % m_BlockSizeBytes;
    	MPI_Offset perProcessBytes = remainingFileSize/MPI_Processes;
        
		//         if(rank == 0)
		//         {
		//           printf("     Last iter Remaining File Size %lld \n", remainingFileSize);
		//           fflush(stdout);
		//         }
        
        MPI_Offset startForProcess0 = filesize - remainingFileSize;   
        MPI_Offset start = startForProcess0 + rank * perProcessBytes;
    
    	/* add overlap to the end of everyone's chunk... */         
        MPI_Offset end   = start + perProcessBytes + OVERLAP;
        
    	/* except the last processor, of course */
    	if (rank == (MPI_Processes-1)) 
    	     end = filesize;

    	MPI_Offset actualPerProcessBytes =  end - start;

    	//cerr<<rank<<" 1st : "<<start<<" , "<<end<<endl;
		// 	   	printf("P[%d], last iter, start %lld, end %lld, bytes %lld \n", rank, start, end,
		// 	   																 actualPerProcessBytes);
		//    		fflush(stdout);
    
    	/* allocate memory */
    	char *chunk = (char *)malloc((actualPerProcessBytes+1) * sizeof(char));

    	if(chunk == NULL) {
      		cerr<<"Error in malloc code 1 for chunk "<<endl;
      		return -1;
   		}
        
        MPI_Status status;
    	/* everyone reads in their part */
    	MPI_File_read_at_all(mpi_layer, start, chunk, actualPerProcessBytes, MPI_CHAR, &status);
    	chunk[actualPerProcessBytes] = '\0';
    	
    	int count;
    	MPI_Get_count(&status, MPI_CHAR, &count);
    	if(count != actualPerProcessBytes)
    	{
        	   printf("%d bytes read \n", count);
        	   fflush(stdout);
        }
        
    	printf("P[%d], last iter, start %lld, end %lld, bytes %lld : %d \n", rank, start, end, actualPerProcessBytes, count);
   		fflush(stdout);
   		
    	free(chunk);
    	
    	return 0;
 }
 
 int MPI_FilePartioner_FixedBlocksPerProcess :: handleLastIterByMessaging(MPI_File mpi_layer, MPI_Offset filesize)
 {
		MPI_Offset remainingFileSize = filesize % m_BlockSizeBytes;
    	MPI_Offset perProcessBytes = remainingFileSize/MPI_Processes;
        
		//         if(rank == 0)
		//         {
		//           printf("     Last iter Remaining File Size %lld \n", remainingFileSize);
		//           fflush(stdout);
		//         }
        
        MPI_Offset startForProcess0 = filesize - remainingFileSize;   
        MPI_Offset start = startForProcess0 + rank * perProcessBytes;
    
    	/* add overlap to the end of everyone's chunk... */         
        MPI_Offset end   = start + perProcessBytes;
        
    	/* except the last processor, of course */
    	if (rank == (MPI_Processes-1)) 
    	     end = filesize;

    	MPI_Offset actualPerProcessBytes =  end - start;

    	// cerr<<rank<<" 1st : "<<start<<" , "<<end<<endl;
		// printf("P[%d], last iter, start %lld, end %lld, bytes %lld \n", rank, start, end,
		// 	   																 actualPerProcessBytes);
		// fflush(stdout);
    
    	/* allocate memory */
    	char *chunk = (char *)malloc((actualPerProcessBytes+1) * sizeof(char));

    	if(chunk == NULL) {
      		cerr<<"Error in malloc code 1 for chunk "<<endl;
      		return -1;
   		}
        
        MPI_Status status;
    	/* everyone reads in their part */
    	MPI_File_read_at_all(mpi_layer, start, chunk, actualPerProcessBytes, MPI_CHAR, &status);
    	chunk[actualPerProcessBytes] = '\0';
    	
    	int count;
    	MPI_Get_count(&status, MPI_CHAR, &count);
    	if(count != actualPerProcessBytes)
    	{
        	   printf("%d bytes read \n", count);
        	   fflush(stdout);
        }
        
    	printf("P[%d], last iter, start %lld, end %lld, bytes %lld: count %d \n", rank, start, end,
	   																 actualPerProcessBytes, count);
   		fflush(stdout);
   		
    	fixBrokenLinesAcrossBoundaries(chunk, actualPerProcessBytes+1);
    	
    	free(chunk);
    	
    	return 0;
 }

