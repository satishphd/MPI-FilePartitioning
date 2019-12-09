#include "filePartition/MPI_FilePartition_ByComm.h"

class FileSplits;

void MPI_FilePartition_ByComm :: finalize() 
{
    MPI_File_close(&mpi_layer1);
    //MPI_File_close(&mpi_layer2);
}

int MPI_FilePartition_ByComm :: initialize(Config &args) 
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
    
    // int ierr2 = MPI_File_open(MPI_COMM_WORLD, layer2, MPI_MODE_RDONLY, myinfo, &mpi_layer2);
    
    // if (ierr1 || ierr2) 
    if (ierr1)
    {
        if (rank == 0) 
         cout<<" Couldn't open file 1\n"<<endl;
        
        MPI_Finalize();
        exit(2);
    }
    
    return 0;
}

MPI_File MPI_FilePartition_ByComm :: initializeLayer(Config &args) 
{
    rank = args.rank;

    MPI_Processes = args.numProcesses;
    
    m_FileSplitSizeBytes = args.blockSize*1024*1024;
    
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


pair<FileSplits*,FileSplits*> MPI_FilePartition_ByComm :: partition()
{
   FileSplits* splitLayer1 = partitionLayer(mpi_layer1); 
   
   //FileSplits* splitLayer2 = partitionLayer(mpi_layer2); 

   //pair<FileSplits*, FileSplits*> p(splitLayer1, splitLayer2);
   pair<FileSplits*, FileSplits*> p(splitLayer1, NULL);

   return p;
}

 // 1438 % 256 = 158 (for last iteration
 // Remaining File Size 165667342 
FileSplits* MPI_FilePartition_ByComm :: partitionLayer(MPI_File mpi_layer) 
{
    MPI_Offset filesize;
    
    MPI_Offset start;
    MPI_Offset end;
    char *chunk;

    /* figure out who reads what */
   // cout<<"File size "<<m_FileSplitSizeBytes<<endl;
    
    MPI_File_get_size(mpi_layer, &filesize);
    
    //printf("File size %lld block-size %lld \n",filesize, m_FileSplitSizeBytes);
    
    MPI_Offset globalStartOfset = 0;
    
    MPI_Offset perProcessBytes = m_FileSplitSizeBytes/MPI_Processes;
    
    double dIterations = filesize/(double)m_FileSplitSizeBytes;
    int numIterations = (int)ceil(dIterations);
    
    if(rank == 0) {
      printf("#Iterations %d file size %lld\n", numIterations, filesize);
      fflush(stdout);
    }
    
    // except last iteration
    for(int i = 0; i < (numIterations-1); i++)
    {     
        // if rank is 0 and iterations 1 onwards 
        //     start offset is preset at the bottom of this for loop
        if(rank == 0 && i == 0)
        {
           start = 0;
        }
        else 
        {
          globalStartOfset = i * m_FileSplitSizeBytes;
    	  start = globalStartOfset + rank * perProcessBytes;
        }
        // what if iteration = 1,2,.. but rank is zero
        
        end   = start + perProcessBytes;
        
    	MPI_Offset actualPerProcessBytes =  end - start;

    	//cerr<<rank<<" 1st : "<<start<<" , "<<end<<endl;
		//    	printf("P[%d], iter->%d, start %lld, end %lld, bytes %lld \n", rank, i, start, end, actualPerProcessBytes);
		//    	fflush(stdout);
    
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
    	
    	printf("P[%d], iter->%d, start %lld, end %lld, bytes %d \n", rank, i, start, end, count);
	   	fflush(stdout);
    	
    	fixBrokenLinesAcrossBoundaries(chunk, actualPerProcessBytes+1);
    	
    	free(chunk);	
    }
    
    // handle last iteration
    handleLastIteration(mpi_layer, filesize);
    
    return NULL;
 }
 
 int MPI_FilePartition_ByComm :: handleLastIteration(MPI_File mpi_layer, MPI_Offset filesize)
 {
		MPI_Offset remainingFileSize = filesize % m_FileSplitSizeBytes;
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
    	
    	printf("P[%d], last iter, start %lld, end %lld, bytes %lld \n", rank, start, end,
	   																 actualPerProcessBytes);
   		fflush(stdout);
   		
    	fixBrokenLinesAcrossBoundaries(chunk, actualPerProcessBytes+1);
    	
    	free(chunk);
    	
    	return 0;
 }

 
 int MPI_FilePartition_ByComm ::fixBrokenLinesAcrossBoundaries(char *chunk, int localsize)
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
        // MPI_Send(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm)

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

char* MPI_FilePartition_ByComm :: concat(const char *s1, const char *s2)
{
    const size_t len1 = strlen(s1);
    const size_t len2 = strlen(s2);
    char *result = (char *)malloc(len1+len2+1);//+1 for the null-terminator
    //in real code you would check for errors in malloc here
    memcpy(result, s1, len1);
    memcpy(result+len1, s2, len2+1);//+1 to copy the null-terminator
    return result;
}
