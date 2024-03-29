#include "filePartition/MPI_File_Partitioner.h"

class FileSplits;

void MPI_File_Partitioner :: finalize() 
{
    MPI_File_close(&mpi_layer1);
    MPI_File_close(&mpi_layer2);
}

int MPI_File_Partitioner :: initialize(Config &args) 
{
    rank = args.rank;

    MPI_Processes = args.numProcesses;

    cout<<"Before file access"<<endl;
    
    const char *layer1 = args.getLayer1()->at(2).c_str();
    
    const char *layer2 = args.getLayer2()->at(2).c_str();
     
     cout<<"MPI layer1 "<<layer1<<endl; 
     cout<<"MPI layer2 "<<layer2<<endl; 
    MPI_Info myinfo;
    MPI_Info_create(&myinfo);
    MPI_Info_set(myinfo, "access_style", "read_once,sequential"); 
    MPI_Info_set(myinfo, "collective_buffering", "true"); 
    MPI_Info_set(myinfo, "romio_cb_read", "enable");
 
    int ierr1 = MPI_File_open(MPI_COMM_WORLD, layer1, MPI_MODE_RDONLY, myinfo, &mpi_layer1);
    
    int ierr2 = MPI_File_open(MPI_COMM_WORLD, layer2, MPI_MODE_RDONLY, myinfo, &mpi_layer2);
    
    if (ierr1 || ierr2) 
    if (ierr1)
    {
        if (rank == 0) 
         cout<<" Couldn't open file 1\n"<<endl;
        
        MPI_Finalize();
        exit(2);
    }
    
    return 0;
}

MPI_File MPI_File_Partitioner :: initializeLayer(Config &args) 
{
    rank = args.rank;

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


pair<FileSplits*,FileSplits*> MPI_File_Partitioner :: partition()
{
   FileSplits* splitLayer1 = partitionLayer(mpi_layer1); 
   
   FileSplits* splitLayer2 = partitionLayer(mpi_layer2); 

   pair<FileSplits*, FileSplits*> p(splitLayer1, splitLayer2);
   //pair<FileSplits*, FileSplits*> p(splitLayer1, NULL);

   return p;
}
 
FileSplits* MPI_File_Partitioner :: partitionLayer(MPI_File mpi_layer) 
{
    MPI_Offset filesize;
    MPI_Offset localsize;
    MPI_Offset start;
    MPI_Offset end;
    char *chunk;

    /* figure out who reads what */

    MPI_File_get_size(mpi_layer, &filesize);
    
    if(rank == 0) {
      //cerr<<"FileSize "<<filesize<<endl;
      //fflush(stdout);
    }
    
    localsize = filesize/MPI_Processes;
    start = rank * localsize;
    end   = start + localsize - 1;

    /* add overlap to the end of everyone's chunk... */
    end += OVERLAP;

    /* except the last processor, of course */
    if (rank == MPI_Processes-1) end = filesize;

    localsize =  end - start + 1;

    //cerr<<rank<<" 1st : "<<start<<" , "<<end<<endl;
    //printf("P%d, 1st %lld, %lld, %lld \n", rank, start, end, localsize);
    //fflush(stdout);
    
    /* allocate memory */
    chunk = (char *)malloc((localsize+1) * sizeof(char));

    if(chunk == NULL) {
      cerr<<"Error in malloc code 1 for chunk "<<endl;
      return NULL;
    }
    
    /* everyone reads in their part */
    MPI_File_read_at_all(mpi_layer, start, chunk, localsize, MPI_CHAR, MPI_STATUS_IGNORE);
    chunk[localsize] = '\0';
   
    /*
     * everyone calculate what their start and end *really* are by going 
     * from the first newline after start to the first newline after the
     * overlap region starts (eg, after end - overlap + 1)
    */

    MPI_Offset locstart, locend;
    locstart = 0;
    locend = localsize;
    //cout<<rank<<" "<<locend<<endl; //<<locstart<<locend
    
    
    //if(rank==0)
    //  printf("rank = %d localsize = %d locstart = %d locend = %d \n",rank, localsize, locstart, locend);
    
    if (rank != 0) {
        while(chunk[locstart] != '\n') 
        locstart++;
        
        locstart++;
    }
    
    if (rank != MPI_Processes-1) {
        locend -= OVERLAP;
        while(locend < (localsize+1) && chunk[locend] != '\n') {
          locend++;
          //assert(locend < (localsize+2));
        }
    }
    localsize = locend-locstart+1;

    //fflush(stdout);    
    //cerr<<rank<<" 2nd : "<<locstart<<" , "<<locend<<endl;
    //printf("P%d, 2nd %lld : %lld\n", rank, locstart, locend);
    //fflush(stdout);
    
    //assert(locend<locstart);
    
    /* Now let's copy our actual data over into a new array, with no overlaps*/
    char *data = (char *)malloc((localsize+1)*sizeof(char));
    
    if(data == NULL) {
      cerr<<"P"<<rank<<" : Error in malloc code 2 for data "<<localsize<<" "<<locstart<<" "<<locend<<endl;
      
      return NULL;
    }
    
    //memcpy(data, &(chunk[locstart]), localsize);
    for(int i = 0; i< localsize; i++)
    {
       data[i] = chunk[i];
    }
    
    data[localsize] = '\0';
    free(chunk);

    FileSplits *splits = new FileSplits();
    splits->write(data);
    
    free(data);
    
    //cout<<"Returning from partionLayer"<<endl;
    return splits;    
}
