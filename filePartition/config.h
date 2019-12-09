#ifndef __CONFIG_H_INCLUDED__
#define __CONFIG_H_INCLUDED__

#include<string>
#include<vector>
#include <cstdlib>
#include <dirent.h>
#include <mpi.h>
#include <iostream>

using namespace std;

class Config
{
  std::vector<std::string> *layer1;
  std::vector<std::string> *layer2;
  
  
  public:
   int rank;
   int numPartitions;
   int numProcesses;
   MPI_Offset blockSize; // in MB
   
   string log_file;
  
  vector<std::string>* getFilesFromDir(char *directory);
  
  Config(int argc, char **argv)
  {
    blockSize = atoi(argv[1]);  
    numPartitions = atoi(argv[2]);
  
	layer1 = getFilesFromDir(argv[3]);
	layer2 = getFilesFromDir(argv[4]);	
	
	if(argc == 5)
	  log_file = argv[4];  //logfile
	else
	  log_file = "dummy";  //logfile
  }

  vector<string>* getLayer1();
  vector<string>* getLayer2(); 
  
  int initMPI(int argc, char **argv);
};

#endif