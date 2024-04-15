#include <libpmem.h>
#include <libpmemobj.h>
#include <unistd.h>
#include <stdio.h>
#include <iostream>
#include <string>
#include <fstream>

#include "leveldb/export.h"
#include "leveldb/options.h"
#include "leveldb/status.h"
#include "db/version_edit.h"

#define PMEM_LEN  4294970
namespace leveldb{
class Pmem_Merge{
    private:

    public:
        const char * readData(int table_name,int choice,int filesize){
            char filename[12]; 
            sprintf(filename, "%05d.ldb", table_name);
            std::string table_path = "/mnt/pmem1/shanlicheng/mydb/leveldbtest-1004/dbbench/" + std::string(filename);
            std::ifstream filein(table_path);
            size_t mem_len = PMEM_LEN;
            if(filein.good()){
                std::fprintf(stdout,"compact file exists1\n");
                std::fprintf(stdout,"%s\n",table_path.c_str());
                
                char *pmem_addr = (char *) pmem_map_file(table_path.c_str(),PMEM_LEN, PMEM_FILE_CREATE, 0666, &(mem_len), NULL);
                char *return_data = new char[filesize];
                memcpy(return_data,pmem_addr,filesize);
                pmem_unmap(pmem_addr,PMEM_LEN);
                filein.close();
                fprintf(stdout,"file size %d\n",filesize);
                return return_data;
            }else{
                return nullptr;
            }
        }
        bool readData(int table_name,int choice,int filesize,char *data){
            char filename[12]; 
            sprintf(filename, "%05d.ldb", table_name);
            std::string table_path = "/mnt/pmem1/shanlicheng/mydb/leveldbtest-1004/dbbench/" + std::string(filename);
            std::ifstream filein(table_path);
            size_t mem_len = PMEM_LEN;
            if(filein.good()){
                std::fprintf(stdout,"compact file exists\n");
                std::fprintf(stdout,"%s\n",table_path.c_str());
                
                char *pmem_addr = (char *) pmem_map_file(table_path.c_str(),PMEM_LEN, PMEM_FILE_CREATE, 0666, &(mem_len), NULL);
                memcpy(data,pmem_addr,filesize);
                pmem_unmap(pmem_addr,PMEM_LEN);
                filein.close();
                fprintf(stdout,"file size %d\n",filesize);
                return true;
            }else{
                fprintf(stdout,"sst not exists\n");
                return false;
            }
        }
};
}