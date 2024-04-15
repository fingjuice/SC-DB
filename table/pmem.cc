#include "pmem.h"
#include <fstream>
#include <iostream>
#include <iomanip>
#include <string>
#include <exception>
#include "db/dbformat.h"

namespace leveldb{
    void Pmem_TableBuilder :: Add(const Slice& key, const Slice& value){
        int key_size = key.size();
        int value_size = value.size();
        memcpy(addr+offset,&key_size,sizeof(int));
        memcpy(addr+offset+sizeof(int),&value_size,sizeof(int));
        memcpy(addr+offset+sizeof(int)*2,key.data(),key.size());
        memcpy(addr+offset+key_size+sizeof(int)*2,value.data(),value.size());

        offset += sizeof(int)*2+(key.size()+value.size());
        block_size += sizeof(int)*2+(key.size()+value.size());
    }
    void Pmem_TableBuilder :: init(){
        char filename[12]; 
        sprintf(filename, "%05d.ldb", this->table_name);
        std::string table_path = "/mnt/pmem1/shanlicheng/mydb/leveldbtest-1004/dbbench/" + std::string(filename);
        std::ifstream filein(table_path);
        if(filein.good()){
            std::fprintf(stdout,"file exists\n");
        }else{
            pmem_addr = (char *) pmem_map_file(table_path.c_str(), PMEM_LEN, PMEM_FILE_CREATE | PMEM_FILE_EXCL, 0666, &(mem_len), NULL);
        }
    }
    void Pmem_TableBuilder :: Flush(FileMetaData* meta,const Slice &smallkey,const Slice &key){
        meta->block_indexes.emplace_back(smallkey,key,block_offset,block_size);
        block_offset = offset;
        block_size = 0;
    }
    void Pmem_TableBuilder :: Finish(){
        memcpy(pmem_addr,addr,offset);
        pmem_unmap(pmem_addr,mem_len);
    }
    int Pmem_TableBuilder :: get_file_size(){
        return offset;
    }
    int Pmem_TableBuilder :: get_block_offset(){
        return block_size;
    }
}