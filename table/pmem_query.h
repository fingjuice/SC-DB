#include <libpmem.h>
#include <libpmemobj.h>
#include <unistd.h>
#include <stdio.h>
#include <iostream>
#include <string>

#include "leveldb/export.h"
#include "leveldb/options.h"
#include "leveldb/status.h"
#include "db/version_edit.h"

#define PMEM_LEN  4294970
namespace leveldb{
class Pmem_Query{
    private:
        PMEMobjpool *pop;
        int query_block_offset;
        int next_block_offset;
        char *pmem_addr;
        size_t mem_len = PMEM_LEN;
        
    public:
    bool init(int file_number,int block_offset,int next_block_offset){
        char table_name[12];
        sprintf(table_name,"%05d.ldb",file_number);
        std::string dbpath = "/mnt/pmem1/shanlicheng/mydb/leveldbtest-1004/dbbench/";
        std::string table_path = dbpath+std::string(table_name);
        if (access(table_path.c_str(), F_OK) != -1) {
            pmem_addr = (char *) pmem_map_file(table_path.c_str(), PMEM_LEN, PMEM_FILE_CREATE, 0666, &(mem_len), NULL);
            this->query_block_offset = block_offset;
            this->next_block_offset = next_block_offset;
            return true;
        } else {
            // 文件不存在
            std::fprintf(stdout,"query :FileMetaData%d文件不存在\n",file_number);
            return false;
        }
    }
    
    bool query_start(const Comparator* ucmp,const Slice &lookup_key,Slice &lookup_value){
        /*PMEMoid root = pmemobj_root(pop, sizeof(char));
        char* root_ptr = static_cast<char*>(pmemobj_direct(root));
        char *addr = (char *)root_ptr;*/

        int combsize;
        int offset = query_block_offset;
        while(offset < next_block_offset){
            memcpy(&combsize,pmem_addr+offset,sizeof(int));

            int key_size = (combsize>>16) & 0xFFFF;
            int value_size = combsize & 0xFFFF;
            char* key = new char[key_size];
            char* value = new char[value_size];

            memcpy(key,pmem_addr+sizeof(int)+offset,key_size);
            memcpy(value,pmem_addr+sizeof(int)+key_size+offset,value_size);

            Slice key_slice = Slice(key,key_size);
            Slice val_slice = Slice(value,value_size);
            
            ParsedInternalKey prased_internal_key;
            ParseInternalKey(key_slice,&prased_internal_key);
            
            if(ucmp->Compare(prased_internal_key.user_key,lookup_key)==0){
                //查询到key-value
                lookup_value = val_slice;
                pmem_unmap(pmem_addr,mem_len);
                return true;
            }
            offset = offset + sizeof(int) + key_size + value_size;
        }
        // 关闭持久内存池
        //pmemobj_close(pop);
        return false;
    }
};
}//end leveldb