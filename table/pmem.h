#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <libpmem.h>
#include <libpmemobj.h>

#include "leveldb/export.h"
#include "leveldb/options.h"
#include "leveldb/status.h"
#include "db/version_edit.h"

#define PMEM_LEN  4294970 // 持久内存区域的大小

namespace leveldb{
    class Pmem_TableBuilder{
        private:
            PMEMobjpool *pop;
            int table_name;
            char *addr = new char[PMEM_LEN];
            char *pmem_addr;
            int offset = 0;
            Slice smallest_key;
            int block_size=0;
            int block_offset=0;
            size_t mem_len = PMEM_LEN;
        public:
            Pmem_TableBuilder(int table_name){
                this->table_name = table_name;
            }
            Pmem_TableBuilder(){
            }
            void init();
            void Add(const Slice& key, const Slice& value);
            void Flush(FileMetaData* meta,const Slice &smallkey,const Slice &key);
            void Finish();
            int get_file_size();
            int get_block_offset();
    };
}