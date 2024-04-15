#include <unistd.h>
#include <stdio.h>
#include <iostream>
#include <string>
#include <fstream>

#include "leveldb/export.h"
#include "leveldb/options.h"
#include "leveldb/status.h"
#include "db/version_edit.h"
namespace leveldb{
    class Pmem_Iterator{
        private:
            char *data;
            int filesize;
            int key_size;
            int value_size;
            char *buf;
            int offset=0;
        public:
        Pmem_Iterator(){}
        Pmem_Iterator(char *data,int filesize){
            this->data = data;
            this->filesize = filesize;
        }
        Slice first(){
            memcpy(&key_size,data,sizeof(int));
            memcpy(&value_size,data+sizeof(int),sizeof(int));
            buf = new char[key_size];
            memcpy(buf,data+sizeof(int)*2,key_size);
            Slice first = Slice(buf,key_size);
            return first;
        }
        bool next(){
            offset = offset+key_size+value_size+sizeof(int)*2;
        }
        Slice Key(){
            fprintf(stdout,"s s %d %d\n",offset,filesize);
            memcpy(&key_size,data+offset,sizeof(int));
            memcpy(&value_size,data+offset+sizeof(int),sizeof(int));
            buf = new char[key_size];
            memcpy(buf,data+offset+sizeof(int)*2,key_size);
            Slice key = Slice(buf,key_size);
            fprintf(stdout,"key,%.*s ",key.size(),key.data());
            fprintf(stdout," %d\n",offset);
            return key;
        }
        bool end(){
            return offset>filesize;
        }
        char* data_split(){
            char *return_data = new char[sizeof(int)*2+key_size+value_size];
            memcpy(return_data,data+offset,sizeof(int)*2+key_size+value_size);
            return return_data;
        }
        int entry_size(){
            return sizeof(int)*2+key_size+value_size;
        }
    };
}