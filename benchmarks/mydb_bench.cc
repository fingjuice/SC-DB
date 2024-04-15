#include <sys/types.h>
#include <string>
#include <iostream>
#include <chrono>
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/write_batch.h"
namespace leveldb{
    class KeyBuffer {
 public:
    KeyBuffer() {
        assert(FLAGS_key_prefix < sizeof(buffer_));
        memset(buffer_, 'a', FLAGS_key_prefix);
    }
    KeyBuffer& operator=(KeyBuffer& other) = delete;
    KeyBuffer(KeyBuffer& other) = delete;

    void Set(int k) {
        std::snprintf(buffer_ + FLAGS_key_prefix,
                    sizeof(buffer_) - FLAGS_key_prefix, "%016d", k);
    }

    Slice slice() const { return Slice(buffer_, FLAGS_key_prefix + 16); }

    private:
    char buffer_[1024];
    int FLAGS_key_prefix = 3;
};

    class myBenckmark{
        public:
        myBenckmark():db_(nullptr){
        }
        ~myBenckmark(){
            delete db_;
        }
        void Open(){
            //key-value pair insert
            leveldb::Env* g_env = leveldb::Env::Default();
            Options options;
            options.create_if_missing = true;
            std::string default_db_path = "/mnt/pmem1/shanlicheng/mydb/leveldbtest-1004/dbbench";
            Status s = DB::Open(options, default_db_path.c_str(), &db_);
            if (!s.ok()) {
            std::cerr << "Unable to open database: " << s.ToString() << std::endl;
            }
        }
        void SeqWrite(){
            WriteOptions write_options_;
            KeyBuffer key;
            WriteBatch batch;
            for(int i=20000; i< 3000000; i++){
                    key.Set(i);
                    batch.Put(key.slice(),Slice("test",5));
                if(i%1000 == 0){
                    Status s = db_->Write(write_options_ , &batch);
                    batch.Clear();
                }
            }
        }
        void Query(){
            std::string data;
            ReadOptions read_options_;
            KeyBuffer key;
            int count = 0;
            auto start_time = std::chrono::high_resolution_clock::now();
            for(int i=20000;i<3000000;i++){
                key.Set(i);
                Status result = db_->Get(read_options_,key.slice(),&data);
                if(result.ok()){
                    count++;
                }
            }
            auto end_time = std::chrono::high_resolution_clock::now();
            auto total_time =  std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
            double micros_per_op = static_cast<double>(total_time.count()) / 10000;
            fprintf(stdout,"%dfound  %f micros/op \n",count,micros_per_op);
            //if(result.ok())
        }
        private:
        DB *db_;
    };
}
int main(){
    
    leveldb::myBenckmark benchmark;
    benchmark.Open();
    //benchmark.SeqWrite();
    benchmark.Query();
}