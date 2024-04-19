## SC-DB is a fast key-value storage system that uses NVM to improve database performance and takes a fine-grained compaction approach to reduce write amplification.



## Dependencies, Compilation and Running

1. ### Dependencies

   Hardware Dependencies

   ```
   [CPU]  64-core 2.30GHz Intel Xeon Gold 5218 CPU
   [DRAM] 128GB DDR4 2666MHz
   [NVM] 512GB Intel Optane DC PMEM *2
   ```

   Software Dependencies

   ```
   g++ 8.4.0
   cmake 3.21.0
   pmdk 2.0.1
   ubuntu 20.04
   ```

   

2. ### Compilation 

   For running SC-DB, the entire project needs to be compiled. We can build SC-DB via cmake. The command to compile the SC-DB:

   ```
   mkdir -p build && cd build
   cmake -DCMAKE_BUILD_TYPE=Release .. && cmake --build .
   ```

   

3. Running

   now the build/db_bench can be run.

```
./db_bench
./db_bench --benchmarks =fillrandom,readrandom,fillseq,readseq
```

​	  To run ycsb,you need the source project of YCSB-cpp

```
git clone https://github.com/ls4154/YCSB-cpp.git
cd YCSB-cpp
git submodule update --init
make

./ycsb -load -db leveldb -P workloads/workloada -P leveldb/leveldb.properties -s
```

​	 The Write Amplification and the Sensitivity Study needs to change the parameters of db_bench and YCSB-cpp.You can see the parameters in our paper and change them.



