// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The representation of a DBImpl consists of a set of Versions.  The
// newest version is called "current".  Older versions may be kept
// around to provide a consistent view to live iterators.
//
// Each Version keeps track of a set of Table files per level.  The
// entire set of versions is maintained in a VersionSet.
//
// Version,VersionSet are thread-compatible, but require external
// synchronization on all accesses.

#ifndef STORAGE_LEVELDB_DB_VERSION_SET_H_
#define STORAGE_LEVELDB_DB_VERSION_SET_H_

#include <map>
#include <set>
#include <vector>

#include "db/dbformat.h"
#include "db/version_edit.h"
#include "port/port.h"
#include "port/thread_annotations.h"

//slc add
#include "table/pmem_merge.h"
#include "table/pmem_iterator.h"
#include "table/pmem.h"

namespace leveldb {

namespace log {
class Writer;
}

class Compaction;
class Iterator;
class MemTable;
class TableBuilder;
class TableCache;
class Version;
class VersionSet;
class WritableFile;

// Return the smallest index i such that files[i]->largest >= key.
// Return files.size() if there is no such file.
// REQUIRES: "files" contains a sorted list of non-overlapping files.
int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files, const Slice& key);

// Returns true iff some file in "files" overlaps the user key range
// [*smallest,*largest].
// smallest==nullptr represents a key smaller than all keys in the DB.
// largest==nullptr represents a key largest than all keys in the DB.
// REQUIRES: If disjoint_sorted_files, files[] contains disjoint ranges
//           in sorted order.
bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const std::vector<FileMetaData*>& files,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key);

class Version {
 public:
  struct GetStats {
    FileMetaData* seek_file;
    int seek_file_level;
  };

  // Append to *iters a sequence of iterators that will
  // yield the contents of this Version when merged together.
  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  void AddIterators(const ReadOptions&, std::vector<Iterator*>* iters);

  // Lookup the value for key.  If found, store it in *val and
  // return OK.  Else return a non-OK status.  Fills *stats.
  // REQUIRES: lock is not held
  Status Get(const ReadOptions&, const LookupKey& key, std::string* val,
             GetStats* stats);

  // Adds "stats" into the current state.  Returns true if a new
  // compaction may need to be triggered, false otherwise.
  // REQUIRES: lock is held
  bool UpdateStats(const GetStats& stats);

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.  Returns true if a new compaction may need to be triggered.
  // REQUIRES: lock is held
  bool RecordReadSample(Slice key);

  // Reference count management (so Versions do not disappear out from
  // under live iterators)
  void Ref();
  void Unref();

  void GetOverlappingInputs(
      int level,
      const InternalKey* begin,  // nullptr means before all keys
      const InternalKey* end,    // nullptr means after all keys
      std::vector<FileMetaData*>* inputs);

  // Returns true iff some file in the specified level overlaps
  // some part of [*smallest_user_key,*largest_user_key].
  // smallest_user_key==nullptr represents a key smaller than all the DB's keys.
  // largest_user_key==nullptr represents a key largest than all the DB's keys.
  bool OverlapInLevel(int level, const Slice* smallest_user_key,
                      const Slice* largest_user_key);

  // Return the level at which we should place a new memtable compaction
  // result that covers the range [smallest_user_key,largest_user_key].
  int PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                 const Slice& largest_user_key);

  int NumFiles(int level) const { return files_[level].size(); }

  // Return a human readable string that describes this version's contents.
  std::string DebugString() const;

    //slc add
  std::vector<FileMetaData*> GetL0Files(){
    return files_[0];
  }
 private:
  friend class Compaction;
  friend class VersionSet;

  //slc add
  friend class BlockCompaction;

  class LevelFileNumIterator;

  explicit Version(VersionSet* vset)
      : vset_(vset),
        next_(this),
        prev_(this),
        refs_(0),
        file_to_compact_(nullptr),
        file_to_compact_level_(-1),
        compaction_score_(-1),
        compaction_level_(-1) {}

  Version(const Version&) = delete;
  Version& operator=(const Version&) = delete;

  ~Version();

  Iterator* NewConcatenatingIterator(const ReadOptions&, int level) const;

  // Call func(arg, level, f) for every file that overlaps user_key in
  // order from newest to oldest.  If an invocation of func returns
  // false, makes no more calls.
  //
  // REQUIRES: user portion of internal_key == user_key.
  void ForEachOverlapping(Slice user_key, Slice internal_key, void* arg,
                          bool (*func)(void*, int, FileMetaData*));


  VersionSet* vset_;  // VersionSet to which this Version belongs
  Version* next_;     // Next version in linked list
  Version* prev_;     // Previous version in linked list
  int refs_;          // Number of live refs to this version

  // List of files per level
  std::vector<FileMetaData*> files_[config::kNumLevels];

  // Next file to compact based on seek stats.
  FileMetaData* file_to_compact_;
  int file_to_compact_level_;

  // Level that should be compacted next and its compaction score.
  // Score < 1 means compaction is not strictly needed.  These fields
  // are initialized by Finalize().
  double compaction_score_;
  int compaction_level_;

  //slc add memtable vector
  //std::vector<MemTable> flushed_tables;
};

class VersionSet {
 public:
  VersionSet(const std::string& dbname, const Options* options,
             TableCache* table_cache, const InternalKeyComparator*);
  VersionSet(const VersionSet&) = delete;
  VersionSet& operator=(const VersionSet&) = delete;

  ~VersionSet();

  // Apply *edit to the current version to form a new descriptor that
  // is both saved to persistent state and installed as the new
  // current version.  Will release *mu while actually writing to the file.
  // REQUIRES: *mu is held on entry.
  // REQUIRES: no other thread concurrently calls LogAndApply()
  Status LogAndApply(VersionEdit* edit, port::Mutex* mu)
      EXCLUSIVE_LOCKS_REQUIRED(mu);

  // Recover the last saved descriptor from persistent storage.
  Status Recover(bool* save_manifest);

  // Return the current version.
  Version* current() const { return current_; }

  // Return the current manifest file number
  uint64_t ManifestFileNumber() const { return manifest_file_number_; }

  // Allocate and return a new file number
  uint64_t NewFileNumber() { return next_file_number_++; }

  // Arrange to reuse "file_number" unless a newer file number has
  // already been allocated.
  // REQUIRES: "file_number" was returned by a call to NewFileNumber().
  void ReuseFileNumber(uint64_t file_number) {
    if (next_file_number_ == file_number + 1) {
      next_file_number_ = file_number;
    }
  }

  // Return the number of Table files at the specified level.
  int NumLevelFiles(int level) const;

  // Return the combined file size of all files at the specified level.
  int64_t NumLevelBytes(int level) const;

  // Return the last sequence number.
  uint64_t LastSequence() const { return last_sequence_; }

  // Set the last sequence number to s.
  void SetLastSequence(uint64_t s) {
    assert(s >= last_sequence_);
    last_sequence_ = s;
  }

  // Mark the specified file number as used.
  void MarkFileNumberUsed(uint64_t number);

  // Return the current log file number.
  uint64_t LogNumber() const { return log_number_; }

  // Return the log file number for the log file that is currently
  // being compacted, or zero if there is no such log file.
  uint64_t PrevLogNumber() const { return prev_log_number_; }

  // Pick level and inputs for a new compaction.
  // Returns nullptr if there is no compaction to be done.
  // Otherwise returns a pointer to a heap-allocated object that
  // describes the compaction.  Caller should delete the result.
  Compaction* PickCompaction();

  // Return a compaction object for compacting the range [begin,end] in
  // the specified level.  Returns nullptr if there is nothing in that
  // level that overlaps the specified range.  Caller should delete
  // the result.
  Compaction* CompactRange(int level, const InternalKey* begin,
                           const InternalKey* end);

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t MaxNextLevelOverlappingBytes();

  // Create an iterator that reads over the compaction inputs for "*c".
  // The caller should delete the iterator when no longer needed.
  Iterator* MakeInputIterator(Compaction* c);

  // Returns true iff some level needs a compaction.
  bool NeedsCompaction() const {
    Version* v = current_;
    return false;
    //return (v->compaction_score_ >= 1) || (v->file_to_compact_ != nullptr);
  }

  // Add all files listed in any live version to *live.
  // May also mutate some internal state.
  void AddLiveFiles(std::set<uint64_t>* live);

  // Return the approximate offset in the database of the data for
  // "key" as of version "v".
  uint64_t ApproximateOffsetOf(Version* v, const InternalKey& key);

  // Return a human-readable short (single-line) summary of the number
  // of files per level.  Uses *scratch as backing store.
  struct LevelSummaryStorage {
    char buffer[100];
  };
  const char* LevelSummary(LevelSummaryStorage* scratch) const;

 private:
  class Builder;

  friend class Compaction;
  friend class Version;
  //slc add
  friend class BlockCompaction;

  bool ReuseManifest(const std::string& dscname, const std::string& dscbase);

  void Finalize(Version* v);

  void GetRange(const std::vector<FileMetaData*>& inputs, InternalKey* smallest,
                InternalKey* largest);

  void GetRange2(const std::vector<FileMetaData*>& inputs1,
                 const std::vector<FileMetaData*>& inputs2,
                 InternalKey* smallest, InternalKey* largest);

  void SetupOtherInputs(Compaction* c);

  // Save current contents to *log
  Status WriteSnapshot(log::Writer* log);

  void AppendVersion(Version* v);

  Env* const env_;
  const std::string dbname_;
  const Options* const options_;
  TableCache* const table_cache_;
  const InternalKeyComparator icmp_;
  uint64_t next_file_number_;
  uint64_t manifest_file_number_;
  uint64_t last_sequence_;
  uint64_t log_number_;
  uint64_t prev_log_number_;  // 0 or backing store for memtable being compacted

  // Opened lazily
  WritableFile* descriptor_file_;
  log::Writer* descriptor_log_;
  Version dummy_versions_;  // Head of circular doubly-linked list of versions.
  Version* current_;        // == dummy_versions_.prev_

  // Per-level key at which the next compaction at that level should start.
  // Either an empty string, or a valid InternalKey.
  std::string compact_pointer_[config::kNumLevels];

  //slc add
};

// A Compaction encapsulates information about a compaction.
class Compaction {
 public:
  ~Compaction();

  // Return the level that is being compacted.  Inputs from "level"
  // and "level+1" will be merged to produce a set of "level+1" files.
  int level() const { return level_; }

  // Return the object that holds the edits to the descriptor done
  // by this compaction.
  VersionEdit* edit() { return &edit_; }

  // "which" must be either 0 or 1
  int num_input_files(int which) const { return inputs_[which].size(); }

  // Return the ith input file at "level()+which" ("which" must be 0 or 1).
  FileMetaData* input(int which, int i) const { return inputs_[which][i]; }

  // Maximum size of files to build during this compaction.
  uint64_t MaxOutputFileSize() const { return max_output_file_size_; }

  // Is this a trivial compaction that can be implemented by just
  // moving a single input file to the next level (no merging or splitting)
  bool IsTrivialMove() const;

  // Add all inputs to this compaction as delete operations to *edit.
  void AddInputDeletions(VersionEdit* edit);

  // Returns true if the information we have available guarantees that
  // the compaction is producing data in "level+1" for which no data exists
  // in levels greater than "level+1".
  bool IsBaseLevelForKey(const Slice& user_key);

  // Returns true iff we should stop building the current output
  // before processing "internal_key".
  bool ShouldStopBefore(const Slice& internal_key);

  // Release the input version for the compaction, once the compaction
  // is successful.
  void ReleaseInputs();

 private:
  friend class Version;
  friend class VersionSet;

  Compaction(const Options* options, int level);

  int level_;
  uint64_t max_output_file_size_;
  Version* input_version_;
  VersionEdit edit_;

  // Each compaction reads inputs from "level_" and "level_+1"
  std::vector<FileMetaData*> inputs_[2];  // The two sets of inputs

  // State used to check for number of overlapping grandparent files
  // (parent == level_ + 1, grandparent == level_ + 2)
  std::vector<FileMetaData*> grandparents_;
  size_t grandparent_index_;  // Index in grandparent_starts_
  bool seen_key_;             // Some output key has been seen
  int64_t overlapped_bytes_;  // Bytes of overlap between current output
                              // and grandparent files

  // State for implementing IsBaseLevelForKey

  // level_ptrs_ holds indices into input_version_->levels_: our state
  // is that we are positioned at one of the file ranges for each
  // higher level than the ones involved in this compaction (i.e. for
  // all L >= level_ + 2).
  size_t level_ptrs_[config::kNumLevels];
};

class BlockCompaction{
  private:
    friend class Version;
    friend class VersionSet;
    VersionEdit edit_;
    VersionSet vest_;
    InternalKeyComparator internal_comp;
  public:
    std::pair<std::vector<FileMetaData *>,int> PickCompactionBlock(VersionSet *vest_){
      const Comparator* internal_comp = vest_->icmp_.user_comparator();
      Version *current_ = vest_->current();
      std::vector<FileMetaData *> level0 = current_->files_[0];
      std::vector<FileMetaData *> level1 = current_->files_[1];

      std::vector<FileMetaData *> dumpcompact;
      std::vector<FileMetaData *> sstcompact;
      std::vector<FileMetaData *> blockcompact;
      for(auto it : level0){
        int flag = 1;
        
        for(auto it1 : level1){
          int compare_small_small = internal_comp->Compare(it->smallest.user_key(),it1->smallest.user_key());
          int compare_small_large = internal_comp->Compare(it->smallest.user_key(),it1->largest.user_key());
          int compare_large_small = internal_comp->Compare(it->largest.user_key(),it1->smallest.user_key());
          int compare_large_large = internal_comp->Compare(it->largest.user_key(),it1->largest.user_key());
          if(((compare_small_small >=0) &&(compare_small_large<=0)) || ((compare_large_small>=0) &&(compare_large_large<=0)) ||((compare_small_small<0) &&(compare_large_large>0))){
              flag=0;
              blockcompact.push_back(it1);
            }
          if((compare_small_small >=0) &&(compare_small_large<=0) && (compare_large_small>=0) &&(compare_large_large<=0)){
            //如果有完全包含的，直接整体合并
            sstcompact.push_back(it);
            sstcompact.push_back(it1);
          }
        }

        if(flag==1){
          //如果有直接可以插入的，直接插入
          level1.push_back(it);
          dumpcompact.push_back(it);
        }
      }
      for(auto it:level0){
        fprintf(stdout,"v0 %.*s ",int(it->smallest.user_key().size()),it->smallest.user_key().data());
        fprintf(stdout,"%.*s\n",int(it->largest.user_key().size()),it->largest.user_key().data());
        fprintf(stdout,"%d %d \n",int(it->block_indexes.size()),int(it->block_indexes.capacity()));
      }
      for(auto it:level1){
        fprintf(stdout,"v1 %.*s ",int(it->smallest.user_key().size()),it->smallest.user_key().data());
        fprintf(stdout,"%.*s\n",int(it->largest.user_key().size()),it->largest.user_key().data());
      }
      if(dumpcompact.size()>0){
        return std::make_pair(dumpcompact,1);
      }
      if(sstcompact.size()>0){
        fprintf(stdout,"sst %.*s\n",int(sstcompact[0]->smallest.user_key().size()),sstcompact[0]->smallest.user_key().data());
        return std::make_pair(sstcompact,2);
      }
      else{
        return std::make_pair(blockcompact,3);
      }
    }
    std::vector<FileMetaData *> PickL0CompactionBlock(VersionSet *vest_){
      //L1 have no file
      const Comparator* internal_comp = vest_->icmp_.user_comparator();
      Version *current_ = vest_->current();
      //compacts = new std::set<std::pair<FileMetaData *,int>>;
      std::vector<FileMetaData *> level0 = current_->files_[0];
      std::vector<FileMetaData *> level1 = current_->files_[1];

      if(level1.size()==0){
        FileMetaData *last = level0.back();
        level1.push_back(last);
      }

      for(auto it : level0){
        int flag = 1;
        for(auto it1 : level1){
          int compare_small_small = internal_comp->Compare(it->smallest.user_key(),it1->smallest.user_key());
          int compare_small_large = internal_comp->Compare(it->smallest.user_key(),it1->largest.user_key());
          int compare_large_small = internal_comp->Compare(it->largest.user_key(),it1->smallest.user_key());
          int compare_large_large = internal_comp->Compare(it->largest.user_key(),it1->largest.user_key());
          if((compare_small_small >=0) &&(compare_small_large<=0)){
              flag=0;
            }
          if((compare_large_small>=0) &&(compare_large_large<=0)){
                flag=0;
              }
          }
          if(flag==1){
            level1.push_back(it);
          }
      }
      return level1;
    }
    std::pair<char **,std::vector<int>> DoBlockCompactionWork(VersionSet *vest_,std::vector<uint64_t> *remove_vector){
     /* const Comparator* internal_comp = vest_->icmp_.user_comparator();

      std::vector<FileMetaData *> level0 = vest_->current()->files_[0];
      std::vector<FileMetaData *> level1 = vest_->current()->files_[1];
      
      std::vector<FileMetaData *> sst_compacts;
      FileMetaData *original_meta = level0[0];
      sst_compacts.push_back(original_meta);
      for(auto it1: level1){
          int compare_small_small = internal_comp->Compare(original_meta->smallest.user_key(),it1->smallest.user_key());
          int compare_small_large = internal_comp->Compare(original_meta->smallest.user_key(),it1->largest.user_key());
          int compare_large_small = internal_comp->Compare(original_meta->largest.user_key(),it1->smallest.user_key());
          int compare_large_large = internal_comp->Compare(original_meta->largest.user_key(),it1->largest.user_key());

          if(compare_small_small<=0 && compare_large_large>=0){
            //L0 包含 L1
            sst_compacts.push_back(it1);
            continue;
          }
          if(compare_small_small<=0 && compare_large_small>=0){
            sst_compacts.push_back(it1);
            continue;
          }
          if(compare_small_large<=0 && compare_large_large>=0){
            sst_compacts.push_back(it1);
            continue;
          }
      }
      assert(sst_compacts.size()>1);
      Pmem_Merge *merge = new Pmem_Merge();
      char **data = new char*[sst_compacts.size()];
      std::vector<Pmem_Iterator> iterators;
      char **sst_data = new char*[sst_compacts.size()];
      std::vector<int> sst_offset;
      int now_sst=0;

      for(int i=0; i<sst_compacts.size();i++){
        data[i] = new char[int(sst_compacts[i]->file_size)];
        bool result = merge->readData(sst_compacts[i]->number,1,sst_compacts[i]->file_size,data[i]);
        iterators.emplace_back(Pmem_Iterator(data[i],int(sst_compacts[i]->file_size)));
        sst_data[i] = new char[4294970];
        sst_offset.push_back(0);
        remove_vector->push_back(sst_compacts[i]->number);

      }
      int end_flag=0;
      while (true){
        for(int i=0;i<iterators.size();i++){
          if(iterators[i].end()){
            end_flag++;
            iterators.erase(iterators.begin() + i);
          }
        }
        if(end_flag==sst_compacts.size()) break;
        Slice small = iterators[0].Key();
        int small_index=0;
        for(int i=0;i<iterators.size();i++){
          ParsedInternalKey prased1;
          ParseInternalKey(small,&prased1);
          ParsedInternalKey prased2;
          ParseInternalKey(iterators[i].Key(),&prased2);
          int cmp = internal_comp->Compare(prased1.user_key,prased2.user_key);
          if(cmp==0){
            if(prased1.sequence == prased2.sequence){
                //同一个key，不操作
            }else if(prased1.sequence > prased2.sequence){
                //key相等，舍弃小的prased2
                iterators[i].next();
            }else{
                //key相等，舍弃小的prased1
                small = iterators[i].Key();
                iterators[small_index].next();
                small_index = i;
            }
          }if(cmp >0){
            //更新最小key
            small = iterators[i].Key();
            small_index = i;
          }
        }
        if(sst_offset[now_sst]+iterators[small_index].entry_size() > 4294970){
            now_sst++;
            fprintf(stdout,"%d %d\n",now_sst,sst_offset.size());
        }
        //ParsedInternalKey prased;
        //ParseInternalKey(small,&prased);
        //fprintf(stdout,"a %.*s\n",prased.user_key.size(),prased.user_key);
        memcpy(sst_data[now_sst]+sst_offset[now_sst],iterators[small_index].data_split(),iterators[small_index].entry_size());
        fprintf(stdout,"%d\n",sst_offset[now_sst]);
        sst_offset[now_sst] = sst_offset[now_sst]+iterators[small_index].entry_size();
        iterators[small_index].next();
      }
      return std::make_pair(sst_data,sst_offset);*/
    }
    std::pair<std::pair<char *,char *>,std::pair<int,int>> DoSSTCompaction(VersionSet *vest_,std::vector<FileMetaData *> *compacts){
      const Comparator* internal_comp = vest_->icmp_.user_comparator();
      int file1 = (*compacts)[0]->number;
      int filesize1 = (*compacts)[0]->file_size;
      int filesize2 = (*compacts)[1]->file_size;
      int file2 = (*compacts)[1]->number;
      
      Pmem_Merge *merge = new Pmem_Merge();
      const char * sst_iterator1 = merge->readData(file1,1,filesize1);
      const char * sst_iterator2 = merge->readData(file2,1,filesize2);
      int offset1=0,offset2=0;

      FileMetaData meta1,meta2;
      char * sst_compact = new char[4294970];
      char * sst_compact2 = new char[4294970];
      int sst_offset2=0;

      int sst_offset = 0;
      while (offset1<filesize1 && offset2<filesize2){
          int key_size1,key_size2,value_size1,value_size2;
          memcpy(&key_size1,sst_iterator1+offset1,sizeof(int));
          memcpy(&key_size2,sst_iterator2+offset2,sizeof(int));

          memcpy(&value_size1,sst_iterator1+offset1+sizeof(int),sizeof(int));
          memcpy(&value_size2,sst_iterator2+offset2+sizeof(int),sizeof(int));
          char *buf1 = new char[key_size1];
          char *buf2 = new char[key_size2];
          memcpy(buf1,sst_iterator1+offset1+sizeof(int)*2,key_size1);
          memcpy(buf2,sst_iterator2+offset2+sizeof(int)*2,key_size2);
          Slice key1 = Slice(buf1,key_size1);
          Slice key2 = Slice(buf2,key_size2);
          ParsedInternalKey prased1;
          ParseInternalKey(key1,&prased1);
          ParsedInternalKey prased2;
          ParseInternalKey(key2,&prased2);
          /*fprintf(stdout,"%d ",key_size1);
          fprintf(stdout,"%d ",value_size1);
          fprintf(stdout,"%.*s\n",int(prased1.user_key.size()),prased1.user_key.data());*/
          int comp = internal_comp->Compare(prased1.user_key,prased2.user_key);
          if(((sst_offset+sizeof(int)+key_size1+value_size1)>4294970) || (sst_offset+sizeof(int)+key_size2+value_size2)>4294970){
              memcpy(sst_compact2,sst_compact,4294970);
              sst_compact = new char[4294970];
              sst_offset2 = sst_offset;
              sst_offset = 0;
          }
          if(comp==0){
            if(prased1.sequence<prased2.sequence){
              memcpy(sst_compact+sst_offset,sst_iterator1+offset1,sizeof(int)*2+key_size1+value_size1);
              offset1 = offset1+sizeof(int)*2+key_size1+value_size1;
              offset2 = offset2+sizeof(int)*2+key_size2+value_size2;
              sst_offset = sst_offset+sizeof(int)*2+key_size1+value_size1;
            }else{
              memcpy(sst_compact+sst_offset,sst_iterator2+offset2,sizeof(int)*2+key_size2+value_size2);
              offset1 = offset1+sizeof(int)*2+key_size1+value_size1;
              offset2=offset2+sizeof(int)*2+key_size2+value_size2;
              sst_offset = sst_offset +sizeof(int)*2+key_size2+value_size2;
            }
          }
          else if(comp<0){
              memcpy(sst_compact+sst_offset,sst_iterator1+offset1,sizeof(int)*2+key_size1+value_size1);
              offset1 = offset1+sizeof(int)*2+key_size1+value_size1;
              sst_offset = sst_offset+sizeof(int)*2+key_size1+value_size1;
          }else{
              memcpy(sst_compact+sst_offset,sst_iterator2+offset2,sizeof(int)*2+key_size2+value_size2);
              offset2=offset2+sizeof(int)*2+key_size2+value_size2;
              sst_offset = sst_offset +sizeof(int)*2+key_size2+value_size2;
          }
      }
      return std::make_pair(std::make_pair(sst_compact2,sst_compact),std::make_pair(sst_offset2,sst_offset));
    }
    
    bool FinishCompaction(const char *data,int offset,FileMetaData *meta){
      int this_offset=0,last_offset=0,tmp_offset=0;
      std::vector<BlockOffset> blocks;
      Slice small;
      Pmem_TableBuilder pmem_builder(meta->number);
      pmem_builder.init();
      while(this_offset<offset){
        int key_size,value_size;
        memcpy(&key_size,data+this_offset,sizeof(int));
        memcpy(&value_size,data+this_offset+sizeof(int),sizeof(int));
        char *buf = new char[key_size];
        char *buf_value = new char[value_size];
        memcpy(buf,data+this_offset+sizeof(int)*2,key_size);
        memcpy(buf_value,data+this_offset+sizeof(int)*2,key_size);
        Slice key = Slice(buf,key_size);
        Slice value = Slice(buf_value,value_size);
        pmem_builder.Add(key,value);
        if(last_offset == 0){
          small = Slice(buf,key_size);
        }
        this_offset = this_offset+key_size+value_size+sizeof(int)*2;
        last_offset = last_offset+key_size+value_size+sizeof(int)*2;
        if(last_offset>4*1024){
          meta->block_indexes.emplace_back(small,Slice(buf,key_size),tmp_offset,last_offset);
          tmp_offset = this_offset;
          last_offset = 0;
        }
      }
      pmem_builder.Finish();
      meta->file_size = offset;
      meta->smallest.DecodeFrom(meta->block_indexes[0].block_small_key);
      meta->largest.DecodeFrom(meta->block_indexes.back().block_large_key);
      return true;
    }
    VersionEdit edit(){
        return edit_;
    }
};
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_SET_H_
