#include "tree_api.hpp"
#include "../include/concur_dptree.hpp"

#include <cstring>
#include <mutex>
#include <shared_mutex>
#include <libpmemobj.h>

// #define DEBUG_MSG
static thread_local std::vector<std::pair<uint64_t,uint64_t>> results;

class dptree_wrapper : public tree_api
{
public:
  dptree_wrapper();
    virtual ~dptree_wrapper();
    
    virtual bool find(const char* key, size_t key_sz, char* value_out) override;
    virtual bool insert(const char* key, size_t key_sz, const char* value, size_t value_sz) override;
    virtual bool update(const char* key, size_t key_sz, const char* value, size_t value_sz) override;
    virtual bool remove(const char* key, size_t key_sz) override;
    virtual int scan(const char* key, size_t key_sz, int scan_sz, char*& values_out) override;
    virtual long get_size() override;

private:
    dptree::concur_dptree<uint64_t, uint64_t> tree_;
};


dptree_wrapper::dptree_wrapper() : tree_("/mnt/pmem0/vogel/dptree.dat")
{
}

dptree_wrapper::~dptree_wrapper()
{
}

bool dptree_wrapper::find(const char* key, size_t key_sz, char* value_out)
{

  uint64_t value;
  bool found = tree_.lookup(*reinterpret_cast<uint64_t*>(const_cast<char*>(key)), value);
  memcpy(value_out, &value, sizeof(value));
  return found;
}


bool dptree_wrapper::insert(const char* key, size_t key_sz, const char* value, size_t value_sz)
{
  tree_.insert(*reinterpret_cast<uint64_t*>(const_cast<char*>(key)), *reinterpret_cast<uint64_t*>(const_cast<char*>(value)));
    return true;
}

bool dptree_wrapper::update(const char* key, size_t key_sz, const char* value, size_t value_sz)
{
  tree_.insert(*reinterpret_cast<uint64_t*>(const_cast<char*>(key)), *reinterpret_cast<uint64_t*>(const_cast<char*>(value)));
  return true;
}

bool dptree_wrapper::remove(const char* key, size_t key_sz)
{
}

int dptree_wrapper::scan(const char* key, size_t key_sz, int scan_sz, char*& values_out)
{
    results.clear();
    results.reserve(scan_sz);

    tree_.lookup_range(*reinterpret_cast<uint64_t*>(const_cast<char*>(key)), scan_sz, results);

    values_out = reinterpret_cast<char *>(results.data());
    return results.size();
}
long dptree_wrapper::get_size() {
  return tree_.allocator.pm_allocator->cur_pmem_size;
}
