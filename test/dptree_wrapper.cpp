#include "dptree_wrapper.hpp"

extern "C" tree_api* create_tree(const tree_options_t& opt)
{
    return new dptree_wrapper();
}