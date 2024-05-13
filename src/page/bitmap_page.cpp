#include "page/bitmap_page.h"

#include "glog/logging.h"

// 每个page的信息存储在一个bit中，page_offset是具体哪一位的索引，而存储结构是一个char数组
/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) { 
  if(IsPageFree(next_free_page_)){
    bytes[next_free_page_ / 8] |= (1 << (next_free_page_ % 8)); // 把指定位设为1
    page_offset = next_free_page_;
    page_allocated_++;
    uint32_t free_index = 0;
    while (!IsPageFree(free_index) && free_index < GetMaxSupportedSize() - 1)
    {
      free_index++;
    }
    next_free_page_ = free_index;
    return true;
  }
  return false;
}


/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if(IsPageFree(page_offset)){
    return false;
  }
  bytes[page_offset / 8] &= ~(1 << (page_offset % 8)); // 把指定位设为0
  page_allocated_--;
  if(page_offset < next_free_page_){
    next_free_page_ = page_offset;
  }
  return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  if (page_offset >= GetMaxSupportedSize())
  {
    LOG(ERROR) << "page_offset is out of range";
    return false;
  }
  return IsPageFreeLow(page_offset / 8, page_offset % 8);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return (bytes[byte_index] & (1 << bit_index)) == 0;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;

