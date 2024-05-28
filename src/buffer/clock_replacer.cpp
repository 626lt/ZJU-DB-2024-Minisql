#include "buffer/clock_replacer.h"

CLOCKReplacer::CLOCKReplacer(size_t num_pages){
    capacity = num_pages;
};

CLOCKReplacer::~CLOCKReplacer() = default;

bool CLOCKReplacer::Victim(frame_id_t *frame_id) {
  // if the clock_list is empty, return false
  if (clock_list.empty()) {
    return false;
  }
  // find the first frame that is not pinned
  while (clock_status[clock_list.front()] == 1) {
    clock_list.push_back(clock_list.front());
    clock_list.pop_front();
    clock_status[clock_list.back()] = 0;
  }
  // return the frame_id
  *frame_id = clock_list.front();
  clock_list.pop_front();
  clock_status.erase(*frame_id);
  return true;
}

void CLOCKReplacer::Pin(frame_id_t frame_id) {
  // if the frame is in the clock_list, then remove it from the clock_list
  if (clock_status.find(frame_id) != clock_status.end()) {
    clock_list.remove(frame_id);
    clock_status.erase(frame_id);
  }
}

void CLOCKReplacer::Unpin(frame_id_t frame_id) {
  // if the frame is not in the clock_list, then add it to the clock_list
  if (clock_status.find(frame_id) == clock_status.end()) {
    clock_list.push_back(frame_id);
    clock_status[frame_id] = 1;
  }else{
    clock_status[frame_id] = 1;
  }
}

size_t CLOCKReplacer::Size() {
  return clock_list.size();
}
