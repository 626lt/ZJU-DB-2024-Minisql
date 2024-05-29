#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  // | SCHEMA_MAGIC_NUM 4 | column size 4 | column1 | column2 | ... |
  uint32_t offset = 0;
  memcpy(buf + offset, &SCHEMA_MAGIC_NUM, sizeof(SCHEMA_MAGIC_NUM));
  offset += sizeof(SCHEMA_MAGIC_NUM);
  uint32_t column_size = columns_.size();
  memcpy(buf + offset, &column_size, sizeof(column_size));
  offset += sizeof(column_size);
  for (auto column : columns_) {
    offset += column->SerializeTo(buf + offset);
  }
  return offset;
}

uint32_t Schema::GetSerializedSize() const {
  // | SCHEMA_MAGIC_NUM 4 | column size 4 | column1 | column2 | ... |
  uint32_t size = sizeof(SCHEMA_MAGIC_NUM) + sizeof(uint32_t);
  for (auto column : columns_) {
    size += column->GetSerializedSize();
  }
  return size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  uint32_t offset = 0;
  uint32_t magic_num;
  memcpy(&magic_num, buf + offset, sizeof(magic_num));
  offset += sizeof(magic_num);
  ASSERT(magic_num == SCHEMA_MAGIC_NUM, "Invalid magic number.");
  if (magic_num != SCHEMA_MAGIC_NUM) {
    return 0;
  }
  uint32_t column_size;
  memcpy(&column_size, buf + offset, sizeof(column_size));
  offset += sizeof(column_size);
  std::vector<Column *> columns;
  for (uint32_t i = 0; i < column_size; i++) {
    Column *temp = nullptr;
    offset += Column::DeserializeFrom(buf + offset, temp);
    columns.emplace_back(temp);
  }
  schema = new Schema(columns, true);
  return offset;
}