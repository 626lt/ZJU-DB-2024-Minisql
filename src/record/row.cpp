#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  uint32_t offset = 0;
  // write Field Nums
  uint32_t fields_num = GetFieldCount();
  memcpy(buf + offset, &fields_num, sizeof(uint32_t));
  offset += sizeof(uint32_t);
  // write Null bitmaps
  uint32_t null_size = (fields_num + 7) / 8;
  char *null_bitmap = new char[null_size];
  memset(null_bitmap, 0, null_size);
  for (uint32_t i = 0; i < fields_num; i++) {
    if (fields_[i]->IsNull()) {
      null_bitmap[i / 8] |= (1 << (i % 8));
    }
  }
  memcpy(buf + offset, null_bitmap, null_size);
  offset += null_size;
  delete[] null_bitmap;
  // write Fields
  for (uint32_t i = 0; i < fields_num; i++) {
    if (!fields_[i]->IsNull()) {
      offset += fields_[i]->SerializeTo(buf + offset);
    }
  }
  return offset; 
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  // replace with your code here
  uint32_t offset = 0;
  // read Field Nums
  uint32_t fields_num;
  memcpy(&fields_num, buf + offset, sizeof(uint32_t));
  offset += sizeof(uint32_t);
  // read Null bitmaps
  uint32_t null_size = (fields_num + 7) / 8;
  char *null_bitmap = new char[null_size];
  memcpy(null_bitmap, buf + offset, null_size);
  offset += null_size * sizeof(char);
  // read Fields
  for (uint32_t i = 0; i < fields_num; i++) {
    TypeId type = schema->GetColumn(i)->GetType();
    Field *field = new Field(type);
    if (null_bitmap[i / 8] & (1 << (i % 8))) {
      offset += Field::DeserializeFrom(buf + offset, type, &field, true);
    } else {
      offset += Field::DeserializeFrom(buf + offset, type, &field, false);
    }
    fields_.push_back(field);
    // the following code is wrong, it will cause segment fault
    // if (null_bitmap[i / 8] & (1 << (i % 8))) {
    //   offset += Field::DeserializeFrom(buf + offset, schema->GetColumn(i)->GetType(), &fields_[i], true);
    // } else {
    //   offset += Field::DeserializeFrom(buf + offset, schema->GetColumn(i)->GetType(), &fields_[i], false);
    // }
  }
  return offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  uint32_t size = 0;
  size += sizeof(uint32_t); // Field Nums
  size += (GetFieldCount() + 7) / 8; // Null bitmaps
  for (uint32_t i = 0; i < GetFieldCount(); i++) {
    if (!fields_[i]->IsNull()) {
      size += fields_[i]->GetSerializedSize();
    }
  }
  return size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
