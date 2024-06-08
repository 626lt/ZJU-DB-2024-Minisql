#ifndef MINISQL_LOG_REC_H
#define MINISQL_LOG_REC_H

#include <unordered_map>
#include <utility>

#include "common/config.h"
#include "common/rowid.h"
#include "record/row.h"

enum class LogRecType {
    kInvalid,
    kInsert,
    kDelete,
    kUpdate,
    kBegin,
    kCommit,
    kAbort,
};

// used for testing only
using KeyType = std::string;
using ValType = int32_t;

/**
 * TODO: Student Implement
 */
struct LogRec {
    LogRec() = default;

    LogRec(LogRecType type, lsn_t lsn, lsn_t prev_lsn, txn_id_t txn_id) : 
    type_(type), lsn_(lsn), prev_lsn_(prev_lsn),txn_id_(txn_id) {}

    LogRecType type_{LogRecType::kInvalid};
    lsn_t lsn_{INVALID_LSN};
    lsn_t prev_lsn_{INVALID_LSN};
    txn_id_t txn_id_{INVALID_TXN_ID};

    /* used for testing only */
    static std::unordered_map<txn_id_t, lsn_t> prev_lsn_map_;
    static lsn_t next_lsn_;

    static lsn_t find_the_log(txn_id_t txn_id, lsn_t lsn) {
        auto iter = prev_lsn_map_.find(txn_id);
        auto prev_lsn = INVALID_LSN;
        if(iter != prev_lsn_map_.end()) {
            prev_lsn = iter->second;
            iter->second = lsn;
        }else{
            prev_lsn_map_.emplace(txn_id, lsn);
        }
        return prev_lsn;
    }

    KeyType target_key_{};
    ValType target_val_{};

    KeyType new_key_{};
    ValType new_val_{};

};

std::unordered_map<txn_id_t, lsn_t> LogRec::prev_lsn_map_ = {};
lsn_t LogRec::next_lsn_ = 0;

typedef std::shared_ptr<LogRec> LogRecPtr;

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateInsertLog(txn_id_t txn_id, KeyType ins_key, ValType ins_val) {
    lsn_t lsn = LogRec::next_lsn_++;
    lsn_t prev_lsn = LogRec::find_the_log(txn_id, lsn);
    auto log = std::make_shared<LogRec>(LogRecType::kInsert, lsn, prev_lsn, txn_id);
    log->target_key_ = ins_key;
    log->target_val_ = ins_val;
    return log;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateDeleteLog(txn_id_t txn_id, KeyType del_key, ValType del_val) {
    lsn_t lsn = LogRec::next_lsn_++;
    lsn_t prev_lsn = LogRec::find_the_log(txn_id, lsn);
    auto log = std::make_shared<LogRec>(LogRecType::kDelete, lsn, prev_lsn, txn_id);
    log->target_key_ = del_key;
    log->target_val_ = del_val;
    return log;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateUpdateLog(txn_id_t txn_id, KeyType old_key, ValType old_val, KeyType new_key, ValType new_val) {
    lsn_t lsn = LogRec::next_lsn_++;
    lsn_t prev_lsn = LogRec::find_the_log(txn_id, lsn);
    auto log = std::make_shared<LogRec>(LogRecType::kUpdate, lsn, prev_lsn, txn_id);
    log->target_key_ = old_key;
    log->target_val_ = old_val;
    log->new_key_ = new_key;
    log->new_val_ = new_val;
    return log;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateBeginLog(txn_id_t txn_id) {
    lsn_t lsn = LogRec::next_lsn_++;
    lsn_t prev_lsn = LogRec::find_the_log(txn_id, lsn);
    return std::make_shared<LogRec>(LogRecType::kBegin, lsn, prev_lsn, txn_id);
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateCommitLog(txn_id_t txn_id) {
    lsn_t lsn = LogRec::next_lsn_++;
    lsn_t prev_lsn = LogRec::find_the_log(txn_id, lsn);
    return std::make_shared<LogRec>(LogRecType::kCommit, lsn, prev_lsn, txn_id);
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateAbortLog(txn_id_t txn_id) {
    lsn_t lsn = LogRec::next_lsn_++;
    lsn_t prev_lsn = LogRec::find_the_log(txn_id, lsn);
    return std::make_shared<LogRec>(LogRecType::kAbort, lsn, prev_lsn, txn_id);
}

#endif  // MINISQL_LOG_REC_H
