//
// Created by lonless on 2022/10/8.
//
#include "transaction.h"
#include <vector>
#include <list>
#include <condition_variable>
#include <unordered_map>

using txn_id_t = int;


class LockManager {
    enum LockMode { SHARED, EXCLUSIVE };
    class LockRequest {
    public:
        LockRequest(txn_id_t txn_id, LockMode lock_mode) : txn_id_(txn_id), lock_mode_(lock_mode) {}
        txn_id_t txn_id_;
        LockMode lock_mode_;
        bool granted_{false};
    };
    class LockRequestQueue {
    public:
        std::list<LockRequest> request_queue_;
        std::condition_variable cv_;
        int upgrading_ = INVALID_TXN_ID;
    };
public:
    LockManager() = default;
    ~LockManager() = default;

    auto LockShared(Transaction *txn, RID index) -> bool;
    auto LockExclusive(Transaction *txn, RID index) -> bool;
    auto LockUpgrade(Transaction *txn, RID index) -> bool;
    auto Unlock(Transaction* txn, RID index) -> bool;
private:
    std::mutex latch_;
    std::unordered_map<int, LockRequestQueue> lock_table_;
    void ResetLockRequest(LockRequestQueue *request_queue, Transaction *txn, RID index);
    void RequestQueueInsert(LockRequestQueue *request_queue, int txn_id, LockMode lock_mode, bool granted);
    bool ValidSharedLock(LockRequestQueue *request_queue, Transaction *txn);
    bool ValidExclusiveLock(LockRequestQueue *request_queue, Transaction *txn);
};
