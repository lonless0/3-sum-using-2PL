//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction.h
//
// Identification: src/include/concurrency/transaction.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <deque>
#include <memory>
#include <string>
#include <thread>  // NOLINT
#include <unordered_set>

const int INVALID_TXN_ID = -1;

/**
 * Transaction states for 2PL:
 *
 *     _________________________
 *    |                         v
 * GROWING -> SHRINKING -> COMMITTED   ABORTED
 *    |__________|________________________^
 *
 * Transaction states for Non-2PL:
 *     __________
 *    |          v
 * GROWING  -> COMMITTED     ABORTED
 *    |_________________________^
 *
 **/
enum class TransactionState { GROWING, SHRINKING, COMMITTED, ABORTED };


/**
 * Type of write operation.
 */

using txn_id_t = int;
using RID = uint64_t;

/**
 * Transaction tracks information related to a transaction.
 */
class Transaction {
 public:
  explicit Transaction(txn_id_t txn_id)
      : txn_id_(txn_id),
        shared_lock_set_{new std::unordered_set<RID>},
        exclusive_lock_set_{new std::unordered_set<RID>} {
  }

  ~Transaction() = default;



  /** @return the id of this transaction */
  inline auto GetTransactionId() const -> txn_id_t { return txn_id_; }


  /** @return the set of resources under a shared lock */
  inline auto GetSharedLockSet() -> std::shared_ptr<std::unordered_set<RID>> { return shared_lock_set_; }

  /** @return the set of resources under an exclusive lock */
  inline auto GetExclusiveLockSet() -> std::shared_ptr<std::unordered_set<RID>> { return exclusive_lock_set_; }

  /** @return true if rid is shared locked by this transaction */
  auto IsSharedLocked(const RID &rid) -> bool { return shared_lock_set_->find(rid) != shared_lock_set_->end(); }

  /** @return true if rid is exclusively locked by this transaction */
  auto IsExclusiveLocked(const RID &rid) -> bool {
    return exclusive_lock_set_->find(rid) != exclusive_lock_set_->end();
  }

  /** @return the current state of the transaction */
  inline auto GetState() -> TransactionState { return state_; }

  /**
   * Set the state of the transaction.
   * @param state new state
   */
  inline void SetState(TransactionState state) { state_ = state; }

 private:
  /** The current transaction state. */
  TransactionState state_{TransactionState::GROWING};
  /** The thread ID, used in single-threaded transactions. */
  std::thread::id thread_id_;
  /** The ID of this transaction. */
  txn_id_t txn_id_;

  /** LockManager: the set of shared-locked tuples held by this transaction. */
  std::shared_ptr<std::unordered_set<RID>> shared_lock_set_;
  /** LockManager: the set of exclusive-locked tuples held by this transaction. */
  std::shared_ptr<std::unordered_set<RID>> exclusive_lock_set_;
};

