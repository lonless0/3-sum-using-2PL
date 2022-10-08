//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//



#include "transaction.h"
#include "transactionManager.h"


auto LockManager::LockShared(Transaction *txn, RID rid) -> bool {
  std::unique_lock latch(latch_);
  auto &request_queue = lock_table_[rid];
  //  Aborted，直接返回false
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  //  不是处于growing状态的事务不应当获得锁，abort
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    ResetLockRequest(&request_queue, txn, rid);
    return false;
  }
  //  已经获得过锁了，假装无事发生
  if (txn->IsSharedLocked(rid) || txn->IsExclusiveLocked(rid)) {
    return true;
  }
  bool success = false;
  //  在该rid对应的request queue中插入该锁
  RequestQueueInsert(&request_queue, txn->GetTransactionId(), LockMode::SHARED, false);
  //  遍历该rid的request queue
  for (auto iter = request_queue.request_queue_.begin(); iter != request_queue.request_queue_.end();) {
    auto transaction = TransactionManager::GetTransaction(iter->txn_id_);
    if (iter->lock_mode_ == LockMode::EXCLUSIVE && iter->txn_id_ > txn->GetTransactionId()) {
      //  发现有独占锁的小东西，直接踢掉
      transaction->GetExclusiveLockSet()->erase(rid);
      transaction->SetState(TransactionState::ABORTED);
      request_queue.request_queue_.erase(iter++);
      success = true;
    } else if (iter->lock_mode_ == LockMode::SHARED && iter->txn_id_ == txn->GetTransactionId()) {
      // 已经有S锁了
      break;
    } else {
      iter++;
    }
  }
  //  成功加锁，notify all
  if (success) {
    request_queue.cv_.notify_all();
  }
  //  等待锁 granted
  while (txn->GetState() != TransactionState::ABORTED && !ValidSharedLock(&request_queue, txn)) {
    request_queue.cv_.wait(latch);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

auto LockManager::LockExclusive(Transaction *txn, RID rid) -> bool {
  std::unique_lock<std::mutex> latch(latch_);
  auto &request_queue = lock_table_[rid];
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    ResetLockRequest(&request_queue, txn, rid);
    return false;
  }
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  bool success = false;
  RequestQueueInsert(&request_queue, txn->GetTransactionId(), LockMode::EXCLUSIVE, false);
  for (auto iter = request_queue.request_queue_.begin(); iter != request_queue.request_queue_.end();) {
    if (iter->txn_id_ > txn->GetTransactionId()) {
      auto transaction = TransactionManager::GetTransaction(iter->txn_id_);
      if (iter->lock_mode_ == LockMode::SHARED) {
        transaction->GetSharedLockSet()->erase(rid);
      } else {
        transaction->GetExclusiveLockSet()->erase(rid);
      }
      transaction->SetState(TransactionState::ABORTED);
      request_queue.request_queue_.erase(iter++);
      success = true;
    } else if (iter->txn_id_ == txn->GetTransactionId() && iter->lock_mode_ == LockMode::EXCLUSIVE) {
      break;
    } else {
      iter++;
    }
  }
  if (success) {
    request_queue.cv_.notify_all();
  }
  while (txn->GetState() != TransactionState::ABORTED && !ValidExclusiveLock(&request_queue, txn)) {
    request_queue.cv_.wait(latch);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

auto LockManager::LockUpgrade(Transaction *txn, RID rid) -> bool {
  std::unique_lock<std::mutex> lock(latch_);
  auto &request_queue = lock_table_[rid];
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    ResetLockRequest(&request_queue, txn, rid);
    return false;
  }
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  if (!txn->IsSharedLocked(rid)) {
    return false;
  }
  if (request_queue.upgrading_ != INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    ResetLockRequest(&request_queue, txn, rid);
    return false;
  }
  request_queue.upgrading_ = txn->GetTransactionId();
  bool success = false;
  RequestQueueInsert(&request_queue, txn->GetTransactionId(), LockMode::EXCLUSIVE, false);
  for (auto iter = request_queue.request_queue_.begin(); iter != request_queue.request_queue_.end();) {
    auto transaction = TransactionManager::GetTransaction(iter->txn_id_);
    if (iter->txn_id_ > txn->GetTransactionId()) {
      if (iter->lock_mode_ == LockMode::SHARED) {
        transaction->GetSharedLockSet()->erase(rid);
      } else {
        transaction->GetExclusiveLockSet()->erase(rid);
      }
      transaction->SetState(TransactionState::ABORTED);
      request_queue.request_queue_.erase(iter++);
      success = true;
    } else if (iter->txn_id_ == txn->GetTransactionId() && iter->lock_mode_ == LockMode::SHARED) {
      if (iter->lock_mode_ == LockMode::SHARED) {
        transaction->GetSharedLockSet()->erase(rid);
      } else {
        transaction->GetExclusiveLockSet()->erase(rid);
      }
      request_queue.request_queue_.erase(iter++);
      success = true;
    } else {
      iter++;
    }
  }
  if (success) {
    request_queue.cv_.notify_all();
  }
  while (txn->GetState() != TransactionState::ABORTED && !ValidExclusiveLock(&request_queue, txn)) {
    request_queue.cv_.wait(lock);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    request_queue.upgrading_ = INVALID_TXN_ID;
    return false;
  }

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  request_queue.upgrading_ = INVALID_TXN_ID;
  return true;
}

auto LockManager::Unlock(Transaction *txn, RID rid) -> bool {
  std::unique_lock<std::mutex> latch(latch_);
  auto &request_queue = lock_table_[rid];

  for (auto iter = request_queue.request_queue_.begin(); iter != request_queue.request_queue_.end(); ++iter) {
    if (iter->txn_id_ == txn->GetTransactionId()) {
      ResetLockRequest(&request_queue, txn, rid);
      request_queue.cv_.notify_all();
      return true;
    }
  }
  return false;
}

void LockManager::ResetLockRequest(LockRequestQueue *request_queue, Transaction *txn, RID rid) {
  txn->GetExclusiveLockSet()->erase(rid);
  txn->GetSharedLockSet()->erase(rid);
  if (request_queue == nullptr) {
    return;
  }
  for (auto iter = request_queue->request_queue_.begin(); iter != request_queue->request_queue_.end();) {
    if (iter->txn_id_ == txn->GetTransactionId()) {
      request_queue->request_queue_.erase(iter++);
      return;
    }
    iter++;
  }
}

void LockManager::RequestQueueInsert(LockRequestQueue *request_queue, txn_id_t txn_id, LockMode lock_mode,
                                     bool granted) {
  for (auto &iter : request_queue->request_queue_) {
    if (iter.txn_id_ == txn_id && iter.lock_mode_ == lock_mode) {
      iter.granted_ = granted;
      return;
    }
  }
  LockRequest request = LockRequest(txn_id, lock_mode);
  request.granted_ = granted;
  request_queue->request_queue_.emplace_back(request);
}

bool LockManager::ValidSharedLock(LockRequestQueue *request_queue, Transaction *txn) {
  for (auto &iter : request_queue->request_queue_) {
    if (iter.txn_id_ == txn->GetTransactionId() && iter.lock_mode_ == LockMode::SHARED) {
      return true;
    }
    if (iter.lock_mode_ == LockMode::EXCLUSIVE) {
      return false;
    }
  }
  return true;
}

bool LockManager::ValidExclusiveLock(LockRequestQueue *request_queue, Transaction *txn) {
  auto front_request = request_queue->request_queue_.front();
  return front_request.txn_id_ == txn->GetTransactionId() && front_request.lock_mode_ == LockMode::EXCLUSIVE;
}
