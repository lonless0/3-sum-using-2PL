//
// Created by lonless on 2022/10/8.
//

#include "transaction_manager.h"
#include "transaction.h"

#include <unordered_map>
#include <unordered_set>
#include <shared_mutex>

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};
std::shared_mutex TransactionManager::txn_map_mutex = {};

auto TransactionManager::Begin(Transaction *txn) -> Transaction * {
    // Acquire the global transaction latch in shared mode.
    global_txn_latch_.RLock();

    if (txn == nullptr) {
        txn = new Transaction(next_txn_id_++);
    }
    txn_map_mutex.lock();
    txn_map[txn->GetTransactionId()] = txn;
    txn_map_mutex.unlock();
    return txn;
}

void TransactionManager::Commit(Transaction *txn) {
    txn->SetState(TransactionState::COMMITTED);


    // Release all the locks.
    ReleaseLocks(txn);
    // Release the global transaction latch.
    global_txn_latch_.RUnlock();
}

void TransactionManager::Abort(Transaction *txn) {
    txn->SetState(TransactionState::ABORTED);
    // Rollback before releasing the lock.


    // Release all the locks.
    ReleaseLocks(txn);
    // Release the global transaction latch.
    global_txn_latch_.RUnlock();
}

void TransactionManager::BlockAllTransactions() { global_txn_latch_.WLock(); }

void TransactionManager::ResumeTransactions() { global_txn_latch_.WUnlock(); }
