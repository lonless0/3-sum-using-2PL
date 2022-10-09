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
    ReleaseLocks(txn);
}

void TransactionManager::Abort(Transaction *txn) {
    txn->SetState(TransactionState::ABORTED);
    ReleaseLocks(txn);
}
