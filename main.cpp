#include <iostream>
#include <vector>
#include <random>
#include "transactionManager.h"
#include <atomic>
#include <chrono>

const int N = 100000;
const int worker_num = 20;
const int times = 10000;


std::atomic_int fails = 0;
std::atomic_int success = 0;

void data_init(std::vector<RID>& data_) {

    for (RID i = 0; i < N; ++i) {
        data_.emplace_back(i);
    }
}

int main() {
    std::default_random_engine e;
    std::vector<RID> data;
    data_init(data);
    LockManager lock_mgr{};
    TransactionManager txn_mgr{&lock_mgr};
    std::vector<Transaction *> txns;
    int num_rids = times * worker_num;
    for (int i = 0; i < num_rids; i++) {
        txns.push_back(txn_mgr.Begin());
    }
    // test

    auto task = [&](int worker_num) {
        txn_id_t txn_id_batch = (worker_num+1)*times;
        for (txn_id_t txn_id = worker_num*times; txn_id < txn_id_batch; txn_id++) {
            int res;
            int i = e()%N;
            int j = e()%N;

            for (RID off = 0; off < 3; off++) {
                RID idx = (i + off)%N;
                lock_mgr.LockShared(txns[txn_id], idx);
                res += data[idx];
            }
            if (txns[txn_id]->IsSharedLocked(j)) {
                lock_mgr.LockUpgrade(txns[txn_id], j);
            } else {
                lock_mgr.LockExclusive(txns[txn_id], j);
            }

            if (txns[txn_id]->GetState() == TransactionState::ABORTED) {
                fails++;
            } else {
//                printf("i=%d, j = %d, %d + %d + %d =%d -> %d\n", i, j, data[i], data[(i+1)%N], data[(i+2)%N], data[j], res);
                success++;
                data[j] = res;
            }
            txn_mgr.Commit(txns[txn_id]);
        }

    };

    auto t1 = std::chrono::system_clock::now();

    std::vector<std::thread> threads;
    threads.reserve(worker_num);

    for (int i = 0; i < worker_num; i++) {
        threads.emplace_back(std::thread{task, i});
    }

    for (int i = 0; i < worker_num; i++) {
        threads[i].join();
    }

    for (int i = 0; i < num_rids; i++) {
        delete txns[i];
    }
//    for (auto const& n:data) {
//        std::cout<<n<<" ";
//    }
//    std::cout<<std::endl;
    std::cout<<"Committed: "<<success<<std::endl;
    std::cout<<"Aborted: "<<fails<<std::endl;

    auto t2 = std::chrono::system_clock::now();

    auto gap = std::chrono::duration_cast<std::chrono::milliseconds>(t2-t1).count();

    std::cout<<"total time:" <<gap<<" ms"<<std::endl;
    std::cout<<success/gap<<" op/ms"<<std::endl;

    return 0;
}
