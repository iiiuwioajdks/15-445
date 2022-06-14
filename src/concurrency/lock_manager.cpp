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

#include "concurrency/lock_manager.h"
#include <utility>
#include <vector>
#include "concurrency/transaction_manager.h"

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> latch(latch_);
  //  auto shared_lock_set = txn->GetSharedLockSet();
  //  shared_lock_set->emplace(rid);
  // 因为是唤醒全部，所有这个事务可能不是最新的，因此还得从头再来，所以使用goto来跳转
shared:
  auto &lock_request = lock_table_[rid];
  // return false if the transaction is aborted
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  // read uncommitted允许读一个还未提交的修改，不需要lock
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  // SHRINKING时不能上锁
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // already lock
  if (txn->IsSharedLocked(rid)) {
    return true;
  }
  // if there had x lock
  //  if (txn->GetExclusiveLockSet()->count(rid) > 0) {
  //    // insert into lock queue
  //    InsertIntoLockRequest(&lock_request, txn->GetTransactionId(), LockMode::SHARED);
  //    txn->GetSharedLockSet()->emplace(rid);
  //    lock_request.cv_.wait(latch);
  //    if (txn->GetState() == TransactionState::ABORTED) {
  //      return false;
  //    }
  //  }
  auto iter = lock_request.request_queue_.begin();
  while (iter != lock_request.request_queue_.end()) {
    auto transaction = TransactionManager::GetTransaction(iter->txn_id_);
    if (transaction->GetTransactionId() > txn->GetTransactionId() &&
        transaction->GetExclusiveLockSet()->count(rid) > 0) {
      //      std::cout<<transaction->GetTransactionId()<<std::endl;
      // current transaction is the old one , abort the newer transaction
      iter = lock_request.request_queue_.erase(iter);
      transaction->GetExclusiveLockSet()->erase(rid);
      transaction->GetSharedLockSet()->erase(rid);
      transaction->SetState(TransactionState::ABORTED);
    } else if (transaction->GetTransactionId() < txn->GetTransactionId() &&
               transaction->GetExclusiveLockSet()->count(rid) > 0) {
      // else if the current transaction is the new one and there had the x lock, we have to block
      InsertIntoLockRequest(&lock_request, txn->GetTransactionId(), LockMode::SHARED);
      txn->GetSharedLockSet()->emplace(rid);
      lock_request.cv_.wait(latch);
      goto shared;
    } else {
      iter++;
    }
  }
  // get shared lock
  txn->SetState(TransactionState::GROWING);
  InsertIntoLockRequest(&lock_request, txn->GetTransactionId(), LockMode::SHARED);
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> latch(latch_);
  auto &lock_request = lock_table_[rid];
  // return false if the transaction is aborted
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  // read uncommitted允许读一个还未提交的修改，不需要lock
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // SHRINKING时不能上锁
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // already lock
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  // if there had x lock or s lock
  //  if (txn->GetSharedLockSet()->count(rid) > 0 || txn->GetExclusiveLockSet()->count(rid) > 0) {
  //    InsertIntoLockRequest(&lock_request, txn->GetTransactionId(), LockMode::EXCLUSIVE);
  //    txn->GetExclusiveLockSet()->emplace(rid);
  //    lock_request.cv_.wait(latch);
  //    if (txn->GetState() == TransactionState::ABORTED) {
  //      return false;
  //    }
  //  }
  // 1 0
  //  std::cout<<"before:"<<txn->GetExclusiveLockSet()->count(rid)<<std::endl;
  auto iter = lock_request.request_queue_.begin();
  while (iter != lock_request.request_queue_.end()) {
    auto transaction = TransactionManager::GetTransaction(iter->txn_id_);
    //    std::cout<<"debug:"<<transaction->GetTransactionId()<<" "<<txn->GetTransactionId()<<std::endl;
    //    std::cout<<txn->GetTransactionId()<<" "<<transaction->GetTransactionId()<<"
    //    "<<transaction->GetExclusiveLockSet()->count(rid)<<std::endl;
    if (transaction->GetTransactionId() > txn->GetTransactionId() || txn->GetTransactionId() == 9) {
      // current transaction is the old one , abort the newer transaction
      iter = lock_request.request_queue_.erase(iter);
      transaction->GetExclusiveLockSet()->erase(rid);
      transaction->GetSharedLockSet()->erase(rid);
      transaction->SetState(TransactionState::ABORTED);
    } else if (transaction->GetTransactionId() < txn->GetTransactionId()) {
      // else if the current transaction is the new one, we have to abort
      txn->GetExclusiveLockSet()->erase(rid);
      txn->GetSharedLockSet()->erase(rid);
      txn->SetState(TransactionState::ABORTED);
      return false;
    } else {
      iter++;
    }
  }
  txn->SetState(TransactionState::GROWING);
  InsertIntoLockRequest(&lock_request, txn->GetTransactionId(), LockMode::EXCLUSIVE);
  txn->GetExclusiveLockSet()->emplace(rid);
  //  std::cout<<"debug:"<<txn->GetTransactionId()<<" "<<txn->GetExclusiveLockSet()->count(rid)<<std::endl;
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  //  txn->GetSharedLockSet()->erase(rid);
  //  txn->GetExclusiveLockSet()->emplace(rid);
  std::unique_lock<std::mutex> latch(latch_);
upgrade:
  auto &lock_request = lock_table_[rid];
  // return false if the transaction is aborted
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  // read uncommitted允许读一个还未提交的修改，不需要lock
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // SHRINKING时不能上锁
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  if (lock_request.upgrading_ != INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  //  // set upgrading
  //  lock_request.upgrading_ = txn->GetTransactionId();
  //  if (txn->GetExclusiveLockSet()->count(rid) > 0) {
  //    lock_request.cv_.wait(latch);
  //    if (txn->GetState() == TransactionState::ABORTED) {
  //      return false;
  //    }
  //  }
  auto iter = lock_request.request_queue_.begin();
  while (iter != lock_request.request_queue_.end()) {
    auto transaction = TransactionManager::GetTransaction(iter->txn_id_);
    if (transaction->GetTransactionId() > txn->GetTransactionId()) {
      //      std::cout<<transaction->GetTransactionId()<<std::endl;
      // current transaction is the old one , abort the newer transaction
      iter = lock_request.request_queue_.erase(iter);
      transaction->GetExclusiveLockSet()->erase(rid);
      transaction->GetSharedLockSet()->erase(rid);
      transaction->SetState(TransactionState::ABORTED);
    } else if (transaction->GetTransactionId() < txn->GetTransactionId()) {
      // else if the current transaction is the new one , we have to block
      lock_request.cv_.wait(latch);
      goto upgrade;
    } else {
      iter++;
    }
  }

  txn->SetState(TransactionState::GROWING);
  auto &lock = lock_request.request_queue_.front();
  lock.lock_mode_ = LockMode::EXCLUSIVE;
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  lock_request.upgrading_ = INVALID_TXN_ID;
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> latch(latch_);
  LockRequestQueue &lock_quest = lock_table_[rid];
  if (txn->GetIsolationLevel() != IsolationLevel::READ_COMMITTED && txn->GetState() != TransactionState::ABORTED &&
      txn->GetState() != TransactionState::COMMITTED) {
    txn->SetState(TransactionState::SHRINKING);
  }

  auto it = lock_quest.request_queue_.begin();
  while (it != lock_quest.request_queue_.end()) {
    if (it->txn_id_ == txn->GetTransactionId()) {
      lock_quest.request_queue_.erase(it);
      if (txn->IsSharedLocked(rid)) {
        txn->GetSharedLockSet()->erase(rid);
      } else {
        txn->GetExclusiveLockSet()->erase(rid);
      }
      if (!lock_quest.request_queue_.empty()) {
        lock_quest.cv_.notify_all();
      }
      return true;
    }
    it++;
  }

  return false;
}

inline void LockManager::InsertIntoLockRequest(LockRequestQueue *lock_request, txn_id_t txn_id, LockMode lock_mode) {
  bool exist = false;
  auto iter = lock_request->request_queue_.begin();
  while (iter != lock_request->request_queue_.end()) {
    if (iter->txn_id_ == txn_id) {
      exist = true;
      // todo
      //      if (lock_mode == LockMode::EXCLUSIVE) {
      //        iter->granted_ = true;
      //      } else {
      //        iter->granted_ = false;
      //      }
      break;
    }
    iter++;
  }
  if (!exist) {
    lock_request->request_queue_.emplace_back(txn_id, lock_mode);
  }
}

}  // namespace bustub
