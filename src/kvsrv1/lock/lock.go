package lock

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	name string
	id   string
}

const (
	Locked   = "Locked"
	UnLocked = "UnLocked"
)

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck, name: lockname, id: kvtest.RandValue(8)}
	// You may add code here
	return lk
}

// 获取锁，如果获取锁失败，开始自旋，直到锁被释放
func (lk *Lock) Acquire() {
	var value string
	var version rpc.Tversion
	var err rpc.Err
	for {
		// 如果锁被其他客户端持有，则自旋等待
		value, version, err = lk.ck.Get(lk.name)
		if err == rpc.ErrNoKey {
			// 创建并获取锁成功，直接返回
			if lk.ck.Put(lk.name, lk.id, 0) == rpc.OK {
				return
			}
			// 创建锁失败，代表锁已经被其他人创建好了,重试尝试获取锁
			continue
		}
		// 如果锁已经被自己占有了，直接返回
		if value == lk.id {
			return
		}
		// 判断锁是否被持有，如果没有被持有继续获取锁，被持有则继续自旋等待
		if err == rpc.OK && value == UnLocked {
			if lk.ck.Put(lk.name, lk.id, version) == rpc.OK {
				return
			}
		}

	}
	// Your code here
}

func (lk *Lock) Release() {
	// for {
	// 	_, version, err := lk.ck.Get(lk.name)
	// 	// 锁状态异常，直接返回
	// 	if err != rpc.OK {
	// 		return
	// 	}
	// 	if lk.ck.Put(lk.name, UnLocked, version) == rpc.OK {
	// 		return
	// 	}
	// }
	// 已经持有了锁，锁一定存在，所以直接获取锁的版本号并更新为UnLocked
	// Your code here
	_, version, _ := lk.ck.Get(lk.name)
	lk.ck.Put(lk.name, UnLocked, version)
}
