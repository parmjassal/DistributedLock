package org.learn.distributedLock.api;

import org.learn.distributedLock.exception.DistributedLockException;

public interface ILock {
	
	void lock() throws DistributedLockException;
	void unLock() throws DistributedLockException;
	
}
