package org.learn.DistributedLock;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;
import org.learn.distributedLock.exception.DistributedLockException;
import org.learn.distributedLock.impl.DistributedLockImpl;
import org.learn.distributedLock.impl.DistributedLockImpl.DistributedLockBuilder;

public class LockTest {
	
	@Test
	public void testLock() throws Exception {
		
		Runnable runnable = new LockRunnable();
		Runnable runnable2 = new LockRunnable();
		Runnable runnable3 = new LockRunnable();
		
		ExecutorService executorService = Executors.newFixedThreadPool(2);
		executorService.execute(runnable);
		executorService.execute(runnable2);
		executorService.execute(runnable3);
		
		
		Thread.sleep(300000);
	}

	
	private static class LockRunnable implements Runnable{
		
		public void run() {
			// TODO Auto-generated method stub
			DistributedLockBuilder builder = new DistributedLockBuilder();
			builder.setZkUrl("localhost:2181/lockTest").setPath("/resource1").setZkTimeout(120000);
			
			try {
				DistributedLockImpl  distributedLockImpl = builder.build();
				distributedLockImpl.lock();
				System.out.println("Got the Lock");
				Thread.sleep(10000);
				distributedLockImpl.unLock();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (DistributedLockException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
}
