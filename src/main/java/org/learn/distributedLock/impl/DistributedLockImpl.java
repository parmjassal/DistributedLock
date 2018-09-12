package org.learn.distributedLock.impl;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeSet;
import java.util.concurrent.Phaser;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.learn.distributedLock.api.ILock;
import org.learn.distributedLock.exception.DistributedLockException;

public class DistributedLockImpl implements ILock {

	private ZooKeeper zooKeeper;
	private String zkUrl;
	private String path;
	private boolean retryOnConnectionLoss;
	private long zkTimeout;
	private String lockPath;
	
	private DistributedLockImpl( String zkUrl,String path, boolean retryOnConnectionLoss, long zkTimeout) {
		super();
		this.zkUrl = zkUrl;
		this.path = path;
		this.retryOnConnectionLoss = retryOnConnectionLoss;
		this.zkTimeout = zkTimeout;
		
	}

	private void init() throws IOException {
		zooKeeper = new ZooKeeper(zkUrl,(int) zkTimeout, null);
	}
	
	public void lock() throws DistributedLockException {
		// TODO Auto-generated method stub
		try{
			final Phaser phaser = new Phaser();
			checkExistAndCreate();
			lockPath = zooKeeper.create(path+"/lock", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			while(true) {
				phaser.bulkRegister(2);
				
				TreeSet<String> orderSet = new TreeSet<String>();
				orderSet.addAll(zooKeeper.getChildren(path, false));
				final String smallerPath = orderSet.lower(lockPath.replaceAll(path+"/", ""));
				if (smallerPath == null) {
					return;
				} else {
					if(zooKeeper.exists(path+"/"+smallerPath, new Watcher() {
						public void process(WatchedEvent event) {
							// TODO Auto-generated method stub
							if (event.getType() == EventType.NodeDeleted)
								phaser.arriveAndDeregister();
						}
					})==null) {
						return ;
					}
				}
				phaser.arriveAndAwaitAdvance();
				
			}
		} catch(Exception e) {
			throw new DistributedLockException(e.getMessage(), e);
		}
		
	}

	private void checkExistAndCreate() throws DistributedLockException  {
		Stat stat =null;
		try {
			stat = zooKeeper.exists(path, false);
			if (stat == null) {
				zooKeeper.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		} catch(Exception e) {
			throw new DistributedLockException(e.getMessage(), e);
		}
	}

	public void unLock() throws DistributedLockException {
		// TODO Auto-generated method stub
		try {
			zooKeeper.delete(lockPath, -1);
			System.out.println("Unlock");
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
	public static class DistributedLockBuilder{
		private String zkUrl;
		private String path;
		private boolean retryOnConnectionLoss;
		private long zkTimeout;
		
		public DistributedLockBuilder() {
			this.zkTimeout = 30000;
			this.retryOnConnectionLoss = true;
		}

		
		public DistributedLockBuilder setPath(String path) {
			this.path = path;
			return this;
		}

		public DistributedLockBuilder setRetryOnConnectionLoss(boolean retryOnConnectionLoss) {
			this.retryOnConnectionLoss = retryOnConnectionLoss;
			return this;
		}

		public DistributedLockBuilder setZkTimeout(long zkTimeout) {
			this.zkTimeout = zkTimeout;
			return this;
		}
		
		public DistributedLockBuilder setZkUrl(String zkUrl) {
			this.zkUrl = zkUrl;
			return this;
		}


		public DistributedLockImpl build() throws IOException {
			DistributedLockImpl distributedLockImpl=new DistributedLockImpl(zkUrl,path, retryOnConnectionLoss, zkTimeout);
			distributedLockImpl.init();
			return distributedLockImpl;
		} 
		
	}
	
}
