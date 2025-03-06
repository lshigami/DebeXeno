package org.example.debexeno.coordination;

import lombok.Getter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@Getter
public class DistributedCoordinationService {

  private static final Logger logger = LoggerFactory.getLogger(
      DistributedCoordinationService.class);

  @Autowired
  private CuratorFramework curatorClient;

  //Instance ID is used to identify the instance in the leader election process
  @Value("${instance.id:#{T(java.util.UUID).randomUUID().toString()}}")
  private String instanceId;

  //LeaderLatch is a mechanism provided by Curator to perform leader election
  private LeaderLatch leaderLatch;
  private boolean isLeader = false;

  /**
   * Start the leader election process
   *
   * @param leaderPath         The ZooKeeper path for leader election
   * @param leadershipCallback Callback to execute when leadership is acquired
   */
  public void startLeaderElection(String leaderPath, Runnable leadershipCallback) {
    try {
      //Register this instance for leader election
      leaderLatch = new LeaderLatch(curatorClient, leaderPath, instanceId);

      //Add a listener to handle leadership changes
      leaderLatch.addListener(new LeaderLatchListener() {
        @Override
        public void isLeader() {
          logger.info("Instance {} is now the leader", instanceId);
          isLeader = true;
          if (leadershipCallback != null) {
            leadershipCallback.run();
          }
        }

        @Override
        public void notLeader() {
          logger.info("Instance {} is no longer the leader", instanceId);
          isLeader = false;
        }
      });

      //Start the leader election process
      leaderLatch.start();
      logger.info("Started leader election for instance {}", instanceId);
    } catch (Exception e) {
      logger.error("Error starting leader election for instance {}", instanceId, e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Stop the leader election process
   */
  public void stopLeaderElection() {
    if (leaderLatch != null) {
      try {
        leaderLatch.close();
        logger.info("Stopped leader election for instance {}", instanceId);
      } catch (Exception e) {
        logger.error("Failed to stop leader election", e);
      }
    }
  }

  /**
   * Acquire a distributed lock
   *
   * @param lockPath  The ZooKeeper path for the lock
   * @param timeoutMs The timeout in milliseconds to wait for the lock
   * @return The lock object if acquired, null otherwise
   */
  public InterProcessMutex acquireLock(String lockPath, long timeoutMs) {
    InterProcessMutex lock = new InterProcessMutex(curatorClient, lockPath);
    try {
      if (lock.acquire(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS)) {
        logger.info("Acquired lock at path {}", lockPath);
        return lock;
      } else {
        logger.warn("Failed to acquire lock at path {}", lockPath);
        return null;
      }
    } catch (Exception e) {
      logger.error("Error acquiring lock at path {}", lockPath, e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Release a distributed lock
   *
   * @param lock     The lock to release
   * @param lockPath The ZooKeeper path for the lock (for logging)
   */
  public void releaseLock(InterProcessMutex lock, String lockPath) {
    try {
      lock.release();
      logger.info("Released lock at path {}", lockPath);
    } catch (Exception e) {
      logger.error("Error releasing lock at path {}", lockPath, e);
      throw new RuntimeException(e);
    }
  }

}
