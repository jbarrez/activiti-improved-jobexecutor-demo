package org.activiti;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.activiti.engine.impl.asyncexecutor.AsyncExecutor;
import org.activiti.engine.impl.asyncexecutor.ExecuteAsyncRunnable;
import org.activiti.engine.impl.interceptor.CommandExecutor;
import org.activiti.engine.impl.persistence.entity.JobEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.IQueue;
import com.hazelcast.monitor.LocalQueueStats;

public class HazelCastAsyncExecutor2 implements AsyncExecutor {
	
	private static final Logger logger = LoggerFactory.getLogger(HazelCastAsyncExecutor2.class);
	
	// Injecteable
	protected boolean isAutoActivate;
	protected CommandExecutor commandExecutor;
	
	// Runtime
	protected boolean isActive;
	protected HazelcastInstance hazelcastInstance;
	protected IQueue<JobEntity> jobQueue;
	
	protected Thread jobQueueListenerThread;
	protected BlockingQueue<Runnable> threadPoolQueue;
	protected ExecutorService executorService;
	
	@Override
	public void start() {
		if (isActive) {
			return;
		}

		logger.info("Starting up the Hazelcast async job executor [{}].", getClass().getName());

		hazelcastInstance = Hazelcast.newHazelcastInstance();
		jobQueue = hazelcastInstance.getQueue("activiti");

		threadPoolQueue = new ArrayBlockingQueue<Runnable>(1024);
		ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(25, 500, 180000L, TimeUnit.MILLISECONDS, threadPoolQueue);
		threadPoolExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
		executorService = threadPoolExecutor;
		
		isActive = true;
		initJobQueueListener();
	}
	
	protected void initJobQueueListener() {
		jobQueueListenerThread = new Thread(new Runnable() {
			
			public void run() {
				while (isActive) {
					JobEntity job = null;
					try {
				    job = jobQueue.take(); // Blocking
			    } catch (InterruptedException e1) {
				    // Do nothing, this can happen when shutting down
			    }
					
					if (job != null) {
						executorService.execute(new ExecuteAsyncRunnable(job, commandExecutor));
					}
				}
			}
			
		});
		jobQueueListenerThread.start();
	}

	@Override
  public void shutdown() {
		jobQueue.destroy();
		
		LocalQueueStats localQueueStats = jobQueue.getLocalQueueStats();
		logger.info("This async job executor has processed " + localQueueStats.getPollOperationCount());

		try {
			hazelcastInstance.shutdown();
		} catch (HazelcastInstanceNotActiveException e) {
			// Nothing to do
		}
	  isActive = false;
  }
	
	@Override
  public void executeAsyncJob(JobEntity job) {
		try {
	    jobQueue.put(job);
    } catch (InterruptedException e) {
	    // Nothing to do about it, can happen at shutdown for example
    }
  }
	
	@Override
  public boolean isActive() {
		return isActive;
  }
	
	@Override
	public CommandExecutor getCommandExecutor() {
		return commandExecutor;
	}


	@Override
  public void setCommandExecutor(CommandExecutor commandExecutor) {
	  this.commandExecutor = commandExecutor;
  }

	@Override
  public boolean isAutoActivate() {
		return isAutoActivate;
  }

	@Override
  public void setAutoActivate(boolean isAutoActivate) {
		this.isAutoActivate = isAutoActivate;
  }
	
}
