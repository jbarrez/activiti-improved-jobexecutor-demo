package org.activiti;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.activiti.engine.ProcessEngines;
import org.activiti.engine.impl.asyncexecutor.AsyncExecutor;
import org.activiti.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.activiti.engine.impl.cmd.ExecuteAsyncJobCmd;
import org.activiti.engine.impl.interceptor.CommandExecutor;
import org.activiti.engine.impl.persistence.entity.JobEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.monitor.LocalExecutorStats;

public class HazelCastAsyncExecutor implements AsyncExecutor {
	
	private static final Logger logger = LoggerFactory.getLogger(HazelCastAsyncExecutor.class);
	
	// Injecteable
	protected boolean isAutoActivate;
	protected CommandExecutor commandExecutor;
	
	// Runtime
	protected boolean isActive;
	private HazelcastInstance hazelcastInstance;
	private IExecutorService executorService;
	
	@Override
  public void start() {
		 if (isActive) {
	      return;
	    }
	    
		 logger.info("Starting up the Hazelcast async job executor [{}].", getClass().getName());

		 hazelcastInstance = Hazelcast.newHazelcastInstance();
		 executorService = hazelcastInstance.getExecutorService("activiti");
		 
	    isActive = true;
  }

	@Override
  public void shutdown() {
		try {
			executorService.shutdown();
	    executorService.awaitTermination(60, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
    	logger.warn("Exception while waiting for executor service shutdown", e);
    }
		
		LocalExecutorStats localExecutorStats = executorService.getLocalExecutorStats();
		logger.info("This async job executor has processed " + localExecutorStats.getCompletedTaskCount() 
				+ " jobs. Total execution time = " + localExecutorStats.getTotalExecutionLatency());
		
		hazelcastInstance.shutdown();
	  isActive = false;
  }
	
	private static AtomicInteger count = new AtomicInteger(0);

	@Override
  public void executeAsyncJob(JobEntity job) {
		try {
//			
//			long start = System.currentTimeMillis();
//			try {
//				DistributedExecuteJobRunnable runnable = new DistributedExecuteJobRunnable(job);
//				ByteArrayOutputStream baos = new ByteArrayOutputStream();
//				ObjectOutputStream out = new ObjectOutputStream(baos);
//				out.writeObject(runnable);
//				out.close();
//				baos.flush();
//				long end = System.currentTimeMillis();
//				System.out.println("Serialization time = " + (end-start) + " ms");
//				byte[] bytes = baos.toByteArray();
//				System.out.println("Byte size = " + bytes.length);
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
			
			executorService.submit(new DistributedExecuteJobRunnable(job));
		} catch (RejectedExecutionException e) {
			logger.info("Async job execution rejected. Executing job in calling thread.");
			// Execute in calling thread so the job executor can be freed
			commandExecutor.execute(new ExecuteAsyncJobCmd(job));
		}
  }
	
	@Override
  public boolean isActive() {
		return isActive;
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
	
	public static class DistributedExecuteJobRunnable implements Runnable, Serializable {
		
    private static final long serialVersionUID = -6294645802377574363L;
    
		protected JobEntity job;
		
		public DistributedExecuteJobRunnable(JobEntity job) {
			this.job = job;
    }
		
		@Override
		public void run() {
			CommandExecutor commandExecutor = ((ProcessEngineConfigurationImpl) ProcessEngines.getDefaultProcessEngine()
					.getProcessEngineConfiguration()).getCommandExecutor();
			commandExecutor.execute(new ExecuteAsyncJobCmd(job));
		}
		
	}

}
