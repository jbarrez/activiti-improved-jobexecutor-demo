package org.activiti;

import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.activiti.engine.ProcessEngine;
import org.activiti.engine.ProcessEngineConfiguration;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
	
	private static final Logger logger = LoggerFactory.getLogger(Main.class);
	
	private static ProcessEngine processEngine;

	public static void main(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.err.println("Arguments needed");
			System.err.println("First argument: { job-creator | job-executor }, to determine what this Main program will");
			System.err.println("Second argument: { nrOfJobs }");
			System.err.println("Example usage : java -jar theJar.jar job-creator 10000");
			System.exit(-1);
		}
		
		int nrOfJobs = Integer.valueOf(args[1]);
		if ("job-creator".equals(args[0])) {
			createJobs(nrOfJobs);
		} else if ("job-executor".equals(args[0])) {
			executeJobs(nrOfJobs);
		}
		
		System.out.println("Press any key to quit the execution before it ends");
		new Scanner(System.in).nextLine();
		
	}
	
	private static void createJobs(int nrOfJobs) {
		
		// Job creator needs to be started first, as it will clean the database!
		
		processEngine = ProcessEngineConfiguration
		    .createProcessEngineConfigurationFromResource("activiti_job_creator.cfg.xml")
		    .setDatabaseSchemaUpdate("drop-create").buildProcessEngine();
		processEngine.getRepositoryService().createDeployment().name("job")
		    .addClasspathResource("job.bpmn20.xml").deploy();

		ExecutorService executor = Executors.newFixedThreadPool(10);
		for (int i = 0; i < nrOfJobs; i++) {
			Runnable worker = new StartThread();
			executor.execute(worker);
		}
		executor.shutdown();
	}
	
	private static void executeJobs(int nrOfJobs) throws Exception {
		
		processEngine = ProcessEngineConfiguration
		    .createProcessEngineConfigurationFromResource("activiti_with_jobexecutor.cfg.xml")
		    .buildProcessEngine();
		
		boolean finishedAllInstances = false;
		long lastPrintTime = 0;
		long finishedCount = 0;
		long start = System.currentTimeMillis();
		while (finishedAllInstances == false) {
			finishedCount = processEngine.getHistoryService()
			    .createHistoricProcessInstanceQuery().finished().count();
			if (finishedCount == nrOfJobs) {
				finishedAllInstances = true;
			} else {
				if (System.currentTimeMillis() - lastPrintTime > 5000L) {
					lastPrintTime = System.currentTimeMillis();
					logger.error("Executed " + finishedCount + " jobs");
				}
				Thread.sleep(50);
			}
		}

		Assert.assertEquals(nrOfJobs, finishedCount);
		long end = System.currentTimeMillis();

		Assert.assertEquals(0, processEngine.getManagementService().createJobQuery().count());
		System.out.println("Jobs in system (should be 0)= "+ processEngine.getManagementService().createJobQuery().count());

		long time = end - start;
		System.out.println("Took " + time + " ms");
		double perSecond = ((double) nrOfJobs / (double) time) * 1000;
		System.out.println("Which is " + perSecond + " jobs/second");
	}

	public static class StartThread implements Runnable {
		
		private static AtomicInteger count = new AtomicInteger(0);
		private static AtomicInteger lastPrintCount = new AtomicInteger(0);

		public void run() {
			processEngine.getRuntimeService().startProcessInstanceByKey("job");
			
			int currentCount = count.incrementAndGet();
			
			if (currentCount - lastPrintCount.get() > 500) {
				logger.info("Created " + currentCount + " jobs");
				lastPrintCount.set(currentCount);
			}
		}

	}

}
