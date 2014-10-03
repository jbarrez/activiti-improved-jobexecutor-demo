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
	
	private static void createJobs(int nrOfJobs) throws Exception{
		
		// Job creator needs to be started first, as it will clean the database!
		
		processEngine = ProcessEngineConfiguration
		    .createProcessEngineConfigurationFromResource("activiti_job_creator.cfg.xml")
		    .setDatabaseSchemaUpdate("drop-create").buildProcessEngine();
		processEngine.getRepositoryService().createDeployment().name("job")
		    .addClasspathResource("job.bpmn20.xml").deploy();

		System.out.println("Process engine created. Press any key to start");
		new Scanner(System.in).nextLine();
		
		ExecutorService executor = Executors.newFixedThreadPool(6);
		for (int i = 0; i < nrOfJobs; i++) {
			Runnable worker = new StartThread();
			executor.execute(worker);
		}
		executor.shutdown();
		logger.info("All work handed off to threadpool");
		
		boolean finishedAllInstances = false;
		long finishedCount = 0;
		while (finishedAllInstances == false) {
			finishedCount = processEngine.getHistoryService()
			    .createHistoricProcessInstanceQuery().unfinished().count();
			logger.error(finishedCount + " unfinished jobs");
			Thread.sleep(5000L);
		}
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
		System.out.println("Jobs executed by this node: " + SleepDelegate.nrOfExecutions.get());
		
		long time = end - start;
		System.out.println("Took " + time + " ms");
		double perSecond = ((double) SleepDelegate.nrOfExecutions.get() / (double) time) * 1000;
		System.out.println("Which is " + perSecond + " jobs/second");
	}

	public static class StartThread implements Runnable {
		
		public void run() {
			processEngine.getRuntimeService().startProcessInstanceByKey("job");
		}

	}

}
