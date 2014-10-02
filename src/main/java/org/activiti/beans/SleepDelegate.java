package org.activiti.beans;


import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.Expression;
import org.activiti.engine.delegate.JavaDelegate;

public class SleepDelegate implements JavaDelegate {

  protected Expression sleepTime;

  public void execute(DelegateExecution execution) throws Exception {
  	long startTime = System.currentTimeMillis();
  	long var = 0;
  	while (System.currentTimeMillis() - startTime < 200) {
  		var += startTime; // Doing something to keep the JVM busy
  		Thread.sleep(10L); // Doing some sleeps to mimic I/O
  	}
  }

  public void setSleepTime(Expression sleepTime) {
    this.sleepTime = sleepTime;
  }
}
