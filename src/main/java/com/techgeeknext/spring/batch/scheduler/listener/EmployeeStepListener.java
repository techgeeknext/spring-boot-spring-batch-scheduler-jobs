package com.techgeeknext.spring.batch.scheduler.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.stereotype.Component;

@Component
public class EmployeeStepListener implements StepExecutionListener {
	private static final Logger LOG = LoggerFactory.getLogger(EmployeeStepListener.class);

	@Override
	public void beforeStep(StepExecution stepExecution) {
		LOG.info("EmployeeStepListener beforeStep with Step {} and completed for job {}", stepExecution.getStepName(),stepExecution.getJobExecution().getJobInstance().getJobName());
	}

	@Override
	public ExitStatus afterStep(StepExecution stepExecution) {
		LOG.info("EmployeeStepListener afterStep with Step {} and completed for job {}", stepExecution.getStepName(),stepExecution.getJobExecution().getJobInstance().getJobName());
		return null;
	}
}
