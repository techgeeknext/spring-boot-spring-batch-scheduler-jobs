package com.techgeeknext.spring.batch.scheduler.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class EmployeeWriteListener<S> implements ItemWriteListener<S> {
	private static final Logger LOG = LoggerFactory.getLogger(EmployeeWriteListener.class);

	@Override
	public void beforeWrite(List<? extends S> employees) {
		LOG.info("EmployeeWriteListener beforeWrite employee: {}", employees);
	}

	@Override
	public void afterWrite(List<? extends S> employees) {
		LOG.info("EmployeeWriteListener afterWrite employee: {}", employees);
	}

	@Override
	public void onWriteError(Exception exception, List<? extends S> employee) {
		LOG.info("EmployeeWriteListener onWriteError employee {} with exception", employee, exception);
	}

}
