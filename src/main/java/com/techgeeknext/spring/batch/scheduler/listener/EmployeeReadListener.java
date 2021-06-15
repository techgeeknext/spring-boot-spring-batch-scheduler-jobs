package com.techgeeknext.spring.batch.scheduler.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.stereotype.Component;

@Component
public class EmployeeReadListener<T> implements ItemReadListener<T> {
	private static final Logger LOG = LoggerFactory.getLogger(EmployeeReadListener.class);

	@Override
	public void beforeRead()
	{
		System.out.println("ReaderListener::beforeRead()");
	}

	@Override
	public void afterRead(T employee) {
		LOG.info("EmployeeReadListener afterRead employee: {}", employee);
	}

	@Override
	public void onReadError(Exception ex) {
		LOG.info("EmployeeReadListener onReadError exception: {}", ex);
	}
}
