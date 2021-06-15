package com.techgeeknext.spring.batch.scheduler.step;

import com.techgeeknext.spring.batch.scheduler.model.Employee;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

public class EmployeeJobReader implements ItemReader<Employee> {
	private static final Logger LOG = LoggerFactory.getLogger(EmployeeJobReader.class);

	private Employee employee;

	public EmployeeJobReader(Employee item) {
		this.employee = employee;
	}

	@Override
	public Employee read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
		LOG.info("EmployeeJobReader read() employee: {}", employee);
		return employee;
	}
}
