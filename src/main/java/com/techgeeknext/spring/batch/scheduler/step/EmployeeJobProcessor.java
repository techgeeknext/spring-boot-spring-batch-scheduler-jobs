package com.techgeeknext.spring.batch.scheduler.step;

import com.techgeeknext.spring.batch.scheduler.model.Employee;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

public class EmployeeJobProcessor implements ItemProcessor<Employee, Employee> {
	private static final Logger LOG = LoggerFactory.getLogger(EmployeeJobProcessor.class);
	@Override
	public Employee process(Employee employee) throws Exception {
		LOG.info("EmployeeJobProcessor process employee: {}", employee);

		Employee employee1 = new Employee();
		employee1.setId(employee.getId());
		employee1.setName(employee.getName());

		return employee1;
	}
}
