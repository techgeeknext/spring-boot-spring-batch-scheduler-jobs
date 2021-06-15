package com.techgeeknext.spring.batch.scheduler.step;

import com.techgeeknext.spring.batch.scheduler.model.Employee;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;

import java.util.List;

public class EmployeeJobWriter implements ItemWriter<Employee> {

    private static final Logger LOG = LoggerFactory.getLogger(EmployeeJobWriter.class);
    private Employee emp = new Employee();

    @Override
    public void write(List<? extends Employee> employee) throws Exception {
        LOG.info("EmployeeJobWriter write() employee: {}", employee);
        emp.setId(employee.get(0).getId());
        emp.setName(employee.get(0).getName());
    }

    public Employee getOutput() {
        return emp;
    }
}
