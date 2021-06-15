package com.techgeeknext.spring.batch.scheduler.config;

import com.techgeeknext.spring.batch.scheduler.listener.EmployeeJobListener;
import com.techgeeknext.spring.batch.scheduler.listener.EmployeeReadListener;
import com.techgeeknext.spring.batch.scheduler.listener.EmployeeStepListener;
import com.techgeeknext.spring.batch.scheduler.listener.EmployeeWriteListener;
import com.techgeeknext.spring.batch.scheduler.model.Employee;
import com.techgeeknext.spring.batch.scheduler.step.EmployeeJobProcessor;
import com.techgeeknext.spring.batch.scheduler.step.EmployeeJobReader;
import com.techgeeknext.spring.batch.scheduler.step.EmployeeJobWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableScheduling
@EnableBatchProcessing
public class SchedulerJobConfiguration {
    private static final Logger LOG = LoggerFactory.getLogger(SchedulerJobConfiguration.class);

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    // @Scheduled(cron = "*/5 * * * * *")
    @Scheduled(fixedRate = 10000)
    public void runJob1() {
        Map<String, JobParameter> jobConfigMap = new HashMap<>();
        jobConfigMap.put("time", new JobParameter(System.currentTimeMillis()));
        JobParameters parameters = new JobParameters(jobConfigMap);
        try {
            jobLauncher().run(job1(), parameters);
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
        }

    }

    // @Scheduled(cron = "*/5 * * * * *")
    @Scheduled(fixedRate = 10000)
    public void runJob2() {
        Map<String, JobParameter> jobConfigMap = new HashMap<>();
        jobConfigMap.put("time", new JobParameter(System.currentTimeMillis()));
        JobParameters parameters = new JobParameters(jobConfigMap);
        try {
            jobLauncher().run(job2(), parameters);
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
        }
    }

    // @Scheduled(cron = "*/5 * * * * *")
    @Scheduled(fixedRate = 10000)
    public void runJob3() {
        Map<String, JobParameter> jobConfigMap = new HashMap<>();
        jobConfigMap.put("time", new JobParameter(System.currentTimeMillis()));
        JobParameters parameters = new JobParameters(jobConfigMap);
        try {
            jobLauncher().run(job3(), parameters);
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
        }
    }

    @Bean
    public ItemReader<Employee> employeeJobReader() {
        return new EmployeeJobReader(new Employee("1100", "TechGeekNext-Employee"));
    }

    @Bean
    public ItemProcessor<Employee, Employee> employeeJobProcessor() {
        return new EmployeeJobProcessor();
    }

    @Bean
    public ItemWriter<Employee> employeeJobWriter() {
        return new EmployeeJobWriter();
    }

    @Bean
    public ItemReadListener<Employee> employeeReadListener() {
        return new EmployeeReadListener<Employee>();
    }

    @Bean
    public StepExecutionListener employeeStepListener() {
        return new EmployeeStepListener();
    }

    @Bean
    public ItemWriteListener<Employee> employeeWriteListener() {
        return new EmployeeWriteListener<Employee>();
    }

    @Bean
    public JobExecutionListener employeeJobListener() {
        return new EmployeeJobListener();
    }

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("Step1").listener(employeeStepListener()).listener(employeeReadListener())
                .listener(employeeWriteListener()).tasklet((contribution, chunkContext) -> {
                    LOG.info("-------Tasklet completed--------");
                    return RepeatStatus.FINISHED;
                }).build();
    }

    @Bean
    public Step step2() {
        return stepBuilderFactory.get("Step2").listener(employeeStepListener()).listener(employeeReadListener())
                .listener(employeeWriteListener()).<String, String>chunk(3)
                .reader(new ListItemReader<>(Arrays.asList("11", "22", "33", "44", "55")))
                .processor(new ItemProcessor<String, String>() {
                    @Override
                    public String process(String item) throws Exception {
                        return String.valueOf(Integer.parseInt(item) * -1);
                    }
                }).writer(items -> {
                    for (String item : items) {
                        LOG.info("items : {} ", items);
                    }
                }).build();
    }

    @Bean
    public Step step3() {
        return this.stepBuilderFactory.get("Step3").listener(employeeStepListener()).listener(employeeReadListener())
                .listener(employeeWriteListener()).tasklet((contribution, chunkContext) -> {
                    LOG.info("Step3 tasklet.");
                    return RepeatStatus.FINISHED;
                }).build();
    }

    @Bean
    public Job job1() {
        return this.jobBuilderFactory.get("Job1").listener(employeeJobListener()).incrementer(new RunIdIncrementer())
                .start(step1()).next(step2()).build();
    }


    @Bean
    public Job job2() {
        return this.jobBuilderFactory.get("Job2").listener(employeeJobListener()).incrementer(new RunIdIncrementer())
                .start(step3()).build();
    }

    @Bean
    public Job job3() {
        Step step = stepBuilderFactory.get("Job3Step").listener(employeeReadListener()).listener(employeeStepListener())
                .listener(employeeWriteListener()).<Employee, Employee>chunk(1).reader(employeeJobReader()).processor(employeeJobProcessor())
                .writer(employeeJobWriter()).build();

        return jobBuilderFactory.get("Job3").listener(employeeJobListener()).start(step).build();
    }

    @Bean
    public ResourcelessTransactionManager transactionManager() {
        return new ResourcelessTransactionManager();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(15);
        taskExecutor.setMaxPoolSize(20);
        taskExecutor.setQueueCapacity(30);
        return taskExecutor;
    }

    @Bean
    public JobLauncher jobLauncher() throws Exception {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setTaskExecutor(taskExecutor());
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.afterPropertiesSet();
        return jobLauncher;
    }

    @Bean
    public JobOperator jobOperator(JobRegistry jobRegistry) throws Exception {
        SimpleJobOperator jobOperator = new SimpleJobOperator();
        jobOperator.setJobExplorer(jobExplorer);
        jobOperator.setJobLauncher(jobLauncher());
        jobOperator.setJobRegistry(jobRegistry);
        jobOperator.setJobRepository(jobRepository);
        return jobOperator;
    }

    @Bean
    public JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor(JobRegistry jobRegistry) {
        JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor = new JobRegistryBeanPostProcessor();
        jobRegistryBeanPostProcessor.setJobRegistry(jobRegistry);
        return jobRegistryBeanPostProcessor;
    }

}
