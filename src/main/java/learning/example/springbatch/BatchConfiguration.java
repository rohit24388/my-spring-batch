package learning.example.springbatch;

import javax.persistence.EntityManagerFactory;

import org.hibernate.SessionFactory;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {
	
	public static final String STEP_NAME = "dbReadWriteStep";
	public static final String JOB_NAME = "dbReadWriteJob";
	public static final int CHUNK_SIZE = 3;
	public static final int CORE_POOL_SIZE = 2;
	public static final int MAX_POOL_SIZE = 4;
	public static final int QUEUE_CAPACITY = 3;

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	
	private SessionFactory sessionFactory;
	
	@Autowired
	private PlatformTransactionManager transactionManager; 
 	
	@Autowired
	public BatchConfiguration(EntityManagerFactory entityManagerFactory) {
		if (entityManagerFactory.unwrap(SessionFactory.class) == null) {
			throw new NullPointerException("The factory is not a hibernate factory");
		}
		this.sessionFactory = entityManagerFactory.unwrap(SessionFactory.class);
	}
	
	@Bean
	public ItemReader<Person> reader() {
		return new PersonReader(sessionFactory);
	}
	
	@Bean
	public ItemProcessor<Person, Employee> processor() {
		return new PersonToEmployeeConverter();
	}
	
	@Bean
	public ItemWriter<Employee> writer(EntityManagerFactory entityManagerFactory) {
		return new EmployeeWriter(entityManagerFactory);
	}
	
	@Bean
	public TaskExecutor taskExecutor(){
		return new CustomThreadPoolTaskExecutor(CORE_POOL_SIZE, MAX_POOL_SIZE, QUEUE_CAPACITY);
	}
	
	@Bean
	public JobExecutionListener jobExecutionListener(ThreadPoolTaskExecutor executor) {
	    return new JobExecutionListener() {
	    	
	        private ThreadPoolTaskExecutor taskExecutor = executor;
	        
	        @Override
	        public void beforeJob(JobExecution jobExecution) {

	        }	        
	        
	        @Override
	        public void afterJob(JobExecution jobExecution) {
	            taskExecutor.shutdown();
	        }
	    };
	}
	
	@Bean
	public Step dbReadWriteStep(ItemWriter<Employee> writer, ChunkListener chunkListener, TaskExecutor taskExecutor) {
		return stepBuilderFactory.get(STEP_NAME)
				.transactionManager(transactionManager)
				.<Person, Employee>chunk(CHUNK_SIZE)
				.reader(reader())
				.processor(processor())
				.writer(writer)
				.taskExecutor(taskExecutor)
				.listener(chunkListener)
				.build();
	}	
	
	@Bean
	public Job dbReadWriteJob(Step dbReadWriteStep, JobExecutionListener listener) {
		return jobBuilderFactory.get(JOB_NAME)
				.incrementer(new RunIdIncrementer())
				.flow(dbReadWriteStep)
				.end()
				.listener(listener)
				.build();
	}	

}
