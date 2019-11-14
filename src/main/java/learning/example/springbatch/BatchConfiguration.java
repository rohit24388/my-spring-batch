package learning.example.springbatch;

import javax.persistence.EntityManagerFactory;

import org.hibernate.SessionFactory;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Job;
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
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {
	
	public static final int CHUNK_SIZE = 3;
	public static final int MAXIMUM_THREAD_COUNT = 2;
	public static final String TEXT_FILE_PATH = "target/test-outputs/employeeDetails.txt";
	public static final String CSV_FILE_PATH = "target/test-outputs/employeeDetails.csv";

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
	
//	@Bean
//	public ItemWriter<Employee> writer(EntityManagerFactory entityManagerFactory) {
//		return new JpaItemWriterBuilder<Employee>().entityManagerFactory(entityManagerFactory).build();
//	}
		
	@Bean
	public ItemWriter<Employee> writer(EntityManagerFactory entityManagerFactory) {
		return new EmployeeWriter(entityManagerFactory);
	}
	
	@Bean
	public TaskExecutor taskExecutor(){
		return new SimpleAsyncTaskExecutor("my_spring_batch");
	}
	
	@Bean
	public Step dbReadWriteStep(ItemWriter<Employee> writer, ChunkListener chunkListener) {
		return stepBuilderFactory.get("dbReadWriteStep")
				.transactionManager(transactionManager)
				.<Person, Employee>chunk(CHUNK_SIZE)
				.reader(reader())
				.processor(processor())
				.writer(writer)
				.taskExecutor(taskExecutor())
				.throttleLimit(2)
				.listener(chunkListener)
				.build();
	}	
	
	@Bean
	public Job dbReadWriteJob(Step dbReadWriteStep) {
		return jobBuilderFactory.get("dbReadWriteJob")
				.incrementer(new RunIdIncrementer())
				.flow(dbReadWriteStep)
				.end()
				.build();
	}	

}
