package learning.example.springbatch;

import javax.persistence.EntityManagerFactory;

import org.hibernate.SessionFactory;
import org.hibernate.exception.ConstraintViolationException;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.HibernateCursorItemReader;
import org.springframework.batch.item.database.builder.HibernateCursorItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {
	
	public static final int CHUNK_SIZE = 3;
	public static final int SKIP_LIMIT = 2;

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	@Qualifier("jobListener")
	private CustomJobExecutionListener jobListener;

	private SessionFactory sessionFactory;
	
	@Autowired
	private PlatformTransactionManager transactionManager; 
 	
	@Autowired
	ConstraintViolationExceptionSkipper constraintViolationExceptionSkipper;
	
	@Autowired
	public BatchConfiguration(EntityManagerFactory entityManagerFactory) {
		if (entityManagerFactory.unwrap(SessionFactory.class) == null) {
			throw new NullPointerException("The factory is not a hibernate factory");
		}
		this.sessionFactory = entityManagerFactory.unwrap(SessionFactory.class);
	}

	@Bean
	public HibernateCursorItemReader<Person> hibernateReader() {
		return new HibernateCursorItemReaderBuilder<Person>().name("hibernateReader").sessionFactory(sessionFactory)
				.queryString("from Person").build();
	}
	
	@Bean
	public ItemProcessor<Person, Employee> processor() {
		return new PersonToEmployeeConverter();
	}
	
	@Bean
	public Job dbWriteJob(Step step1) {
		return jobBuilderFactory.get("dbWriteJob")
				.incrementer(new RunIdIncrementer())
				.flow(step1)
				.end()
				.build();
	}

	@Bean
	public Step step1(ItemWriter<Employee> customJpaWriter, ChunkListener chunkListener) {
		return stepBuilderFactory.get("step1")
				.transactionManager(transactionManager)
				.<Person, Employee>chunk(CHUNK_SIZE)
				.reader(hibernateReader())
				.processor(processor())
				.writer(customJpaWriter)
				.faultTolerant()
				.skip(ConstraintViolationException.class)
				.skipLimit(SKIP_LIMIT)
				.listener(chunkListener)
				.build();
	}

}
