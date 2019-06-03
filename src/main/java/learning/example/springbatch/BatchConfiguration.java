package learning.example.springbatch;

import java.util.List;

import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceException;
import javax.sql.DataSource;

import org.hibernate.SessionFactory;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.HibernateCursorItemReader;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.builder.HibernateCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private DataSource dataSource;
	
	@Autowired
	private PlatformTransactionManager transactionManager; 
	
	@Autowired
	private EntityManagerFactory entityManagerFactory;

	private SessionFactory sessionFactory;
	
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
	public FlatFileItemReader<Employee> flatFileReader() {
		return new FlatFileItemReaderBuilder<Employee>().name("flatFileReader")
				.resource(new ClassPathResource("sample-data.csv")).delimited()
				.names(new String[] { "employeeId", "firstName", "lastName", "department" })
				.fieldSetMapper(new BeanWrapperFieldSetMapper<Employee>() {
					{
						setTargetType(Employee.class);
					}
				}).build();
	}
	
	@Bean(destroyMethod="")
	public JdbcCursorItemReader<Person> jdbcReader() {
		return new JdbcCursorItemReaderBuilder<Person>().dataSource(dataSource).name("jdbcReader")
				.sql("select * from person").rowMapper(new BeanPropertyRowMapper<>(Person.class)).build();
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
	public ItemWriter<Person> customWriter() {
		ItemWriter<Person> writer = (List<? extends Person> persons) -> {
			for (Person person : persons) {
				System.out.println("Processing " + person);
			}
		};
		return writer;
	}
	
	@Bean
	public JdbcBatchItemWriter<Employee> jdbcWriter(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<Employee>()
				.itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
				.sql("INSERT INTO employee (employee_id, first_name, last_name, department) VALUES (:employeeId, :firstName, :lastName, :department)")
				.dataSource(dataSource).build();
	}
	
//	@Bean
//	public HibernateItemWriter<Employee> hibernateWriter() {
//		return new HibernateItemWriterBuilder<Employee>().sessionFactory(sessionFactory).build();
//	}
	
	@Bean
	public JpaItemWriter<Employee> jpaWriter() {
		return new JpaItemWriterBuilder<Employee>().entityManagerFactory(entityManagerFactory).build();
	}
	
	@Bean
	public ChunkListener chunkListener() {
		return new CustomChunkListener();
	}

	@Bean
	public Job displayJob(Step step1) {
		return jobBuilderFactory.get("displayJob").incrementer(new RunIdIncrementer()).flow(step1).end().build();
	}

	@Bean
	public Step step1() {
		return stepBuilderFactory.get("step1")
				.transactionManager(transactionManager)
				.<Person, Employee>chunk(3)
				.reader(hibernateReader())
				.processor(processor())
				.writer(jpaWriter())
				.faultTolerant()
//				.skip(PersistenceException.class)
				.skipPolicy(constraintViolationExceptionSkipper)
				.skipLimit(1)
//				.listener(chunkListener())
				.build();
	}

}
