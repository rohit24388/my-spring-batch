package learning.example.springbatch;

import java.util.List;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;

import org.hibernate.SessionFactory;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobOperator;
import org.springframework.batch.core.repository.JobRepository;
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
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.transform.PassThroughLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
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
	@Qualifier("jobListener")
	private CustomJobExecutionListener jobListener;

	@Autowired
	private DataSource dataSource;
	
	@Autowired
	private EntityManagerFactory entityManagerFactory;

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
	public JobOperator jobOperator(final JobLauncher jobLauncher, final JobRepository jobRepository,
	        final JobRegistry jobRegistry, final JobExplorer jobExplorer) {
	    final SimpleJobOperator jobOperator = new SimpleJobOperator();
	    jobOperator.setJobLauncher(jobLauncher);
	    jobOperator.setJobRepository(jobRepository);
	    jobOperator.setJobRegistry(jobRegistry);
	    jobOperator.setJobExplorer(jobExplorer);
	    return jobOperator;
	}

	@Bean
	public JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor(JobRegistry jobRegistry) {
	    JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor = new JobRegistryBeanPostProcessor();
	    jobRegistryBeanPostProcessor.setJobRegistry(jobRegistry);
	    return jobRegistryBeanPostProcessor;
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
	
	@Bean(destroyMethod = "")
	public JdbcCursorItemReader<EmployeeDetails> jdbcReader() {
		return new JdbcCursorItemReaderBuilder<EmployeeDetails>().dataSource(dataSource).name("jdbcReader")
				.sql("select e.*, p.degree_major from employee e join person p on e.person_id = p.person_id")
				.rowMapper(new BeanPropertyRowMapper<>(EmployeeDetails.class)).build();
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
	public FlatFileItemWriter<EmployeeDetails> flatFileWriter() {
		return new FlatFileItemWriterBuilder<EmployeeDetails>().name("flatFileWriter")
				.resource(new FileSystemResource("target/test-outputs/employeeDetails.txt"))
				.lineAggregator(new PassThroughLineAggregator<>()).build();
	}

	@Bean
	public ChunkListener chunkListener() {
		return new CustomChunkListener();
	}
	
//	@Bean
//	public JobExecutionListener jobListener() {
//		return new CustomJobExecutionListener();
//	}

	@Bean
	public Job dbWriteJob(Step step1) {
		return jobBuilderFactory.get("dbWriteJob")
				.incrementer(new RunIdIncrementer())
				.flow(step1)
				.end()
				.listener(jobListener)
				.build();
	}
//	
//	@Bean
//	public Job mergeJob(Step step2) {
//		return jobBuilderFactory.get("mergeJob").incrementer(new RunIdIncrementer()).flow(step2).end().build();
//	}
	
//	@Bean
//	public Job dbWriteAndMergeJob(Step step1, Step step2) {
//		return jobBuilderFactory.get("dbWriteAndMergeJob").incrementer(new RunIdIncrementer())
//				.start(step1).on("*").to(step2)
//				.end()
//				.build();
//	}

	@Bean
	public Step step1() {
		return stepBuilderFactory.get("step1")
				.transactionManager(transactionManager)
				.<Person, Employee>chunk(3)
				.reader(hibernateReader())
				.processor(processor())
				.writer(jpaWriter())
//				.faultTolerant()
//				.skipPolicy(constraintViolationExceptionSkipper)
//				.skipLimit(1)
				.listener(chunkListener())
				.build();
	}
	
	@Bean
	public Step step2() {
		return stepBuilderFactory.get("step2")
				.transactionManager(transactionManager)
				.<EmployeeDetails, EmployeeDetails>chunk(3)
				.reader(jdbcReader())
//				.processor(processor())
				.writer(flatFileWriter())
//				.faultTolerant()
//				.skipPolicy(constraintViolationExceptionSkipper)
//				.skipLimit(1)
//				.listener(chunkListener())
				.build();
	}

}
