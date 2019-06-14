package learning.example.springbatch;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;

import org.hibernate.SessionFactory;
import org.hibernate.exception.ConstraintViolationException;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.HibernateCursorItemReader;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.HibernateCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.PassThroughLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {
	
	public static final int CHUNK_SIZE = 3;
	public static final int SKIP_LIMIT = 4;
	public static final String CSV_FILE_PATH = "target/test-outputs/employeeDetails.csv";  

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	private DataSource dataSource;
	
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
				.queryString("from Person p order by p.personId").build();
	}
	
	@Bean(destroyMethod = "")
	public JdbcCursorItemReader<EmployeeDetails> jdbcReader() {
		return new JdbcCursorItemReaderBuilder<EmployeeDetails>().dataSource(dataSource).name("jdbcReader")
				.sql("select e.*, p.degree_major from employee e join person p on e.person_id = p.person_id order by e.person_id")
				.rowMapper(new BeanPropertyRowMapper<>(EmployeeDetails.class)).build();
	}
	
	@Bean
	public ItemProcessor<Person, Employee> processor() {
		return new PersonToEmployeeConverter();
	}
	
	@Bean
	public FlatFileItemWriter<EmployeeDetails> textFileWriter() {
		return new FlatFileItemWriterBuilder<EmployeeDetails>().name("flatFileWriter")
				.resource(new FileSystemResource("target/test-outputs/employeeDetails.txt"))
				.lineAggregator(new PassThroughLineAggregator<>()).build();
	}
	
	@Bean
	DelimitedLineAggregator<EmployeeDetails> delimitedLineAggregator() {
		return new DelimitedLineAggregator<EmployeeDetails>() {
            {
                setDelimiter(",");
                setFieldExtractor(new BeanWrapperFieldExtractor<EmployeeDetails>() {
                    {
                        setNames(new String[] { "employeeId", "firstName", "lastName", "department", "personId", "degreeMajor" });
                    }
                });
            }
        };
	}
	
	@Bean
	public FlatFileItemWriter<EmployeeDetails> csvWriter() {
		return new FlatFileItemWriterBuilder<EmployeeDetails>().name("flatFileWriter")
				.resource(new FileSystemResource(CSV_FILE_PATH))
				.lineAggregator(delimitedLineAggregator()).build();
	}

	@Bean
	public Step dbReadWriteStep(ItemWriter<Employee> customJpaWriter, ChunkListener chunkListener) {
		return stepBuilderFactory.get("dbReadWriteStep")
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
	
	@Bean
	public Step dbReadCsvWriteStep() {
		return stepBuilderFactory.get("dbReadFlatFileWriteStep")
				.transactionManager(transactionManager)
				.<EmployeeDetails, EmployeeDetails>chunk(3)
				.reader(jdbcReader())
				.writer(csvWriter())
				.build();
	}
	
	//@Bean
	public Job dbReadWriteJob(Step dbReadWriteStep, @Qualifier("myJobListener") JobExecutionListener listener) {
		return jobBuilderFactory.get("dbReadWriteJob")
				.incrementer(new RunIdIncrementer())
				.flow(dbReadWriteStep)
				.end()
				.listener(listener)
				.build();
	}	
	
	@Bean
	public Job dbWriteAndMergeJob(Step dbReadWriteStep) {
		return jobBuilderFactory.get("dbWriteAndMergeJob").incrementer(new RunIdIncrementer())
				.start(dbReadWriteStep).on("*").to(dbReadCsvWriteStep())
				.end()
				.build();
	}

}
