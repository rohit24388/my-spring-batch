package learning.example.springbatch.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.database.HibernateCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.MetaDataInstanceFactory;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import learning.example.springbatch.BatchConfiguration;
import learning.example.springbatch.Employee;
import learning.example.springbatch.EmployeeDetails;
import learning.example.springbatch.Person;
import learning.example.springbatch.SpringBatchApplication;

@SpringBootTest
@SpringBatchTest
@RunWith(SpringRunner.class)
@ContextConfiguration(classes= {SpringBatchApplication.class})
public class DbReadCsvWriteJobTests {
	
	@Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;
	
	private JdbcTemplate jdbcTemplate;
	
	@Autowired
    public void setDataSource(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }
	
	@Autowired
	@Qualifier("hibernateReader")
	private HibernateCursorItemReader<Person> hibernateReader;
	
	@Autowired
	@Qualifier("customJpaWriter")
	private ItemWriter<Employee> customJpaWriter;
	
	@Autowired
	@Qualifier("dbReadWriteStep")
	private Step dbReadWriteStep;
	
	@Before
	public void setUp() {
		jdbcTemplate.execute("delete from PERSON");
		jdbcTemplate.execute("delete from EMPLOYEE");
		File flatFile = new File("target/test-outputs/employeeDetails.csv");
		if(flatFile.exists()) {
			flatFile.delete();
		}
		jdbcTemplate.execute("insert into person (person_id, first_name, last_name, degree_major) values (1, '1st', 'Person', 'Science')");
		jdbcTemplate.execute("insert into person (person_id, first_name, last_name, degree_major) values (2, '2nd', 'Person', 'Commerce')");
	}
	
	@Test
	public void testStepAndJob() throws Exception {
		JobExecution jobExecution = jobLauncherTestUtils.launchJob();
		for(StepExecution stepExecution : jobExecution.getStepExecutions()) {
			assertEquals("COMPLETED", stepExecution.getExitStatus().getExitCode()); //test for success of all steps
		}
		assertEquals("COMPLETED", jobExecution.getExitStatus().getExitCode()); //test success for the overall job
		File csvFile = new File(BatchConfiguration.CSV_FILE_PATH);
		assertTrue(csvFile.exists()); //file was created by the dbReadCsvWriteStep step
		assertNotEquals(0, csvFile.length()); //file is not empty
		assertEquals(2, countLinesInCsvFile()); //since PERSON table contains 2 rows
		List<EmployeeDetails> employeeDetailsList = readEmployeeDetailsCsvFile(); //retrieve all EmployeDetails objects from employeeDetails.csv
		assertEquals(2, employeeDetailsList.size());
		EmployeeDetails secondEmployeeDetails = employeeDetailsList.get(1);
		assertEquals(2, secondEmployeeDetails.getEmployeeId());
		assertEquals("2nd", secondEmployeeDetails.getFirstName());
		assertEquals("Person", secondEmployeeDetails.getLastName());
		assertEquals("Commerce", secondEmployeeDetails.getDegreeMajor());
	}
	
	@Test
	public void testDbReadWriteStepOnly() throws Exception {
		JobExecution jobExecution = jobLauncherTestUtils.launchStep("dbReadWriteStep");
		for(StepExecution stepExecution : jobExecution.getStepExecutions()) {
			assertEquals("COMPLETED", stepExecution.getExitStatus().getExitCode()); //step status
		}
		assertEquals("COMPLETED", jobExecution.getExitStatus().getExitCode()); //job status
		File csvFile = new File(BatchConfiguration.CSV_FILE_PATH);
		assertFalse(csvFile.exists()); //the flat file could not be created because the corresponding step did not run
	}
	
	@Test
	public void testDbReadCsvWriteStepOnly() throws Exception {
		JobExecution jobExecution = jobLauncherTestUtils.launchStep("dbReadFlatFileWriteStep");
		for(StepExecution stepExecution : jobExecution.getStepExecutions()) {
			assertEquals("COMPLETED", stepExecution.getExitStatus().getExitCode()); //step status
		}
		assertEquals("COMPLETED", jobExecution.getExitStatus().getExitCode()); //job status
		int employeeCount = jdbcTemplate.queryForObject("select count(*) from EMPLOYEE", Integer.class);
		assertEquals(0, employeeCount);
		File csvFile = new File(BatchConfiguration.CSV_FILE_PATH);
		assertEquals(0, csvFile.length()); //the file is empty because one of the source tables, 
												//namely EMPLOYEE is empty because the first step 'dbReadWriteStep' did not run
	}
	
	private int countLinesInCsvFile() throws UnexpectedInputException, ParseException, Exception {
		FlatFileItemReader<EmployeeDetails> csvReader = getCsvReader();
		csvReader.open(MetaDataInstanceFactory.createStepExecution().getExecutionContext());
		int numberOfLines = 0;
		EmployeeDetails employeeDetails;
		do {
			employeeDetails = csvReader.read();
			if(employeeDetails != null) {
				numberOfLines++;
			}
		} while(employeeDetails != null);
		csvReader.close();
		return numberOfLines;
	}
		
	private List<EmployeeDetails> readEmployeeDetailsCsvFile() throws UnexpectedInputException, ParseException, Exception {
		FlatFileItemReader<EmployeeDetails> csvReader = getCsvReader();
		csvReader.open(MetaDataInstanceFactory.createStepExecution().getExecutionContext());
		List<EmployeeDetails> employeeDetailsList = new ArrayList<>();
		EmployeeDetails employeeDetails;
		do {
			employeeDetails = csvReader.read();
			if(employeeDetails != null) {
				employeeDetailsList.add(employeeDetails);
			}
		} while(employeeDetails != null);
		csvReader.close();
		return employeeDetailsList;
	}

	private FlatFileItemReader<EmployeeDetails> getCsvReader() {
		return new FlatFileItemReaderBuilder<EmployeeDetails>().name("flatFileReader")
				.resource(new FileSystemResource(BatchConfiguration.CSV_FILE_PATH)).delimited()
				.names(new String[] { "employeeId", "firstName", "lastName", "department", "personId", "degreeMajor" })
				.fieldSetMapper(new BeanWrapperFieldSetMapper<EmployeeDetails>() {
					{
						setTargetType(EmployeeDetails.class);
					}
				}).build();
	}
}
