package learning.example.springbatch.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
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
import org.springframework.batch.item.file.mapping.PassThroughFieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
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
@ContextConfiguration(classes = { SpringBatchApplication.class })
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
		File csvFile = new File(BatchConfiguration.CSV_FILE_PATH);
		if (csvFile.exists()) {
			csvFile.delete();
		}
		File textFile = new File(BatchConfiguration.TEXT_FILE_PATH);
		if (textFile.exists()) {
			textFile.delete();
		}
		jdbcTemplate.execute(
				"insert into person (person_id, first_name, last_name, degree_major) values (1, '1st', 'Person', 'Science')");
		jdbcTemplate.execute(
				"insert into person (person_id, first_name, last_name, degree_major) values (2, '2nd', 'Person', 'Commerce')");
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
		List<EmployeeDetails> employeeDetailsList = readEmployeeDetailsFromCsvFile(); //retrieve all EmployeDetails objects from employeeDetails.csv
		assertEquals(2, employeeDetailsList.size());
		EmployeeDetails secondEmployeeDetails = employeeDetailsList.get(1);
		assertEquals(2, secondEmployeeDetails.getEmployeeId());
		assertEquals("2nd", secondEmployeeDetails.getFirstName());
		assertEquals("Person", secondEmployeeDetails.getLastName());
		assertEquals("Commerce", secondEmployeeDetails.getDegreeMajor());
	}

//	@Test
//	public void testStepAndJob() throws Exception {
//		JobExecution jobExecution = jobLauncherTestUtils.launchJob();
//		for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
//			assertEquals("COMPLETED", stepExecution.getExitStatus().getExitCode()); // test for success of all steps
//		}
//		assertEquals("COMPLETED", jobExecution.getExitStatus().getExitCode()); // test success for the overall job
//		File textFile = new File(BatchConfiguration.TEXT_FILE_PATH);
//		assertTrue(textFile.exists()); // file was created by the dbReadCsvWriteStep step
//		assertNotEquals(0, textFile.length()); // file is not empty
//		assertEquals(2, countLinesInTextFile()); // since PERSON table contains 2 rows
//		List<EmployeeDetails> employeeDetailsList = readEmployeeDetailsFromTextFile(); // retrieve all EmployeDetails
//																						// objects from
//																						// employeeDetails.txt
//		assertEquals(2, employeeDetailsList.size());
//		EmployeeDetails secondEmployeeDetails = employeeDetailsList.get(1);
//		assertEquals(2, secondEmployeeDetails.getEmployeeId());
//		assertEquals("2nd", secondEmployeeDetails.getFirstName());
//		assertEquals("Person", secondEmployeeDetails.getLastName());
//		assertEquals("Commerce", secondEmployeeDetails.getDegreeMajor());
//	}

	@Test
	public void testDbReadWriteStepOnly() throws Exception {
		JobExecution jobExecution = jobLauncherTestUtils.launchStep("dbReadWriteStep");
		for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
			assertEquals("COMPLETED", stepExecution.getExitStatus().getExitCode()); // step status
		}
		assertEquals("COMPLETED", jobExecution.getExitStatus().getExitCode()); // job status
		File csvFile = new File(BatchConfiguration.CSV_FILE_PATH);
		assertFalse(csvFile.exists()); // the flat file could not be created because the corresponding step did not run
	}

	@Test
	public void testDbReadCsvWriteStepOnly() throws Exception {
		JobExecution jobExecution = jobLauncherTestUtils.launchStep("dbReadFlatFileWriteStep");
		for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
			assertEquals("COMPLETED", stepExecution.getExitStatus().getExitCode()); // step status
		}
		assertEquals("COMPLETED", jobExecution.getExitStatus().getExitCode()); // job status
		int employeeCount = jdbcTemplate.queryForObject("select count(*) from EMPLOYEE", Integer.class);
		assertEquals(0, employeeCount);
		File csvFile = new File(BatchConfiguration.CSV_FILE_PATH);
		assertEquals(0, csvFile.length()); // the file is empty because one of the source tables,
											// namely EMPLOYEE is empty because the first step 'dbReadWriteStep' did not
											// run
	}

	private int countLinesInCsvFile() throws UnexpectedInputException, ParseException, Exception {
		FlatFileItemReader<EmployeeDetails> csvReader = getCsvReader();
		csvReader.open(MetaDataInstanceFactory.createStepExecution().getExecutionContext());
		int numberOfLines = 0;
		EmployeeDetails employeeDetails;
		do {
			employeeDetails = csvReader.read();
			if (employeeDetails != null) {
				numberOfLines++;
			}
		} while (employeeDetails != null);
		csvReader.close();
		return numberOfLines;
	}

	private int countLinesInTextFile() throws UnexpectedInputException, ParseException, Exception {
		FlatFileItemReader<FieldSet> textReader = getTextReader();
		textReader.open(MetaDataInstanceFactory.createStepExecution().getExecutionContext());
		int numberOfLines = 0;
		FieldSet fieldSet;
		do {
			fieldSet = textReader.read();
			if (fieldSet != null) {
				numberOfLines++;
			}
		} while (fieldSet != null);
		textReader.close();
		return numberOfLines;
	}

	private List<EmployeeDetails> readEmployeeDetailsFromCsvFile()
			throws UnexpectedInputException, ParseException, Exception {
		FlatFileItemReader<EmployeeDetails> csvReader = getCsvReader();
		csvReader.open(MetaDataInstanceFactory.createStepExecution().getExecutionContext());
		List<EmployeeDetails> employeeDetailsList = new ArrayList<>();
		EmployeeDetails employeeDetails;
		do {
			employeeDetails = csvReader.read();
			if (employeeDetails != null) {
				employeeDetailsList.add(employeeDetails);
			}
		} while (employeeDetails != null);
		csvReader.close();
		return employeeDetailsList;
	}

	private List<EmployeeDetails> readEmployeeDetailsFromTextFile()
			throws UnexpectedInputException, ParseException, Exception {
		FlatFileItemReader<FieldSet> textReader = getTextReader();
		textReader.open(MetaDataInstanceFactory.createStepExecution().getExecutionContext());
		List<EmployeeDetails> employeeDetailsList = new ArrayList<>();
		FieldSet fieldSet;
		do {
			fieldSet = textReader.read();
			if (fieldSet != null) {
				EmployeeDetails employeeDetails = mapFieldSetToEmployeeDetails(fieldSet);
				employeeDetailsList.add(employeeDetails);
			}
		} while (fieldSet != null);
		textReader.close();
		return employeeDetailsList;
	}

	private EmployeeDetails mapFieldSetToEmployeeDetails(FieldSet fieldSet) {
		EmployeeDetails employeeDetails = new EmployeeDetails();
		String[] values = fieldSet.getValues();
		employeeDetails.setEmployeeId(Integer.valueOf(StringUtils.substringAfter(values[0], ": ")));
		employeeDetails.setFirstName(StringUtils.substringAfter(values[1], ": "));
		employeeDetails.setLastName(StringUtils.substringAfter(values[2], ": "));
		employeeDetails.setDepartment(StringUtils.substringAfter(values[3], ": "));
		employeeDetails.setPersonId(Integer.valueOf(StringUtils.substringAfter(values[4], ": ")));
		employeeDetails.setDegreeMajor(StringUtils.substringAfter(values[5], ": "));
		return employeeDetails;
	}

	private FlatFileItemReader<EmployeeDetails> getCsvReader() {
		return new FlatFileItemReaderBuilder<EmployeeDetails>().name("csvReader")
				.resource(new FileSystemResource(BatchConfiguration.CSV_FILE_PATH)).delimited()
				.names(new String[] { "employeeId", "firstName", "lastName", "department", "personId", "degreeMajor" })
				.fieldSetMapper(new BeanWrapperFieldSetMapper<EmployeeDetails>() {
					{
						setTargetType(EmployeeDetails.class);
					}
				})
				.linesToSkip(1)
				.build();
	}

	private FlatFileItemReader<FieldSet> getTextReader() {
		return new FlatFileItemReaderBuilder<FieldSet>().name("textReader")
				.resource(new FileSystemResource(BatchConfiguration.TEXT_FILE_PATH))
				.lineTokenizer(new DelimitedLineTokenizer())
				.fieldSetMapper(new PassThroughFieldSetMapper())
				.build();
	}
}
