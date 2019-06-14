package learning.example.springbatch.test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.HibernateCursorItemReader;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import learning.example.springbatch.BatchConfiguration;
import learning.example.springbatch.Employee;
import learning.example.springbatch.Person;
import learning.example.springbatch.SpringBatchApplication;

@SpringBootTest
@SpringBatchTest
@RunWith(SpringRunner.class)
@ContextConfiguration(classes= {SpringBatchApplication.class})
public class DbReadWriteJobTests {

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
		jdbcTemplate.execute("insert into person (person_id, first_name, last_name, degree_major) values (1, '1st', 'Person', 'Science')");
		jdbcTemplate.execute("insert into person (person_id, first_name, last_name, degree_major) values (2, '2nd', 'Person', 'Commerce')");
		jdbcTemplate.execute("insert into person (person_id, first_name, last_name, degree_major) values (3, '3rd', 'Person', 'Business')");
		jdbcTemplate.execute("insert into person (person_id, first_name, last_name, degree_major) values (4, '4th', 'Person', 'Science')");
		jdbcTemplate.execute("insert into person (person_id, first_name, last_name, degree_major) values (5, '5th', 'Person', 'Business')");
		jdbcTemplate.execute("insert into person (person_id, first_name, last_name, degree_major) values (6, '6th', 'Person', 'Business')");
		jdbcTemplate.execute("insert into person (person_id, first_name, last_name, degree_major) values (7, '7th', 'Person', 'Commerce')");
	}
	
	@Test
	public void testStepAndJob() throws Exception {
		JobExecution jobExecution = jobLauncherTestUtils.launchJob();
		for(StepExecution stepExecution : jobExecution.getStepExecutions()) {
			assertEquals("COMPLETED", stepExecution.getExitStatus().getExitCode());
		}
		assertEquals("COMPLETED", jobExecution.getExitStatus().getExitCode());
	}
	
	@Test
	public void testStepOnly() throws Exception { //does not add value in case of a single-step job
		JobExecution jobExecution = jobLauncherTestUtils.launchStep("dbReadWriteStep");
		for(StepExecution stepExecution : jobExecution.getStepExecutions()) {
			assertEquals("COMPLETED", stepExecution.getExitStatus().getExitCode());
		}
		assertEquals("COMPLETED", jobExecution.getExitStatus().getExitCode());
	}
	
	@Test
	public void testHibernateReader() throws Exception {
		jdbcTemplate.execute("delete from PERSON");
		jdbcTemplate.execute("insert into person (person_id, first_name, last_name, degree_major) values (1, '1st', 'Person', 'Science')");
		jdbcTemplate.execute("insert into person (person_id, first_name, last_name, degree_major) values (2, '2nd', 'Person', 'Commerce')");
		jdbcTemplate.execute("insert into person (person_id, first_name, last_name, degree_major) values (3, '3rd', 'Person', 'Business')");
		ExecutionContext executionContext = new ExecutionContext();
		hibernateReader.open(executionContext);
		Person p1 = hibernateReader.read();
		Person p2 = hibernateReader.read();
		Person p3 = hibernateReader.read();
		assertNull(hibernateReader.read());
		hibernateReader.update(executionContext);
		hibernateReader.close();
		assertEquals("1st", p1.getFirstName());
		assertEquals("2nd", p2.getFirstName());
		assertEquals("3rd", p3.getFirstName());
	}
	
	@Test
	@Transactional
	public void testCustomJpaWriter() throws Exception {
		List<Employee> expectedEmployees = new ArrayList<Employee>();
		Employee e1 = new Employee(1, "1st", "Person", "IT");
		Employee e2 = new Employee(2, "2nd", "Person", "Agency");
		Employee e3 = new Employee(3, "3rd", "Person", "Actuary");
		expectedEmployees.add(e1);
		expectedEmployees.add(e2);
		expectedEmployees.add(e3);
		customJpaWriter.write(expectedEmployees);
		List<Employee> actualEmployees = jdbcTemplate.
				query("select person_id, first_name, last_name, department from EMPLOYEE order by person_id", 
						new BeanPropertyRowMapper<Employee>(Employee.class));
		assertArrayEquals(expectedEmployees.toArray(), actualEmployees.toArray());
	}
	
	@Test
	public void testSkipEqualToSkipLimit() throws Exception {
		//till this point, PERSON and EMPLOYEE tables have been created; PERSON table populated with the 7 records in setUp()
		JobExecution jobExecution = jobLauncherTestUtils.launchJob(); //all record in PERSON table should process and copy over to EMPLOYEE
		Integer[] expectedPersonIds = {1,2,3,4,5,6,7};
		List<Integer> actualPersonIds = jdbcTemplate.queryForList("select PERSON_ID from EMPLOYEE", Integer.class);
		assertArrayEquals(expectedPersonIds, actualPersonIds.toArray());
		int initialCount = jdbcTemplate.queryForObject("select count(*) from EMPLOYEE", Integer.class);
		assertEquals(7, initialCount);
		jdbcTemplate.execute("delete from EMPLOYEE where person_id in (1,2,4)"); //3 out 7 records deleted, leaving 4 records which is equal to SKIP_LIMIT
		int countAfterDelete = jdbcTemplate.queryForObject("select count(*) from EMPLOYEE", Integer.class);
		assertEquals(BatchConfiguration.SKIP_LIMIT, countAfterDelete);
		jobExecution = jobLauncherTestUtils.launchJob(); //rerun of the job must skip the records already present in EMPLOYEE
		for(StepExecution stepExecution : jobExecution.getStepExecutions()) {
			assertEquals("COMPLETED", stepExecution.getExitStatus().getExitCode()); //Step status
			assertEquals(BatchConfiguration.SKIP_LIMIT, stepExecution.getWriteSkipCount());
		}
		assertEquals("COMPLETED", jobExecution.getExitStatus().getExitCode()); //Job status
	}	
	
	@Test
	public void testSkipGreaterThanSkipLimit() throws Exception {
		//till this point, PERSON and EMPLOYEE tables have been created; PERSON table populated with the 7 records in setUp()
		JobExecution jobExecution = jobLauncherTestUtils.launchJob(); //all record in PERSON table should process and copy over to EMPLOYEE
		Integer[] expectedPersonIds = {1,2,3,4,5,6,7};
		List<Integer> actualPersonIds = jdbcTemplate.queryForList("select PERSON_ID from EMPLOYEE", Integer.class);
		assertArrayEquals(expectedPersonIds, actualPersonIds.toArray());
		int initialCount = jdbcTemplate.queryForObject("select count(*) from EMPLOYEE", Integer.class);
		assertEquals(7, initialCount);
		jdbcTemplate.execute("delete from EMPLOYEE where person_id in (1,4)"); //2 out 7 records deleted, leaving 5 records which is greater SKIP_LIMIT
		int countAfterDelete = jdbcTemplate.queryForObject("select count(*) from EMPLOYEE", Integer.class);
		assertTrue(countAfterDelete > BatchConfiguration.SKIP_LIMIT);
		jobExecution = jobLauncherTestUtils.launchJob();
		for(StepExecution stepExecution : jobExecution.getStepExecutions()) {
			assertEquals("FAILED", stepExecution.getExitStatus().getExitCode()); //Step status
			assertEquals(BatchConfiguration.SKIP_LIMIT, stepExecution.getWriteSkipCount()); //SKIP_LIMIT was already reached when there was one more record to skip
		}
		assertEquals("FAILED", jobExecution.getExitStatus().getExitCode()); //Job status
	}
	
	@Test
	public void testUnskippableExceptionAtProcessorLevel() throws Exception {
		//till this point, PERSON and EMPLOYEE tables have been created; PERSON table populated with the 7 records in setUp()
		//the following SQL is inserting a degree_major of 'Hospitality' which is not a part of DegreeMajor enum, and hence should not be processed
		jdbcTemplate.execute("insert into person (person_id, first_name, last_name, degree_major) values (100, '100th', 'Person', 'Hospitality')");
		JobExecution jobExecution = jobLauncherTestUtils.launchStep("dbReadWriteStep");
		for(StepExecution stepExecution : jobExecution.getStepExecutions()) {
			assertEquals("FAILED", stepExecution.getExitStatus().getExitCode());
			assertEquals(0, stepExecution.getWriteSkipCount()); //not skipped because ConstarintViolationException is the only skippable exception
		}
	}
	
	/** 
	 * can only be run in isolation 
	 * until someone thinks of a better way to trigger a non-ConstraintViolation Persistence exception in CustomJpaWriter than dropping EMPLOYEE table
	 * @throws Exception
	 */
	//@Test
	public void testUnskippableExceptionAtWriterLevel() throws Exception {
		//till this point, PERSON and EMPLOYEE tables have been created; PERSON table populated with the 7 records in setUp()
		//the following SQL drops EMPLOYEE table so as to force an exception when the writer executes
		jdbcTemplate.execute("drop table EMPLOYEE");
		JobExecution jobExecution = jobLauncherTestUtils.launchStep("dbReadWriteStep");
		for(StepExecution stepExecution : jobExecution.getStepExecutions()) {
			assertEquals("FAILED", stepExecution.getExitStatus().getExitCode());
			assertEquals(0, stepExecution.getWriteSkipCount()); //not skipped because ConstarintViolationException is the only skippable exception
		}
	}
	
	@Test
	public void testProcessorFilter() throws Exception {
		jdbcTemplate.execute("delete from PERSON");
		jdbcTemplate.execute("delete from EMPLOYEE");
		jdbcTemplate.execute("insert into person (person_id, first_name, last_name, degree_major) values (1, '1st', 'Person', 'Engineering')");
		JobExecution jobExecution = jobLauncherTestUtils.launchStep("dbReadWriteStep");
		for(StepExecution stepExecution : jobExecution.getStepExecutions()) {
			assertEquals("COMPLETED", stepExecution.getExitStatus().getExitCode());
			//BELOW: not processed because 'engineering' is a valid Degree/Major but something that PersonToEmployeeConverter cannot map to a Department
			assertEquals(1, stepExecution.getReadCount());
			assertEquals(0, stepExecution.getWriteCount());
		}
		int employeeCount = jdbcTemplate.queryForObject("select count(*) from EMPLOYEE", Integer.class);
		assertEquals(0, employeeCount);
	}
	
	//@Test
	public void testDefaultRetryLimit() throws Exception {
		
	}
	
	//@Test
	public void testCustomRetryExceptionType() throws Exception {
		
	}
	
	//@Test
	public void testCustomRetryLimit() throws Exception {
		
	}
	
	//@Test
	public void testSkipLog() throws Exception {
		
	}
	
	//@Test
	public void testRetryLog() throws Exception {
		
	}
	
	//@Test
	public void testRetryExhaustLog() throws Exception {
		
	}
	
	//@Test
	public void testRestart() throws Exception {
		
	}

}
