package learning.example.springbatch.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import javax.sql.DataSource;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.database.HibernateCursorItemReader;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

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
	@Qualifier("dbReadWriteStep")
	private Step dbReadWriteStep;
	
	@Before
	public void setUp() {
		jdbcTemplate.execute("delete from PERSON");
		jdbcTemplate.execute("delete from EMPLOYEE");
		jdbcTemplate.execute("insert into person (person_id, first_name, last_name, degree) values (1, '1st', 'Person', 'Science')");
		jdbcTemplate.execute("insert into person (person_id, first_name, last_name, degree) values (2, '2nd', 'Person', 'Commerce')");
		jdbcTemplate.execute("insert into person (person_id, first_name, last_name, degree) values (3, '3rd', 'Person', 'Business')");
		jdbcTemplate.execute("insert into person (person_id, first_name, last_name, degree) values (4, '4th', 'Person', 'Science')");
		jdbcTemplate.execute("insert into person (person_id, first_name, last_name, degree) values (5, '5th', 'Person', 'Business')");
		jdbcTemplate.execute("insert into person (person_id, first_name, last_name, degree) values (6, '6th', 'Person', 'Business')");
		jdbcTemplate.execute("insert into person (person_id, first_name, last_name, degree) values (7, '7th', 'Person', 'Commerce')");
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
		jdbcTemplate.execute("insert into person (person_id, first_name, last_name, degree) values (1, '1st', 'Person', 'Science')");
		jdbcTemplate.execute("insert into person (person_id, first_name, last_name, degree) values (2, '2nd', 'Person', 'Commerce')");
		jdbcTemplate.execute("insert into person (person_id, first_name, last_name, degree) values (3, '3rd', 'Person', 'Business')");
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
	public void testProcessorFilter() throws Exception {
		jdbcTemplate.execute("delete from PERSON");
		jdbcTemplate.execute("delete from EMPLOYEE");
		jdbcTemplate.execute("insert into person (person_id, first_name, last_name, degree) values (1, '1st', 'Person', 'Engineering')");
		JobExecution jobExecution = jobLauncherTestUtils.launchStep("dbReadWriteStep");
		for(StepExecution stepExecution : jobExecution.getStepExecutions()) {
			assertEquals("COMPLETED", stepExecution.getExitStatus().getExitCode());
			//BELOW: not processed because 'engineering' is a valid Degree but something that PersonToEmployeeConverter cannot map to a Department
			assertEquals(1, stepExecution.getReadCount());
			assertEquals(0, stepExecution.getWriteCount());
		}
		int employeeCount = jdbcTemplate.queryForObject("select count(*) from EMPLOYEE", Integer.class);
		assertEquals(0, employeeCount);
	}

}
