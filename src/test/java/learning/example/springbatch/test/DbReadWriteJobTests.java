package learning.example.springbatch.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import javax.sql.DataSource;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import learning.example.springbatch.BatchConfiguration;
import learning.example.springbatch.Person;
import learning.example.springbatch.PersonReader;
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
	private PersonReader reader;
	
	@Before
	public void setUp() {
		jdbcTemplate.execute("delete from PERSON");
		jdbcTemplate.execute("delete from EMPLOYEE");
	}
	
	@Test
	public void testStep() throws Exception {
		
		for(int i=1; i<=1000; i++) {
			jdbcTemplate.update("insert into person (person_id, first_name, last_name, degree) values (?, 'Person', ?, 'Science')", i, i);
		}
		JobExecution jobExecution = jobLauncherTestUtils.launchStep(BatchConfiguration.STEP_NAME);
		for(StepExecution stepExecution : jobExecution.getStepExecutions()) {
			assertEquals("COMPLETED", stepExecution.getExitStatus().getExitCode());
		}
		assertEquals("COMPLETED", jobExecution.getExitStatus().getExitCode());
	}
	
	@Test
	public void testReader() throws Exception {
		jdbcTemplate.execute("insert into person (person_id, first_name, last_name, degree) values (1, '1st', 'Person', 'Science')");
		jdbcTemplate.execute("insert into person (person_id, first_name, last_name, degree) values (2, '2nd', 'Person', 'Commerce')");
		jdbcTemplate.execute("insert into person (person_id, first_name, last_name, degree) values (3, '3rd', 'Person', 'Business')");
		ExecutionContext executionContext = new ExecutionContext();
		reader.open(executionContext);
		Person p1 = reader.read();
		Person p2 = reader.read();
		Person p3 = reader.read();
		assertNull(reader.read());
		reader.update(executionContext);
		reader.close();
		assertEquals("1st", p1.getFirstName());
		assertEquals("2nd", p2.getFirstName());
		assertEquals("3rd", p3.getFirstName());
	}
	
	@Test
	public void testProcessorFilter() throws Exception {
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
	
	@After
	public void tearDown() {
		jdbcTemplate.execute("delete from PERSON");
		jdbcTemplate.execute("delete from EMPLOYEE");
	}

}
