package learning.example.springbatch.test;

import java.util.List;

import javax.sql.DataSource;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import learning.example.springbatch.BatchConfiguration;
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
	
	@Test
	public void testJob() throws Exception {
		JobExecution jobExecution = jobLauncherTestUtils.launchJob();
		Assert.assertEquals("COMPLETED", jobExecution.getExitStatus().getExitCode());
	}
	
	@Test
	public void testSkipEqualToSkipLimit() throws Exception {
		JobExecution jobExecution = jobLauncherTestUtils.launchJob();
		Long[] expectedPersonIds = {1l,2l,3l,4l,5l,6l,7l};
		List<Long> actualPersonIds = jdbcTemplate.queryForList("select PERSON_ID from EMPLOYEE", Long.class);
		Assert.assertArrayEquals(expectedPersonIds, actualPersonIds.toArray());
		int initialCount = jdbcTemplate.queryForObject("select count(*) from EMPLOYEE", Integer.class);
		Assert.assertEquals(7, initialCount);
		jdbcTemplate.execute("delete from EMPLOYEE where person_id in (1,2,4)");
		int countAfterDelete = jdbcTemplate.queryForObject("select count(*) from EMPLOYEE", Integer.class);
		Assert.assertEquals(BatchConfiguration.SKIP_LIMIT, countAfterDelete);
		jobExecution = jobLauncherTestUtils.launchJob();
		Assert.assertEquals("COMPLETED", jobExecution.getExitStatus().getExitCode());
	}	
	
	@Test
	public void testSkipGreaterThanSkipLimit() throws Exception {
		JobExecution jobExecution = jobLauncherTestUtils.launchJob();
		Long[] expectedPersonIds = {1l,2l,3l,4l,5l,6l,7l};
		List<Long> actualPersonIds = jdbcTemplate.queryForList("select PERSON_ID from EMPLOYEE", Long.class);
		Assert.assertArrayEquals(expectedPersonIds, actualPersonIds.toArray());
		int initialCount = jdbcTemplate.queryForObject("select count(*) from EMPLOYEE", Integer.class);
		Assert.assertEquals(7, initialCount);
		jdbcTemplate.execute("delete from EMPLOYEE where person_id in (1,4)");
		int countAfterDelete = jdbcTemplate.queryForObject("select count(*) from EMPLOYEE", Integer.class);
		Assert.assertTrue(countAfterDelete > BatchConfiguration.SKIP_LIMIT);
		jobExecution = jobLauncherTestUtils.launchJob();
		Assert.assertEquals("FAILED", jobExecution.getExitStatus().getExitCode());
	}
	
	@Test
	public void testSkipExceptionType() throws Exception {
		//POSITIVE
		//NEGATIVE
	}
	
	@Test
	public void testDefaultRetryLimit() throws Exception {
		
	}
	
	@Test
	public void testCustomRetryExceptionType() throws Exception {
		
	}
	
	@Test
	public void testCustomRetryLimit() throws Exception {
		
	}
	
	@Test
	public void testSkipLog() throws Exception {
		
	}
	
	@Test
	public void testRetryLog() throws Exception {
		
	}
	
	@Test
	public void testRetryExhaustLog() throws Exception {
		
	}
	
	@Test
	public void testRestart() throws Exception {
		
	}
	
	@Test
	public void testProcessorFilter() throws Exception {
		
	}
}
