package learning.example.springbatch.test;

import javax.sql.DataSource;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import learning.example.springbatch.BatchConfiguration;
import learning.example.springbatch.SpringBatchApplication;

@SpringBootTest
@SpringBatchTest
@RunWith(SpringRunner.class)
@ContextConfiguration(classes= {BatchConfiguration.class, SpringBatchApplication.class})
public class DbReadWriteJobTests {

	@Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;
	
	@Autowired
	private DataSource dataSource;
	
	@Test
	public void testJob() {
		Assert.assertTrue(1==1);
	}
}
