package learning.example.springbatch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringBatchApplication implements ApplicationRunner {
	
	@Autowired
	private JobRegistry jobRegistry;
	
	@Autowired
	private JobOperator jobOperator;
	
	@Autowired
    private JobLauncher jobLauncher;
	
	@Autowired
	@Qualifier("jobListener")
	private CustomJobExecutionListener jobListener;

	public static void main(String[] args) {
		SpringApplication.run(SpringBatchApplication.class, args);
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {
		Job job = jobRegistry.getJob("dbWriteJob");
		JobParameters params = new JobParameters();
		jobLauncher.run(job, params);
		if(!jobListener.getFailedJobs().isEmpty()) {
			long failedJobId = jobListener.getFailedJobs().get(0);
			jobOperator.restart(failedJobId);
		}
	}

}
