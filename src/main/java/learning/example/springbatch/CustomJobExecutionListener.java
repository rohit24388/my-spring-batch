package learning.example.springbatch;

import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.stereotype.Component;

@Component("jobListener")
public class CustomJobExecutionListener extends JobExecutionListenerSupport {
	
	private List<Long> failedJobs = new ArrayList<Long>();

	@Override
	public void afterJob(JobExecution jobExecution) {
		if (jobExecution.getStatus() == BatchStatus.FAILED) {
			failedJobs.clear();
			failedJobs.add(jobExecution.getId());
		}
	}

	public List<Long> getFailedJobs() {
		return failedJobs;
	}
	
}
