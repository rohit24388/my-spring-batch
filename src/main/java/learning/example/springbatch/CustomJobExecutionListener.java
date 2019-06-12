package learning.example.springbatch;

import javax.mail.MessagingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component("myJobListener")
public class CustomJobExecutionListener extends JobExecutionListenerSupport {
	
	private static final Logger logger = LoggerFactory.getLogger(CustomJobExecutionListener.class);
	
//	@Autowired
//	@Qualifier("batchReportEmailSender")
//	BatchReportEmailSender emailSender;
	
	@Override
	public void afterJob(JobExecution jobExecution) {
//		try {
//			emailSender.sendEmail(jobExecution);
//		} catch (MessagingException e) {
//			logger.error("Error sending email report for batch job " + jobExecution.getJobInstance().getJobName());
//			e.printStackTrace();
//		}
	}
	
}
