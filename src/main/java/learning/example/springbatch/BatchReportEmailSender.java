package learning.example.springbatch;

import java.util.List;

import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Component;

//@Component("batchReportEmailSender")
public class BatchReportEmailSender {

	@Autowired
    public JavaMailSender emailSender;
	
	@Autowired
	private ErrorRepository errorRepository;
	
	public void sendEmail(JobExecution jobExecution) throws MessagingException {
		MimeMessage message = emailSender.createMimeMessage();
		MimeMessageHelper helper = new MimeMessageHelper(message);
        helper.setTo(""); 
        helper.setSubject("Spring Batch");
        message.setContent(composeEmailBody(jobExecution), "text/html");
        emailSender.send(message);
	}
 
	private String composeEmailBody(JobExecution jobExecution) {
		StringBuilder text = new StringBuilder();
	    text.append("<html>\n<body>\n");
	    buildJobTable(text, jobExecution);
	    buildStepTable(text, jobExecution);	    
	    List<BatchErrorLog> errors = errorRepository.findByJobExecutionId(jobExecution.getId());
	    buildChunkTable(text, errors);
	    text.append("</body>\n</html>");
	    return text.toString();
	}
	
	private void buildJobTable(StringBuilder text, JobExecution jobExecution) {
		text.append("<h3>JOB</h3>\n");
		text.append("<table style='border:2px solid black'>\n");
	    text.append("<tr>\n" + 
		      		"<th style='border:1px solid black'>JOB_INSTANCE_ID</th>\n" + 
		      		"<th style='border:1px solid black'>JOB_NAME</th>\n" + 
		      		"<th style='border:1px solid black'>JOB_EXECUTION_ID</th>\n" + 
		      		"<th style='border:1px solid black'>STATUS</th>\n" + 
		      		"<th style='border:1px solid black'>EXIT_STATUS</th>\n" + 
		      		"<th style='border:1px solid black'>START_TIME</th>\n" + 
		      		"<th style='border:1px solid black'>END_TIME</th>\n" +
		      		"</tr>\n");
	    text.append("<tr>\n" + 
	      		"<td style='border:1px solid black'>" + jobExecution.getJobInstance().getId() + "</td>\n" + 
	      		"<td style='border:1px solid black'>" + jobExecution.getJobInstance().getJobName() + "</td>\n" + 
	      		"<td style='border:1px solid black'>" + jobExecution.getId() + "</td>\n" + 
	      		"<td style='border:1px solid black'>" + jobExecution.getStatus() + "</td>\n" + 
	      		"<td style='border:1px solid black'>" + jobExecution.getExitStatus() + "</td>\n" + 
	      		"<td style='border:1px solid black'>" + jobExecution.getStartTime() + "</td>\n" + 
	      		"<td style='border:1px solid black'>" + jobExecution.getEndTime() + "</td>\n" +
	      		"</tr>\n");
	    text.append("</table>\n");
	    text.append("<br/><br/>");
	}
	
	private void buildStepTable(StringBuilder text, JobExecution jobExecution) {
		text.append("<h3>STEPS</h3>\n");
		text.append("<table style='border:2px solid black'>\n");
	    text.append("<tr>\n" + 
		      		"<th style='border:1px solid black'>STEP_EXECUTION_ID</th>\n" + 
		      		"<th style='border:1px solid black'>STEP_NAME</th>\n" + 
		      		"<th style='border:1px solid black'>JOB_EXECUTION_ID</th>\n" + 
		      		"<th style='border:1px solid black'>STATUS</th>\n" + 
		      		"<th style='border:1px solid black'>EXIT_STATUS</th>\n" + 
		      		"<th style='border:1px solid black'>START_TIME</th>\n" + 
		      		"<th style='border:1px solid black'>END_TIME</th>\n" +
		      		"</tr>\n");
	    for(StepExecution stepExecution : jobExecution.getStepExecutions()) {
	    	  text.append("<tr>\n");
	    	  text.append("<td style='border:1px solid black'>" + stepExecution.getId() + "</td>\n");
	    	  text.append("<td style='border:1px solid black'>" + stepExecution.getStepName() + "</td>\n");
	    	  text.append("<td style='border:1px solid black'>" + stepExecution.getJobExecutionId() + "</td>\n");
	    	  text.append("<td style='border:1px solid black'>" + stepExecution.getStatus() + "</td>\n");
	    	  text.append("<td style='border:1px solid black'>" + stepExecution.getExitStatus() + "</td>\n");
	    	  text.append("<td style='border:1px solid black'>" + stepExecution.getStartTime() + "</td>\n");
	    	  text.append("<td style='border:1px solid black'>" + stepExecution.getEndTime() + "</td>\n");
	    	  text.append("</tr>\n");
	      }
	    text.append("</table>\n");
	    text.append("<br/><br/>");
	}
	
	private void buildChunkTable(StringBuilder text, List<BatchErrorLog> errors) {
		text.append("<h3>RETRIES/ SKIPS/ FAILURES</h3>\n");
		text.append("<table style='border:2px solid black'>\n");
	    text.append("<tr>\n" + 
		      		"<th style='border:1px solid black'>LOG_ID</th>\n" + 
		      		"<th style='border:1px solid black'>TIMESTAMP</th>\n" + 
		      		"<th style='border:1px solid black'>JOB_NAME</th>\n" + 
		      		"<th style='border:1px solid black'>JOB_EXECUTION_ID</th>\n" + 
		      		"<th style='border:1px solid black'>STEP_NAME</th>\n" + 
		      		"<th style='border:1px solid black'>STEP_EXECUTION_ID</th>\n" + 
		      		"<th style='border:1px solid black'>ERROR</th>\n" +
		      		"<th style='border:1px solid black'>ACTION_TAKEN</th>\n" + 
		      		"</tr>\n");
	      for(BatchErrorLog error : errors) {
	    	  text.append("<tr>\n");
	    	  text.append("<td style='border:1px solid black'>" + error.getLogId() + "</td>\n");
	    	  text.append("<td style='border:1px solid black'>" + error.getTimestamp() + "</td>\n");
	    	  text.append("<td style='border:1px solid black'>" + error.getJobName() + "</td>\n");
	    	  text.append("<td style='border:1px solid black'>" + error.getJobExecutionId() + "</td>\n");
	    	  text.append("<td style='border:1px solid black'>" + error.getStepName() + "</td>\n");
	    	  text.append("<td style='border:1px solid black'>" + error.getStepExecutionId() + "</td>\n");
	    	  text.append("<td style='border:1px solid black'>" + error.getError() + "</td>\n");
	    	  text.append("<td style='border:1px solid black'>" + error.getActionTaken() + "</td>\n");
	    	  text.append("</tr>\n");
	      }
		    text.append("</table>\n");
	}
}
