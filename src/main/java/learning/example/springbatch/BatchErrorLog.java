package learning.example.springbatch;

import java.sql.Timestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "BATCH_ERROR_LOG")
public class BatchErrorLog {

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	private long logId;
	private String actionTaken;
	@Column(length = 1000)
	private String error;
	private Long jobExecutionId;
	private String jobName;
	private Long stepExecutionId;
	private String stepName;
	private Timestamp timestamp;
	
	public BatchErrorLog() {
	}
	
	public BatchErrorLog(Timestamp timestamp, String error) {
		super();
		this.timestamp = timestamp;
		this.error = error;
	}

	public long getLogId() {
		return logId;
	}
	public void setLogId(long logId) {
		this.logId = logId;
	}
	public Timestamp getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(Timestamp timestamp) {
		this.timestamp = timestamp;
	}
	public String getError() {
		return error;
	}
	public void setError(String error) {
		this.error = error;
	}

	public Long getJobExecutionId() {
		return jobExecutionId;
	}

	public void setJobExecutionId(Long jobExecutionId) {
		this.jobExecutionId = jobExecutionId;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public Long getStepExecutionId() {
		return stepExecutionId;
	}

	public void setStepExecutionId(Long stepExecutionId) {
		this.stepExecutionId = stepExecutionId;
	}

	public String getStepName() {
		return stepName;
	}

	public void setStepName(String stepName) {
		this.stepName = stepName;
	}

	public String getActionTaken() {
		return actionTaken;
	}

	public void setActionTaken(String actionTaken) {
		this.actionTaken = actionTaken;
	}
	
}
