# spring-batch

This implementation demonstrates how Spring Batch and Database Transaction Management work in case of chunked database read and write

How to test this implementation?

I have used an external Oracle database. PERSON table as input and EMPLOYEE table as output. The EMPLOYEE table has person_id column as unique. 
You will need to run the application twice.
1. In the first run, set spring.jpa.hibernate.ddl-auto=create in application.properties so that both the tables are created and populated. The test data can be found in import.sql.
2. Now manually delete every record from EMPLOYEE table except the one with person_id=5. Also set spring.jpa.hibernate.ddl-auto=none in application.properties so that from the next run onward, tables are not dropped and created.
3. Run the application a second time. You will see that the job fails. A closer look at the log will tell you that the first chunk of 3 records (person_id = {1, 2, 3})were read and written just fine. It was the second chunk of 3 records (person_id = {4, 5, 6}) that failed. It is so because the second chunk has a record with person_id=5, something which EMPLOYEE table already had, and hence SQLIntegrityConstraintViolationException.
   To know which exact database record errored out, I have logged out Hibernate SQL statements which clearly shows that the last SQL executed was the one with person_id=5
   The exception will cause the database transaction to fail. Since faultTolerant() has been commented is enabled for the Step 'step1', retries till the retry-limit (default value seems to be 2) will happen. Since the constraint violation exception will re-occur until someone corrects the data, the Batch job will eventually end in failure. This results in the 7th record of PERSON table not being read at all. You can verify these stats from BATCH_STEP_EXECUTION table:
   
   READ_COUNT = 6
   WRITE_COUNT = 3
   COMMIT_COUNT = 1
   ROLLBACK_COUNT = 1