# spring-batch

This implementation demonstrates how to configure a multi-step job where the failure of a preceding step does not cause the job to fail

How to test this implementation?

I have used an external Oracle database. PERSON table as input and EMPLOYEE table as output. The EMPLOYEE table has person_id column as unique. 
You will need to run the application twice.
1. In the first run, set spring.jpa.hibernate.ddl-auto=create in application.properties so that both the tables are created and populated. The test data can be found in import.sql.
2. Now manually delete every record from EMPLOYEE table except the one with person_id=5. Also set spring.jpa.hibernate.ddl-auto=none in application.properties so that from the next run onward, tables are not dropped and created.
3. Run the application a second time. You will see that the job completes successfully but step1 fails because of ConstraintViolationException. So only the first chunk of records (person_id={1,2,3}) are inserted into EMPLOYEE table. However, that does not cause the entire job to fail. The flow still goes to step2 which does a join of PERSON and EMPLOYEE tables and writes the data to a flat file.