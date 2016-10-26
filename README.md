# palmetto-meta-scheduler
1. This is a utility provided to perform some actions on the scheduler.
2. Currently only two schedulers are supported. They are PBS and Condor.
3. The following actions can be performed:
	a. Submit a job to the scheduler
	b. Delete a job from the scheduler
	c. Query the status of the job from the scheduler
	d. Obtain the complete history of jobs submitted by the user
4. This utility receives the input the user in the form of JSON format
5. The JSON format contains the details related to the hostname, username etc.
