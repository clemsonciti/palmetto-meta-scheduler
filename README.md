Meta-scheduler 

1.	This is a utility to submit jobs locally. The user need not login to the actual cluster for submission of the jobs.
2.	This is a utility provided to perform some actions on the scheduler.
      •	Currently only two schedulers are supported. They are:
            -	PBS 
            -	Condor
      •	PBS is used by the Palmetto cluster and Condor is used by the Open Science Grid (OSG)
3.	The following actions can be performed on the meta-scheduler:
      •	Submit a job to the scheduler
             -	submit.py script is used to submit the jobs. Transfer of files can be onto the cluster is done using ‘scp’ command. The                   number of files dependent to submit a job can be transferred
      •	Delete a job from the scheduler 
            -	delete.py script is used to delete the jobs
      •	Query the status of the job from the scheduler
            -	query.py script is used to know statistics of the jobs
      •	Obtain the complete history of jobs submitted by the user
            -	history.py shows all the jobs submitted to the cluster depending upon cluster
4.	This utility receives the input the user in the form of JSON format
      •	The config.json is the file provided by the user. This includes some of the cluster information like:
            -	Name of the cluster
            -	Scheduler used for that cluster
            -	Hostname
            -	Username
            -	Remote directory 
            -	Transfer type (ssh, Putty)
      •	The user can add ‘n’ number of cluster information in the config.json. If the cluster is not explicitly provided then the                 default value is set to “palmetto”
      •	This file is configurable by the user at any point of time
5.	Whenever each job is submitted, a corresponding pickle object is created. This pickle object is used to for further references. When the user wants to delete a job or query a job, then that corresponding pickle object is read and the expected output is displayed
6.	The commands for submitting a job, deleting a job or querying a job for a particular cluster is provided internally as they are not likely to change
7.	This utility also provides translation of jobscripts
      •	From PBS to Condor
            -	When the user submits a job with the PBS script to the OSG cluster. 
      •	From Condor to PBS
            -	When the user submits a job with the Condor script to the Palmetto cluster
8.	We can transfer files that are required by the scripts as arguments during submission of the job. This will put the files into the directory specified by the user in the config.json file.
9.	As the jobs are submitted locally, to ease the work of the users there is local id mapped to the remote id. The users can access the local id to query the details of that particular job.  map_jobid.csv is used to store the details of the jobs such as remote id, local id and the cluster information.


