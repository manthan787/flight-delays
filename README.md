# A4 - On-Time Performance Data
## Authors: Manthan Thakar, Vineet Trivedi and Sharad Boni

### Goal:
To plot the mean delay of the five most active airlines and for the five most active airports in the country.

### System Design:
We have used 5 jobs to achieve this goal.

* 1 Job to clean the data.
This job does not have a reducer. The mapper of this job writes its output (CleanDataWritable) to hdfs.
It forms the base for all other jobs. The remaining jobs need not iterate over the data again and get all their information from the CleanDataWritables.
* 2 MR Jobs that calculate the mean flight delay and mean airport delay.
* 2 MR Jobs that calculate the mean delay for the 5 most active airlines and 5 most active airports.

### How to run the project end to end:

* Change the Hadoop Home path in the Makefile to the Hadoop Home path on your system
* Make sure you put all the input files inside the `input` folder and run `make setup`
* Open terminal in the root directory of the project and execute command `make run`
