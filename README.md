# Visualizing Flight Delays

Using flight ![on-time performance data](http://web.archive.org/web/20170113153149/http://transtats.bts.gov/DL_SelectFields.asp?Table_ID=236&DB_Short_Name=On-Time) published by Bureau of Transportation Statistics, flight delays are collected using a **Hadoop MapReduce job.**

After gathering the data of interest, the visualizations were created in R. Following sections show the visualizations, to know more about the analysis, please read `report.pdf`.

You can run the whole pipeline (includes running MapReduce job and building the report file with graphics), by following instructions in the **Running the project** section below.

## Top 5 Airports and Airlines based on activity

![top](images/top.png)

## Mean Delay Per Year

![perYear](images/yearly_delays.png)

## Mean Delay across all years for top 5 airports

![airports](images/airport_heatmap.png)

## Mean Delay Per Month

![monthly](images/monthly_delays.png)

# Running the project

### Requirements

Install `Java 1.8`

Download `Hadoop 2.8.1`

**R Requirements**

Install following dependencies to avoid errors while generating the Rmarkdown Report.

From your R console execute following commands:

```
install.packages("ggplot2")
install.packages("RColorBrewer")
install.packages("gridExtra")
```

### How to run the project end to end:

* Change the Hadoop Home path in the Makefile to the Hadoop Home path on your system
* Make sure you put all the input files inside the `input` folder and run `make setup`
* Open terminal in the root directory of the project and execute command `make run`
