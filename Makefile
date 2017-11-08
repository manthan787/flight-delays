HADOOP_HOME = /usr/local/Cellar/hadoop/2.8.1
COMPILE_DIR = bin
MY_CLASSPATH = $(HADOOP_HOME)/share/hadoop/common/hadoop-common-2.8.1.jar:$(HADOOP_HOME)/share/hadoop/mapreduce/hadoop-mapreduce-client-common-2.8.1.jar:$(HADOOP_HOME)/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.8.1.jar
K = 2
ITERS = 1
INPUT_PATH = flight
CLEAN_DATA_PATH = output/

all: build run

build: bin compile jar

compile:
	javac -cp $(MY_CLASSPATH):$(COMPILE_DIR):. -d bin src/**/*.java

clean:
	rm -r bin/*
	$(HADOOP_HOME)/bin/hdfs dfs -rm -rf $(CLEAN_DATA_PATH)

run: build
	$(HADOOP_HOME)/bin/hadoop jar FlightDelay.jar jobs.CleanDataDriver $(INPUT_PATH) $(CLEAN_DATA_PATH)
	Rscript -e "rmarkdown::render('report.Rmd')"

jar:
	jar cvfm FlightDelay.jar MANIFEST.MF -C bin/ .

setup:
	$(HADOOP_HOME)/bin/hdfs dfs -mkdir -p flight/
	$(HADOOP_HOME)/bin/hdfs dfs -copyFromLocal input/* flight/

teardown:
	$(HADOOP_HOME)/bin/hdfs dfs -rm -r input/

bin:
	mkdir -p bin

gzip:
	gzip input/big-corpus/*; gzip input/books/*

gunzip:
	gunzip input/big-corpus/*; gunzip input/books/*
