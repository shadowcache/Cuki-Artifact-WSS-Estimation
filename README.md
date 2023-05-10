# Adaptive Online Cache Capacity Optimization via Lightweight Working Set Size Estimation at Scale

This is the repository for the artifact evaluation of Cuki. Cuki is described in the ATC'23 paper "Adaptive Online Cache Capacity Optimization via Lightweight Working Set Size Estimation at Scale".

Cuki proposes an approximate data structure for efficiently estimating online WSS and IRR for variable-size item access with proven accuracy guarantee. Our solution is cache-friendly, thread-safe, and light-weighted in design. Based on that, we design an adaptive online cache capacity tuning mechanism.

The whole artifact is departed into three parts:
- wss estimation: https://github.com/shadowcache/Cuki-Artifact-WSS-Estimation
- query engine application: https://github.com/shadowcache/Cuki-Artifact-Presto
- cache system: https://github.com/shadowcache/Cuki-Artifact-Alluxio

## Experimental Environment
Cuki is implemented on Alluxio, which is compiled using Maven and run with Java. It also relies on Presto and Hive to function properly.

To save you the trouble of setting up all these components, we provide two ways to get a pre-prepared environment. You can SSH into our pre-prepared machine in the AWS Cloud or deploy the environment yourself.

### Remote Machine via SSH
We provide an AWS EC2 server and have all the dependencies well-prepared.

You can contact us to get access to the machine (ip address and password etc.) anytime during the artifact evaluation process. After that, you can log in via ssh:

> ssh -p {password} atc23@host

The home directory contains the following files:
```
├─ download                 # dependencies
    ├── apache-hive-3.1.3-bin 
    ├── apache-maven-3.5.4
    ├── aws
    ├── hadoop-3.3.1
    ├── jdk1.8.0_151
    ├── jmx_prometheus
    ├── mysql-connector-jar-8.0.30
    ├── prometheus-2.37.0.linux-amd64
├─ alluxio                  # the cache system with cuki
├─ presto_cuki              # the query system with alluxio
├─ presto-data              # presto data directory
├─ wss-estimation           # the wss estimation of cuki
```

### Deploy your own environment
Dependencies are:
- hive 3.1.3
- maven 3.5.4
- hadoop 3.3.1
- java 8
- prometheus
- mysql 8.0.3
- S3


First, you need to deploy hive with its metastore in hdfs and mysql. The TPC-DS data should be located in S3. We also prepare the TPC-DS data in our S3, if you want to access it, please contact us. Then compile the alluxio provided by us:
```cmd
cd alluxio
mvn clean install -Dmaven.javadoc.skip=true -DskipTests -Dlicense.skip=true -Dcheckstyle.skip=true -Dfindbugs.skip=true -Prelease
```

Then, you can build the presto by:
```cmd
cd presto_cuki
mvn -N io.takari:maven:wrapper
mvnw clean install -T2C -DskipTests -Dlicense.skip=true -Dcheckstyle.skip=true -Dfindbugs.skip=true -pl '!presto-docs'
``` 

If you decide to use TPC-DS data in our S3, rename the `preto_cuki/etc-example` to the `preto_cuki/etc`, then config the key provided by us to the `presto_cuki/etc/hive.properties`.
```
hive.s3.aws-access-key=xxx
hive.s3.aws-secret-key=xxx
```

Then, load the data by the command:
```cmd
bash ./benchmarks/restart.sh
export PRESTO="/presto_cuki/presto-cli/target/presto-cli-0.266-SNAPSHOT-executable.jar"
${PRESTO} -f ./benchmarks/create_from_tpcds_sf10.sql
hive -f ./benchmarks/create_hive_s3_table.sql
```


The wss-estimation of the paper can be compiled by:
```cmd
cd wss-estimation
mvn assembly:assembly \
  -T 4C \
  -Dmaven.javadoc.skip=true \
  -DskipTests \
  -Dlicense.skip=true \
  -Dcheckstyle.skip=true \
  -Dfindbugs.skip=true
```

The wss-estimation dataset is too large to upload, you can download the MSR dataset at http://iotta.snia.org/traces/block-io/388, and twitter dataset at https://github.com/twitter/cache-trace. We have prepare them in our EC2 machine path `~/wss-estimation/datasets`.  


##  Steps for Evaluating Cuki
We have automated most of the integration and launching operations of our artifact. You can refer to the script files in wss-estimation and presto_cuki.

### Evaluate the accuracy of wss-estimation
1. build the wss-estimation repo:
```
cd wss-estimation
mvn assembly:assembly \
  -T 4C \
  -Dmaven.javadoc.skip=true \
  -DskipTests \
  -Dlicense.skip=true \
  -Dcheckstyle.skip=true \
  -Dfindbugs.skip=true
```
2. Run the `.sh` files, note that msr_ccf_mem should run twice with different `OPPO_AGING` parameters (true|false), the cmd will output the result file path.:
```
cd wss-estimation
bash ./bin/accuracy/msr_ccf_mem.sh
bash ./bin/accuracy/msr_bmc_mem.sh
bash ./bin/accuracy/msr_mbf_mem.sh
bash ./bin/accuracy/msr_ss_mem.sh
bash ./bin/accuracy/msr_swamp_mem.sh
```
3. After all methods get evaluated, run the following command to get your figure! The output figure path will displayed in the cmd:
```
python3 ./plot/plot_msr_accuracy.py
```

### Evaluate the cache hit rate

1. Build alluxio
```cmd
cd alluxio
mvn clean install -Dmaven.javadoc.skip=true -DskipTests -Dlicense.skip=true -Dcheckstyle.skip=true -Dfindbugs.skip=true -Prelease
```
2. Build presto
```cmd
cd presto_cuki
mvn -N io.takari:maven:wrapper
mvnw clean install -T2C -DskipTests -Dlicense.skip=true -Dcheckstyle.skip=true -Dfindbugs.skip=true -pl '!presto-docs'
```
3. check whether the hdfs is running by `jps`, if it is running, there will be namenode and datanode, else run the hdfs:
```cmd
cd download/hadoop-3.3.1
bash ./sbin/start-dfs.sh
```
4. Run hive metastore
```cmd
hive --service metastore
```
5. Run prometheus
```cmd
cd download/prometheus-2.37.0.linux-amd64
./prometheus --config.file=cuki.yml
```
6. Run Presto evaluate the cache hit rate, open the presto website page at port 8080, you should see it is working:
```cmd
cd presto_cuki
bash ./benchmarks/tpcds_s3.sh 
```
7. Run the bash to auto collect exp data and get your figure
```cmd
cd presto_cuki
python3 ./benchmarks/get_metrics.py
python3 ./benchmarks/plot.py
```

### Evaluate the accuracy of MRC generation
1. switch the wss-estimation's branch to rarcm
```cmd
cd wss-estimation
git switch rarcm
```
2. re-compile the wss-estimation
```cmd
mvn assembly:assembly \
  -T 4C \
  -Dmaven.javadoc.skip=true \
  -DskipTests \
  -Dlicense.skip=true \
  -Dcheckstyle.skip=true \
  -Dfindbugs.skip=true
```
3. run the scripts:
```cmd
bash ./benchmark_scripts/bench_rarcm_mrc.sh
bash ./benchmark_scripts/bench_cuki_mrc.sh
```
4. wait for the exp, and run python files to get your figure:
```cmd
python3 ./plot/plot_mrc_accuracy.py
```

