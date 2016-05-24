# preliminary steps required

# install maven: http://stackoverflow.com/questions/12076326/how-to-install-maven2-on-redhat-linux
wget http://mirror.olnevhost.net/pub/apache/maven/binaries/apache-maven-3.2.1-bin.tar.gz
tar xvf apache-maven-3.2.1-bin.tar.gz
export M2_HOME=/home/hadoop/apache-maven-3.2.1
export M2=$M2_HOME/bin
export PATH=$M2:$PATH

# clone brickhouse and compile jar: 
sudo yum install git
git clone https://github.com/klout/brickhouse
cd brickhouse/
mvn package

# copy the file to hdfs
hadoop fs -cp s3://aws-logs-783442766762-us-west-2/elasticmapreduce/j-1MBITR7WDWKNH/plagirism_corpus_tr_8_shingle /small_data

hadoop fs -cp s3://aws-logs-783442766762-us-west-2/elasticmapreduce/j-3V92LJ80TRM06/intrinsic-detection-hive-8-shingle /large_data
