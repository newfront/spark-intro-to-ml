## Workshop Material: Introduction to Machine Learning with Apache Spark and Redis

### About the Speaker
Find me on Twitter: [@newfront](https://twitter.com/newfront)
Find me on Medium [@newfrontcreative](https://medium.com/@newfrontcreative)
About Twilio: [Twilio](https://twilio.com)

## Runtime Requirments
1. Docker (at least 2 CPU cores and 8gb RAM)
2. System Terminal (iTerm, Terminal, etc)
3. Working Web Browser (Chrome or Firefox)

### Technologies Used
1. [Apache Zeppelin](https://zeppelin.apache.org/docs/latest/interpreter/spark.html)
2. [Apache Spark](http://spark.apache.org/)
3. [Redis](https://redis.io/)

#### Spark 2.4.5
- http://mirror.metrocast.net/apache/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz
- (222 MB)

#### Redis Docker Hub (v5.0.7)
https://hub.docker.com/_/redis/

#### Spark Redis (v2.4.0)
https://github.com/RedisLabs/spark-redis

### Datasets
* Netflix Movies and Shows: https://www.kaggle.com/shivamb/netflix-shows
* House Prices: https://www.kaggle.com/c/house-prices-advanced-regression-techniques/data
* GoodReads Books: https://www.kaggle.com/jealousleopard/goodreadsbooks

### Docker
Install Docker Desktop (https://www.docker.com/products/docker-desktop)

Additional Docker Resources:
* https://docs.docker.com/get-started/
* https://hub.docker.com/

#### Docker Runtime Recommendations
1. 2 or more cpu cores.
2. 8gb/ram or higher.

#### Configuration
1. The Apache Spark configuration is stored in `/install/spark-defaults.conf`. You can update those settings to match the configuration of your Docker setup.

The spark defaults are below.
~~~bash
spark.cores.max 4
spark.executor.memory 8g
~~~

## Installation
1. Install Docker (See Docker above)
2. Once Docker is installed. Open up your terminal application and `cd /spark-intro-to-ml/docker`
3. `./run.sh install`
4. `./run.sh start`

## Checking Zeppelin and Updating Zeppelin
1. The **Main Application** should now be running at http://localhost:8080/
2. `docker exec -it redis5 redis-cli` should show `127.0.0.1:6379>` this should be a new install. Try inputting `info` to see the redid-server configuration.

### Monitoring Redis as you run the Workshop Material
The following command will let you view all commands hitting redis during the workshop
~~~
docker exec -it redis5 redis-cli monitor
~~~

## The Workshop
1. Open up your Browser on http://localhost:8080 and you should see the Zeppelin Home Screen
2. Click on the Notebook named **1-LoadAndQuery**. When this loads select the `spark` and `md` interpreters to attach to the notebook and then press the button at the top that says **Run All Paragraphs**
3. This first note in the notebook will take you through to **2-LoadTransformAndCluster** and finally to **3-ReloadAndPredictLogistically**
