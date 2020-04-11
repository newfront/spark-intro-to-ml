## Workshop Material: Introduction to Machine Learning with Apache Spark and Redis

### About the Speaker
Find me on Twitter: [@newfront](https://twitter.com/newfront)
Find me on Medium [@newfrontcreative](https://medium.com/@newfrontcreative)
About Twilio: [Twilio](https://twilio.com)

## Runtime Requirments
1. Docker (at least 2 CPU cores and 8gb RAM)
2. System Terminal (iTerm, Terminal, etc)
3. Working Web Browser (Chrome or Firefox)

### Docker
Install Docker Desktop (https://www.docker.com/products/docker-desktop)

Additional Docker Resources:
* https://docs.docker.com/get-started/
* https://hub.docker.com/

#### Docker Runtime Recommendations
1. 2 or more cpu cores.
2. 8gb/ram or higher.

## Installation
1. Install Docker (See Docker above)
2. Once Docker is installed. Open up your terminal application and `cd /spark-intro-to-ml/docker`
3. `./run.sh install`
4. `./run.sh start`

## Checking Zeppelin and Updating Zeppelin
1. The **Main Application** should now be running at http://localhost:8080/
2. `docker exec -it redis5 redis-cli` should show `127.0.0.1:6379>` this should be a new install. Try inputting `info` to see the redid-server configuration.

### Update the Zeppelin Spark Interpreter Runtime
1. Go to http://localhost:8080/#/interpreter on your Web Browser
2. Search for `spark` in the `Search Interpreters` input field.
3. Click the `edit` button to initiate editing mode.

#### Update the Properties (under the properties section)
Add the following key/values.
1. **spark.redis.host** redis5
2. **spark.redis.port** 6379

Updated the following key/values
1. **spark.cores.max** 2
2. **spark.executor.memory** 8g

## Reference Material
Below are a list of links and references to the underlying framework running within this multi-docker setup 

### Technologies Used
1. [Apache Zeppelin](https://zeppelin.apache.org/docs/latest/interpreter/spark.html)
2. [Apache Spark](http://spark.apache.org/)
3. [Redis](https://redis.io/)

#### Spark 2.4.5
This is already installed for you as part of the `./run.sh install` process and is provided for reference
- http://mirror.cc.columbia.edu/pub/software/apache/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz
- (222 MB)

#### Redis Docker Hub (v5.0.7)
https://hub.docker.com/_/redis/

#### Spark Redis (v2.4.1)
https://github.com/RedisLabs/spark-redis

### Datasets
* Netflix Movies and Shows: https://www.kaggle.com/shivamb/netflix-shows (Showcased in the Workshop)
* House Prices: https://www.kaggle.com/c/house-prices-advanced-regression-techniques/data (Fun dataset)
* GoodReads Books: https://www.kaggle.com/jealousleopard/goodreadsbooks (Dataset for making Book Recommendations)

### Update the Dependencies (under the dependencies section)
1. Add `com.redislabs:spark-redis:2.4.1`
2. Click `Save` and these settings will be applied to the Zeppelin Runtime.