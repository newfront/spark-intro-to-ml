#!/bin/bash

PWD="${PWD}"
DATA_DIR="${PWD}/data/"
SPARK_VERSION='2.4.5'
SPARK_HOME="${PWD}/spark-${SPARK_VERSION}"
DOCKER_COMPOSE_FILE='docker-compose-all.yaml'
DOCKER_NETWORK_NAME='learnml'
BOOKS_DETAILED_COMPRESSED='goodreads/books_detailed_etl_large.json.zip'
BOOK_DATASET_COMPRESSED='goodreads/goodreadsbooks.zip'

export SPARK_VERSION=${SPARK_VERSION}
export SPARK_HOME=${SPARK_HOME}

echo "Current Context: SPARK_HOME=${SPARK_HOME} which is running SPARK_VERSION=${SPARK_VERSION}"

function installSpark() {
    if test ! -d "${PWD}/spark-${SPARK_VERSION}"
    then
    echo "using curl to download spark 2.4.5"
      curl -XGET "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz" > "${PWD}/install/spark-${SPARK_VERSION}.tgz"
      cd "${PWD}/install" && tar -xvzf spark-${SPARK_VERSION}.tgz && rm spark-${SPARK_VERSION}.tgz
      mv spark-${SPARK_VERSION}-bin-hadoop2.7 ../spark-${SPARK_VERSION}
      cd ..
    else
      echo "Spark is already installed under ${PWD}/spark-${SPARK_VERSION}"
    fi
    echo "${PWD}"
}

function prepData() {
    cd "${DATA_DIR}"
    unzip "${BOOK_DATASET_COMPRESSED}"
    unzip "${BOOKS_DETAILED_COMPRESSED}"
    cd ..
    echo "${PWD}"
}

function sparkConf() {
    cp "${PWD}/install/spark-defaults.conf" "${PWD}/spark-${SPARK_VERSION}/conf/"
}

function init() {
   installSpark
   prepData
   sparkConf
}

function createNetwork() {
  cmd="docker network ls | grep ${DOCKER_NETWORK_NAME}"
  eval $cmd
  retVal=$?
  if [ $retVal -ne 0 ]; then
    docker network create -d bridge ${DOCKER_NETWORK_NAME}
  else
    echo "docker network already exists ${DOCKER_NETWORK_NAME}"
  fi
}

function cleanDocker() {
    docker rm -f `docker ps -aq` # deletes the old containers
}

function start() {
    #init
    export SPARK_HOME=${PWD}/"spark-${SPARK_VERSION}"
    echo "Your Spark Home is set to ${SPARK_HOME}"
    cleanDocker
    createNetwork
    docker-compose -f ${DOCKER_COMPOSE_FILE} up -d --remove-orphans redis5
    docker-compose -f ${DOCKER_COMPOSE_FILE} up -d --remove-orphans zeppelin
}

function stop() {
    docker-compose -f ${DOCKER_COMPOSE_FILE} down
}

function info() {
    CONTAINER=$2
    echo "INSPECT THIS ${CONTAINER}"
    docker inspect ${CONTAINER}
}

case "$1" in
    install)
        init
    ;;
    prep)
        prepData
    ;;
    start)
        start
    ;;
    stop)
        stop
    ;;
    info)
        info
    ;;
    *)
        echo $"Usage: $0 {install | prep | start | stop | info {CONTAINER_NAME}"
    ;;
esac
