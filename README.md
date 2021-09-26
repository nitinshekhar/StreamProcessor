# Spring Kafka Streams using Spring Cloud Streams for Notification

## EndPoint
* https://localhost:8080/kafka/publish = To Publish the notification

## MicroServices
* NotificationService - Accepts notification and uses Spring Kafka topic to publish
* ParticipantService - Provides participant communication preference
* StreamProcessor - uses Spring Cloud Stream with Kafka Streams binder
* NotificationHandler - uses Spring Cloud Stream with Kafka Streams binder

## Architecture


## Process

### Install Kafka
Use Docker and Docker-compose to install Kafka 
1. Copy the content from here [Confluent Platform all-in-ine-Docker Compose fie](https://github.com/confluentinc/cp-all-in-one/blob/6.2.0-post/cp-all-in-one/docker-compose.yml)
2. Start the Confluent Platform in a detached mode using -d option
	docker-compose up -d
3. This should start confluent platform  with a separate container for each component. The output will resemble as below
	Creating network "cp-all-in-one_default" with the default driver
	Creating zookeeper ... done
	Creating broker    ... done
	Creating schema-registry ... done
	Creating rest-proxy      ... done
	Creating connect         ... done
	Creating ksql-datagen    ... done
	Creating ksqldb-server   ... done
	Creating control-center  ... done
	Creating ksqldb-cli      ... done

### Create Topic
1. Create a new Topic "Notification" using confluent console at
	http://localhost:9021

### Run Applications
1. Start all the applications. Check the Port in the application properties file to avoid any  overlap
2. Once all application is started, send a get request to notification service with required data


### Reference Documentation
For further reference, please consider the following sections:
