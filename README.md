# ProActiveQueueClient

The **ProActiveQueue** Project is a highly opinionated library focused on quick & reliable ***ActiveMQ*** messaging in Java. In addition, this project is intended
(but not required) to be connected with ***AWS ECS*** for queue-scaled microservices.

## Project Focus:

1. Fast, Opinionated, Reliable messaging with ActiveMQ/AmazonMQ
2. Fast, Opinionated, Responsive Microservice scaling based on an ActiveMQ Queue
  2.a Stack: AWS ECS Fargate, AmazonMQ/ActiveMQ

#### 1. Messaging
On the messaging side, ActiveMQ java integrations require a lot of boilerplate code gathered from various documentation 
sources in order to get useful production work done. The ***ProActiveQueue*** project aims to minimize the time to get off the 
ground with ActiveMQ with strong opinions on:
1. ***Transactions:*** If messages aren't processed, they are returned for processing in the future
2. ***Batching:*** Send and recieve messages in batches within the same connection/transaction
3. ***Minimal Boilerplate:*** Configuring, Receiving, and Sending should require minimal code


#### 2. Microservice Queue Orchestration
On the AWS microservice side, this project aims to effortlessly link ***AWS AmazonMQ/ActiveMQ*** with ***AWS ECS Fargate***. The integration goals are aimed 
at the following architecture/flow:

1. A group of messages are sent from 1 (or more) applications to an ActiveMQ Queue.
2. The messages are detected and an AWS ECS cluster spins up the required containers to process all messages.
3. The messages are transactionally processed across AWS ECS (Fargate) containers.
4. The AWS ECS cluster scales down as all messages are processed

