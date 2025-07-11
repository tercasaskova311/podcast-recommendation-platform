# GROUP MEETING
# 28-05-2025 – Phase 1
We discussed the architecture proposed by Tommaso and decided how to split the tasks within the group:

- Terka is working on the transcripts component.
- Matteo is working on the simulations component.
- Tommaso is working on the Kafka infrastructure.

## KAFKA PART
We aim to simulate a real and scalable scenario by configuring a Kafka cluster with 3 brokers, each running in a Docker container. This setup will allow us to test how the system reacts to network failures—for example, simulating what happens if one broker goes offline.

Since the project is starting from scratch, we will use the latest stable release of Kafka, which includes KRaft (Kafka Raft metadata mode). This feature allows us to manage the cluster without ZooKeeper, simplifying the architecture.

### Our Kafka Architecture
We decided to start with a 3-broker cluster, where all brokers also act as KRaft controllers. Only one controller is active at any time, responsible for managing the Kafka cluster's metadata and operations. This improves fault tolerance and simplifies deployment.

### Why Kafka?
We chose Kafka because it is a pub/sub-based messaging system that decouples producers and consumers. It is ideal for building horizontally scalable systems.

Compared to other technologies like MQTT, Kafka is more sophisticated. It provides:

- Message durability
- High throughput
- Built-in scalability and fault tolerance

Another advantage of Kafka is that it does not differentiate between batch and streaming data—both are handled in the same way. The key distinction lies in how producers and consumers process the data.



### Topics
Next, we need to define the logic and pipeline structure for the project. We'll use four Kafka topics:

- user-events-stream
- podcast-metadata
- transcripts-foreign
- transcripts-en

We decided to separate transcripts by language because they will be processed by different consumer services.

### Replication and Fault Tolerance
To ensure fault tolerance, we will set the replication factor to 3 for each topic.

Kafka allows producers to:

- Not specify a partition (default is round-robin),
- Specify a key (ensuring that all messages with the same key go to the same partition, preserving order),
- Or specify a specific partition.

Depending on the use case, we will decide whether message order matters.

### Topic-by-Topic Strategy

**transcripts-foreign & transcripts-en:**

- Order not important
- Batch data, high volume
- Use round-robin for distribution
- Use more partitions to parallelize processing

**podcast-metadata**

- Order not important
- Low volume, low throughput
- Use round-robin, fewer partitions
- **Set retention to infinite so that the producer can check for already processed episodes and only send new ones**

**user-events-stream**

- High volume, streaming data
- Order is important per user (Use user ID as key to guarantee message order per user)
- Use more partitions to ensure scalability

*We verified through documentation that this approach won’t cause overload, assuming a normal distribution of user activity*

## CONSUMER LOGIC
Kafka is responsible for partition assignment when a consumer group subscribes to a topic. Kafka ensures that:

- Each partition is assigned to exactly one consumer within a consumer group
- If a consumer fails, Kafka will rebalance the partitions across the remaining consumers


## MONGODB PART
We decided to store the aggregated data, which needs to be accessed by dashboards for content creators and by filtering/recommendation systems, in MongoDB—a NoSQL database optimized for document storage.

The advantages of this solution include:

- A flexible schema, which is ideal for evolving data models
- Fast read performance, making it suitable for analytics and recommendation queries
- Horizontal scalability through built-in sharding capabilities (not implemented in our project)


## SPARK PART
The core idea is to create a Spark cluster with a Spark master acting as the cluster manager (responsible for resource management and job scheduling) and multiple workers executing the jobs.

For our project, we simulate a scalable system using 1 master and 3 workers.
By connecting to localhost:8080, we can access the Spark Master Web UI to monitor the state of the cluster.

### Why Spark?
Apache Spark allows us to decouple the computation engine (which performs the actual operations) from the client (the driver program that submits the jobs). This separation is useful because we chose to use Python for the driver, but in the future, we could switch to a different language without modifying the Spark cluster itself.

### Why not Hadoop?
We opted for Spark instead of Hadoop because:

- Spark is significantly faster, thanks to in-memory processing
- Spark has native support for streaming, making it easier to unify batch and streaming workloads

### Parallelization and Partitioning
Using Kafka partitions, we can parallelize data processing in Spark.
For example, the **user-events-stream** topic has 24 partitions, which allows Spark to execute up to 24 parallel tasks to read and process the data efficiently.

### Job Scheduling Strategy
To make our system as efficient as possible, we avoid keeping batch services running all the time. Since new episodes are ingested only once a day, batch jobs are scheduled to run daily and process only the new data.

### Execution Strategy
We created a general script called main.py that, based on a parameter, selects which job to run.

- Streaming job (user-events): Runs continuously in a dedicated Docker container, always ready to consume data from Kafka.

- Batch jobs (transcripts-foreign, transcripts-en, podcast-metadata): Run once per day in separate Docker containers and each batch job has its own driver program that starts and stops as needed.

- Summary job: Reads processed data directly from Delta Lake (not Kafka) to compute aggregates and summaries for dashboards and recommendations. It also runs in its own Docker container.

To support scalability, the Spark workers can be scaled horizontally by adding more containers. Each driver program runs independently per job.

For the batch and summary jobs, we use Docker container templates, built once for efficiency. These containers are instantiated only when needed, minimizing resource usage.