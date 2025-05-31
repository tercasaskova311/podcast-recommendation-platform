# GROUP MEETING
# 28-05-2025 – Phase 1
We discussed the architecture proposed by Tommaso and decided how to split the tasks within the group:

- Terka is working on the transcripts component.
- Matteo is working on the simulations component.
- Tommaso is working on the Kafka infrastructure.

## KAFKA PART
We aim to simulate a real and scalable scenario by configuring a Kafka cluster with 3 brokers, each running in a Docker container. This setup will allow us to test how the system reacts to network failures—for example, simulating what happens if one broker goes offline.

Since the project is starting from scratch, we will use the latest stable release of Kafka, which includes KRaft (Kafka Raft metadata mode). This feature allows us to manage the cluster without ZooKeeper, simplifying the architecture.

Our Kafka Architecture
We decided to start with a 3-broker cluster, where all brokers also act as KRaft controllers. Only one controller is active at any time, responsible for managing the Kafka cluster's metadata and operations. This improves fault tolerance and simplifies deployment.

Why Kafka?
We chose Kafka because it is a pub/sub-based messaging system that decouples producers and consumers. It is ideal for building horizontally scalable systems.

Compared to other technologies like MQTT, Kafka is more sophisticated. It provides:

- Message durability
- High throughput
- Built-in scalability and fault tolerance

Another advantage of Kafka is that it does not differentiate between batch and streaming data—both are handled in the same way. The key distinction lies in how producers and consumers process the data.



