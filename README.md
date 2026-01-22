# Key-Value Store

A distributed key-value store supporting basic operations on string-typed keys and values, with sharding and master-slave replication.

## Features

### Supported Operations
- **GET**: Retrieve the value associated with a key.  
- **PUT**: Store or update a value under a key.  
- **DELETE**: Remove the value associated with a key.  

### Sharding
- Keys are routed to shards using **simple hashing**.  
- Each shard manages a subset of the keyspace.  

### Replication
- Each shard uses **master-slave replication**.  
- A shard can have **any number of slaves** to improve read availability and fault tolerance.  
- Writes are handled by the **master** and replicated asynchronously to slaves.  

### Consistency
- **Master reads** are eventually consistent with slave replicas.  
- Slaves may lag behind the master, depending on replication progress.  

### Scalability
- New slaves can be added dynamically to handle increased read load.  
- The system is designed to scale horizontally across multiple shards.  