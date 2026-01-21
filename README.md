This is a distributed key value store.

Supports GET, PUT, DELETE operations on string-typed keys and string-typed values.

Simple hashing to route key to shard.

Each shard is replicated via master slave.

Arbitrary number of slaves can be added to serve a shard master.

Reads from master are consistent.

Reads from slaves are eventually consistent.
