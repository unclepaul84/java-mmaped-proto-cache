# ProtoHashMapCache

ProtoHashMapCache is an on-disk caching solution for Protocol Buffer messages that uses memory-mapped files. It efficiently maps byte-array keys to Protocol Buffer messages, providing configurable indexing and optional on-heap caching for fast access.

## Overview

The cache is designed to support large datasets by leveraging memory-mapped file storage for both the index and the value data. It offers two constructors:

1. **Full Configuration Constructor:** Enables full control over the cache settings such as the number of index segments, maximum entries per segment, on-heap cache size, and the size of the deserialization buffer.
2. **Default Constructor:** Uses preset default values for configuration parameters.

This design makes it possible to tune performance based on the specific application requirements.

## Features

- **Memory-Mapped Storage:** Uses off-heap memory for storing large amounts of data efficiently.
- **Configurable Index Segments:** Partition keys across multiple index segments for improved performance.
- **On-Heap LRU Cache:** Optional cache to speed up frequently accessed values.
- **Automatic Growth:** Value buffers automatically expand when capacity is reached using a configurable growth factor.
- **Performance Statistics:** Tracks metrics such as hash collisions and probe counts to assist with optimization and debugging.
- **Thread-Safe Access:** Read/write locks ensure safe concurrent access across multiple threads.

## Getting Started

### Prerequisites

- **Java 11** or higher is required.
- **Protocol Buffers Java runtime** must be added as a dependency.
- JDK with support for NIO and memory-mapped files.

### Installation

1. **Clone the Repository:**
   ```shell
   git clone https://github.com/your_username/java-mmaped-proto-cache.git
   cd java-mmaped-proto-cache
   ```
2. **Build the Project:**

   If using Gradle:
   ```shell
   gradlew build
   ```

   Or with Maven:
   ```shell
   mvn clean install
   ```

## Usage

Below is an example of how to create and use the ProtoHashMapCache with a Protocol Buffer message (`PriceScenarios`):

```java
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import org.example.MMappedProtoHashMapCache;
import proto.PriceScenariosOuterClass;

// Create a cache instance using the default constructor
Path mapDir = Paths.get("cache_dir");
MMappedProtoHashMapCache<PriceScenariosOuterClass.PriceScenarios> cache =
    new MMappedProtoHashMapCache<>(mapDir, protoBytes -> {
        try {
            return PriceScenariosOuterClass.PriceScenarios.parseFrom(protoBytes);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse PriceScenarios", e);
        }
    });

// Alternatively, create an instance with full configuration.
MMappedProtoHashMapCache<PriceScenariosOuterClass.PriceScenarios> cacheFull =
    new MMappedProtoHashMapCache<>(mapDir,
        protoBytes -> {
            try {
                return PriceScenariosOuterClass.PriceScenarios.parseFrom(protoBytes);
            } catch (Exception e) {
                throw new RuntimeException("Deserialization error", e);
            }
        },
        2,          // indexSegmentCount: 2 segments for indexing
        500000,     // indexSegmentMaxEntries: Maximum of 500,000 entries per segment
                    // ⚠ Make sure this value is large enough to handle the expected number of entries.
        1000,       // onHeapValueCacheSize: Enable LRU cache for up to 1000 entries (0 to disable)
        200 * 1024  // deserializeBufferCacheSizeBytes: 200KB buffer size for deserialization
    );

// Storing a Protocol Buffer message
byte[] key = "AAPLDEC2023".getBytes(StandardCharsets.UTF_8);
PriceScenariosOuterClass.PriceScenarios message = PriceScenariosOuterClass.PriceScenarios.newBuilder()
        .setUnderlyer("AAPL")
        .setInstrumentSymbol("AAPLDEC2023")
        .build();
cache.put(key, message);

// Retrieving the message
PriceScenariosOuterClass.PriceScenarios retrieved = cache.get(key);
System.out.println(retrieved);
```

## Constructor Parameters

### Full Configuration Constructor

```java
MMappedProtoHashMapCache(Path mapDir, Function<byte[], P> protoFactory,
                         int indexSegmentCount, int indexSegmentMaxEntries, 
                         int onHeapValueCacheSize, int deserializeBufferCacheSizeBytes)
```

- **mapDir:**  
  The directory path where the index and value files will be stored. If the directory or files do not exist, they are created automatically.

- **protoFactory:**  
  A function that converts a byte array into a Protocol Buffer message. This is used for deserializing values from the on-disk cache.

- **indexSegmentCount:**  
  The number of index segments. More segments can reduce contention and optimize lookup time by partitioning the index data.

- **indexSegmentMaxEntries:**  
  The maximum number of entries that each index segment can hold. This setting defines the capacity of each individual segment.  
  ⚠ **Warning:** Ensure that this value is large enough to handle the number of entries you expect to store (2X the number of expected entries). If it is set too low, the cache.put() will start throwing exceptions.

- **onHeapValueCacheSize:**  
  The size of the optional on-heap cache for stored entries. A non-zero value enables caching of frequently accessed items for faster retrieval. Set to 0 to disable this feature. This feature is a trade off between deserialization cost and on-heap memory usage. 

- **deserializeBufferCacheSizeBytes:**  
  The size (in bytes) of the thread-local buffer used during the deserialization of Protocol Buffer messages. Adjust this value based on the expected size of your messages.

## Advanced Customization & Metrics

- **Buffer Growth:**  
  The value storage grows automatically when the available space is exhausted. This is managed by a growth factor that increases the size of the memory-mapped file.

- **Performance Metrics:**  
  The cache records statistics such as the total number of probes during key search and the number of hash collisions. Monitoring these metrics can help fine-tune the cache performance.

## Comparison to LMDB and RocksDB

ProtoHashMapCache offers a unique balance between simplicity and performance when compared to other embedded data stores:

- **LMDB:**  
  LMDB is a mature key-value store known for its high performance and efficiency when working with memory-mapped files. However, it has limitations on key size and requires careful handling of transactions. ProtoHashMapCache provides a more specialized, protocol buffer–focused solution with built-in support for Java objects and customizable caching mechanisms.

- **RocksDB:**  
  RocksDB excels at managing large volumes of data using a log-structured merge tree architecture. While it provides robust support for concurrent writes and reads with configurable options, it typically requires more complex configuration and management of native libraries. ProtoHashMapCache simplifies the deployment by leveraging Java’s memory-mapping and minimizing the need for complex setup.

By focusing on protocol buffer serialization and leveraging off-heap memory with automatic buffer growth, ProtoHashMapCache aims to deliver predictable performance while simplifying application integration.

## Advanced Customization & Metrics

- **Buffer Growth:**  
  The value storage grows automatically when the available space is exhausted. This is managed by a growth factor that increases the size of the memory-mapped file.

- **Performance Metrics:**  
  The cache records statistics such as the total number of probes during key search and the number of hash collisions. Monitoring these metrics can help fine-tune the cache performance.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.