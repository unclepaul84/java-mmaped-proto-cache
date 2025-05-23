/*
 * This source file was generated by the Gradle 'init' task
 */

package org.example;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.DynamicMessage;
import proto.*;
import com.google.protobuf.Descriptors;
import java.io.File;

import java.io.FileDescriptor;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.nio.ByteBuffer;
import org.lmdbjava.*;
import static org.lmdbjava.ByteBufferProxy.PROXY_OPTIMAL;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.DbiFlags.MDB_DUPSORT;
import static org.lmdbjava.DirectBufferProxy.PROXY_DB;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.GetOp.MDB_SET;
import static org.lmdbjava.SeekOp.MDB_FIRST;
import static org.lmdbjava.SeekOp.MDB_LAST;
import static org.lmdbjava.SeekOp.MDB_PREV;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.charset.StandardCharsets.UTF_8;
import org.lmdbjava.CursorIterable.KeyVal;
import org.rocksdb.RocksDB;
import org.rocksdb.Options;

public class App {
        
        enum TestType {
                MMAPPED_PROTO_CACHE,
                LMDB,
                ROCKSDB
        }
        public static void main(String[] args)
                        throws Exception, com.google.protobuf.InvalidProtocolBufferException, IllegalArgumentException {

                var itemCount = Integer.parseInt(System.getenv().getOrDefault("ITEM_COUNT", "500000"));
                var testType = TestType.valueOf(System.getenv().getOrDefault("TEST_TYPE", "MMAPPED_PROTO_CACHE").toUpperCase());

                switch (testType) {
                                case LMDB:
                                                TestLMDB(itemCount);
                                                break;
                                case ROCKSDB:
                                                TestRocksDB(itemCount);
                                                break;
                                case MMAPPED_PROTO_CACHE:
                                default:
                                                TestMMappedProtoCache(itemCount);
                                                break;
                }
        }

        private static void TestMMappedProtoCache(int itemCount) throws IOException {
                MMappedProtoHashMapCache<PriceScenariosOuterClass.PriceScenarios> priceScenariosMap = new MMappedProtoHashMapCache<>(
                                java.nio.file.Paths.get("price_scenarios"), bytes -> {
                                        try {
                                                return PriceScenariosOuterClass.PriceScenarios.parseFrom(bytes);
                                        } catch (InvalidProtocolBufferException e) {
                                                throw new RuntimeException("Failed to parse PriceScenarios", e);
                                        }
                                });

                for (int i = 0; i < itemCount; i++) {
                        var pc = PriceScenariosOuterClass.PriceScenarios.newBuilder().setUnderlyer(("AAPL" + i))
                                        .setInstrumentSymbol(("AAPLDEC2023" + i));

                        for (int j = 0; j < 10000; j++) {

                                pc.addPrice(j * 0.1);
                        }
                        var sc = pc.build();

                        priceScenariosMap.put(sc.getInstrumentSymbol().getBytes(), sc);
                }

                long startTime = System.currentTimeMillis();
                int iterations = itemCount;
                int missingKeysCount = 0;

                for (int i = 0; i < iterations; i++) {
                        var key = ("AAPLDEC2023" + i);
                        var scenario = priceScenariosMap.get(key.getBytes());

                        if (scenario == null) {

                                missingKeysCount++;

                        } else {

                                if (!scenario.getInstrumentSymbol().equals(key))
                                        throw new IllegalArgumentException("Key mismatch: expected " + key
                                                        + ", but got " + scenario.getInstrumentSymbol());

                        }

                }

                long endTime = System.currentTimeMillis();
                double finalAvgTime = (double) (endTime - startTime) / iterations;
                System.out.println("Average time for " + iterations + " iterations: " + finalAvgTime + " ms");
                System.out.println("Missing keys count: " + missingKeysCount);
        }

        private static void TestLMDB(int itemCount) throws InvalidProtocolBufferException {
                String DB_NAME = "my DB";
                File file = new File("lmdb");

                final Env<ByteBuffer> env = create()
                                // LMDB also needs to know how large our DB might be. Over-estimating is OK.
                                .setMapSize(1073741824L * 100L)
                                // LMDB also needs to know how many DBs (Dbi) we want to store in this Env.
                                .setMaxDbs(1)
                                // Now let's open the Env. The same path can be concurrently opened and
                                // used in different processes, but do not open the same path twice in
                                // the same process at the same time.
                                .open(file);

                // We need a Dbi for each DB. A Dbi roughly equates to a sorted map. The
                // MDB_CREATE flag causes the DB to be created if it doesn't already exist.
                final Dbi<ByteBuffer> db = env.openDbi(DB_NAME, MDB_CREATE);

                // We want to store some data, so we will need a direct ByteBuffer.
                // Note that LMDB keys cannot exceed maxKeySize bytes (511 bytes by default).
                // Values can be larger.

                for (int i = 0; i < itemCount; i++) {
                        var pc = PriceScenariosOuterClass.PriceScenarios.newBuilder().setUnderlyer(("AAPL" + i))
                                        .setInstrumentSymbol(("AAPLDEC2023" + i));

                        for (int j = 0; j < 10000; j++) {

                                pc.addPrice(j * 0.1);
                        }
                        var sc = pc.build();
                        var valSize = sc.getSerializedSize();

                        final ByteBuffer key = allocateDirect(env.getMaxKeySize());
                        final ByteBuffer val = allocateDirect(valSize);
                        key.put(sc.getInstrumentSymbol().getBytes(UTF_8)).flip();
                        val.put(sc.toByteArray()).flip();

                        // Now store it. Dbi.put() internally begins and commits a transaction (Txn).
                        db.put(key, val);
                }

                long startTime = System.currentTimeMillis();
                int iterations = itemCount;
                int missingKeysCount = 0;
                final ByteBuffer key = allocateDirect(env.getMaxKeySize());
                for (int i = 0; i < iterations; i++) {

                        try (Txn<ByteBuffer> txn = env.txnRead()) {
                                key.clear();
                                key.put(("AAPLDEC2023" + i).getBytes(UTF_8)).flip();

                                final ByteBuffer found = db.get(txn, key);

                                // The fetchedVal is read-only and points to LMDB memory
                                final ByteBuffer fetchedVal = txn.val();

                                var ps = PriceScenariosOuterClass.PriceScenarios.parseFrom(fetchedVal);

                        }
                }

                long endTime = System.currentTimeMillis();
                double finalAvgTime = (double) (endTime - startTime) / iterations;
                System.out.println("Average time for " + iterations + " iterations: " + finalAvgTime + " ms");
        }

        private static void TestRocksDB(int itemCount) throws Exception {
                // Load RocksDB native library.
                RocksDB.loadLibrary();

                // Create RocksDB options: create DB if missing.
                Options options = new Options().setCreateIfMissing(true);

                // Path where RocksDB will store its data.
                options.setAllowMmapReads(true);
                options.setAllowMmapWrites(true);
                String dbPath = "rocksdb_data";

                try (RocksDB db = RocksDB.open(options, dbPath)) {
                        // Write test: store PriceScenarios messages.
                        for (int i = 0; i < itemCount; i++) {
                                var pc = PriceScenariosOuterClass.PriceScenarios.newBuilder()
                                                .setUnderlyer("AAPL" + i)
                                                .setInstrumentSymbol("AAPLDEC2023" + i);

                                for (int j = 0; j < 10000; j++) {
                                        pc.addPrice(j * 0.1);
                                }
                                var sc = pc.build();
                                byte[] keyBytes = sc.getInstrumentSymbol().getBytes(UTF_8);
                                byte[] valueBytes = sc.toByteArray();
                                db.put(keyBytes, valueBytes);
                        }

                        long startTime = System.currentTimeMillis();
                        int missingKeysCount = 0;

                        // Read test: fetch and verify each entry.
                        for (int i = 0; i < itemCount; i++) {
                                String key = "AAPLDEC2023" + i;
                                byte[] valueBytes = db.get(key.getBytes(UTF_8));
                                if (valueBytes == null) {
                                        missingKeysCount++;
                                } else {
                                        var scenario = PriceScenariosOuterClass.PriceScenarios.parseFrom(valueBytes);
                                        if (!scenario.getInstrumentSymbol().equals(key)) {
                                                throw new IllegalArgumentException("Key mismatch: expected " + key
                                                                + ", but got " + scenario.getInstrumentSymbol());
                                        }
                                }
                        }
                        long endTime = System.currentTimeMillis();
                        double finalAvgTime = (double) (endTime - startTime) / itemCount;
                        System.out.println("RocksDB - Average time for " + itemCount + " iterations: " + finalAvgTime
                                        + " ms");
                        System.out.println("RocksDB - Missing keys count: " + missingKeysCount);
                } finally {
                        options.close();
                }
        }
}
