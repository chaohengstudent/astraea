/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.app;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.app.web.BackboneImbalanceScenario;
import org.astraea.common.ByteUtils;
import org.astraea.common.Configuration;
import org.astraea.common.DataRate;
import org.astraea.common.DataSize;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicConfigs;
import org.astraea.common.json.JsonConverter;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class BackboneImbalanceApplyTest {

  public static final String realCluster =
      "192.168.103.177:25655,192.168.103.178:25655,192.168.103.179:25655,192.168.103.180:25655,192.168.103.181:25655,192.168.103.182:25655";
  public static final List<String> clients =
      List.of(
          // "192.168.103.184",
          "192.168.103.142",
          "192.168.103.143",
          "192.168.103.144",
          "192.168.103.145",
          "192.168.103.146",
          "192.168.103.147");

  @Test
  @Disabled
  void testBackbone() {
    try (Admin admin = Admin.of(realCluster)) {
      var scenario = new BackboneImbalanceScenario();
      var config =
          new Configuration(
              Map.ofEntries(
                  Map.entry(BackboneImbalanceScenario.CONFIG_PERF_ZIPFIAN_EXPONENT, "1.1"),
                  Map.entry(BackboneImbalanceScenario.CONFIG_PERF_KEY_TABLE_SEED, "0"),
                  // Map.entry(BackboneImbalanceScenario.CONFIG_TOPIC_COUNT, "1000"),
                  // Map.entry(BackboneImbalanceScenario.CONFIG_REPLICATION_FACTOR, "1,1,1,1,1,2,2,3"),
                  // Map.entry(BackboneImbalanceScenario.CONFIG_TOPIC_DATA_RATE_PARETO_SCALE,
                  //       Double.toString(DataRate.KB.of(900).byteRate())),
                  Map.entry(
                      BackboneImbalanceScenario.CONFIG_PERF_CLIENT_COUNT,
                      Integer.toString(clients.size()))));
      var result = scenario.apply(admin, config).toCompletableFuture().join();

      // admin.topicNames(false)
      //     .thenCompose(names -> admin.setTopicConfigs(names.stream()
      //         .collect(Collectors.toUnmodifiableMap(
      //             x -> x,
      //             x -> Map.of(
      //                 TopicConfigs.RETENTION_BYTES_CONFIG,
      //                 Long.toString(DataSize.GB.of(10).bytes()))))))
      //     .toCompletableFuture()
      //     .join();

      // print summary
      var converter = JsonConverter.defaultConverter();
      System.out.println(converter.toJson(result));
      // save result to json format
      var ansibleInventory = converter.toJson(toAnsibleInventory(result));
      var ansibleInventoryFile =
          "/home/chaoheng/Downloads/balancer/ansible/backbone-imbalance-scenario-inventory.json";
      try (var stream = Files.newBufferedWriter(Path.of(ansibleInventoryFile))) {
        stream.write(ansibleInventory);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  Map<?, ?> toAnsibleInventory(BackboneImbalanceScenario.Result result) {
    var hosts =
        IntStream.range(0, clients.size())
            .boxed()
            .collect(
                Collectors.toUnmodifiableMap(
                    clients::get,
                    index -> {
                      var clientHostname = clients.get(index);
                      var clientPerf = result.perfCommands().get(index);
                      return Map.ofEntries(
                          Map.entry("ansible_host", clientHostname),
                          Map.entry("ansible_user", "kafka"),
                          Map.entry("expected_produce_rate", clientPerf.get("produce_rate")),
                          Map.entry("expected_consume_rate", clientPerf.get("consume_rate")),
                          Map.entry("key_table_seed", clientPerf.get("key_table_seed")),
                          Map.entry("key_distribution", clientPerf.get("key_distribution")),
                          Map.entry(
                              "key_distribution_config", clientPerf.get("key_distribution_config")),
                          Map.entry("throttle", clientPerf.get("throttle")),
                          Map.entry("throttle_enable", !clientPerf.get("backbone").equals("true")),
                          Map.entry("throughput", clientPerf.get("throughput")),
                          Map.entry("throughput_enable", clientPerf.get("backbone").equals("true")),
                          Map.entry("no_producer", clientPerf.get("no_producer")),
                          Map.entry("no_consumer", clientPerf.get("no_consumer")),
                          Map.entry("topics", clientPerf.get("topics")));
                    }));

    return Map.of("backbone_imbalance_hosts", Map.of("hosts", hosts));
  }

  @Test
  void setRetentionSize() {
    try (Admin admin = Admin.of(realCluster)) {
      var names = admin.topicNames(false).toCompletableFuture().join();
      admin.setTopicConfigs(names.stream()
          .collect(Collectors.toUnmodifiableMap(
              name -> name,
              name -> Map.of(TopicConfigs.RETENTION_BYTES_CONFIG,
                  Long.toString(DataSize.GB.of(5).bytes())))))
          .toCompletableFuture()
          .join();
      admin.topics(names)
          .toCompletableFuture()
          .join()
          .stream()
          .map(x -> x.config().value(TopicConfigs.RETENTION_BYTES_CONFIG))
          .forEach(System.out::println);
    }
  }

  @Test
  void testSaveClusterInfo() {
    try (Admin admin = Admin.of(realCluster)) {
      ClusterInfo cluster = admin.topicNames(true)
          .thenCompose(admin::clusterInfo)
          .toCompletableFuture()
          .join();
      byte[] bytes = ByteUtils.toBytes(cluster);
      Path tempFile = Files.createTempFile("cluster-info", ".bin");
      try (OutputStream outputStream = Files.newOutputStream(tempFile)) {
        outputStream.write(bytes);
      }
      System.out.println(tempFile.toString());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void restoreClusterInfo() throws IOException {
    var file = "/home/garyparrot/Programming/ncku-thesis-template-latex/thesis/context/performance/experiments/exp2-cluster-info-before-greedy.bin";
    try (
        var admin = Admin.of(realCluster);
        var stream = Files.newInputStream(Path.of(file))) {
      var cluster = ByteUtils.readClusterInfo(stream.readAllBytes());

      System.out.println("Delete Topics");
      admin.topicNames(false)
          .thenApply(x -> x.stream()
              .filter(xx -> !xx.startsWith("__"))
              .collect(Collectors.toSet()))
          .thenApply(x -> {
            System.out.println("Delete: " + x);
            return x;
          })
          .thenCompose(admin::deleteTopics)
          .toCompletableFuture()
          .join();

      Utils.sleep(Duration.ofSeconds(10));

      System.out.println("Recreate Topics");
      cluster.replicas().stream()
          .collect(Collectors.groupingBy(Replica::topic, Collectors.mapping(Replica::topicPartition, Collectors.counting())))
          .entrySet()
          .stream()
          .map(x -> admin.creator()
              .topic(x.getKey())
              .numberOfPartitions(x.getValue().intValue())
              .numberOfReplicas((short)1)
              .run()
              .toCompletableFuture())
          .toList()
          .forEach(CompletableFuture::join);

      System.out.println("Relocate Replicas");
      admin.moveToBrokers(cluster.topicPartitions()
          .stream()
          .collect(Collectors.toUnmodifiableMap(
              tp -> tp,
              tp -> cluster.replicas(tp).stream()
                  .sorted(Comparator.comparing(x -> !x.isPreferredLeader()))
                  .map(Replica::brokerId)
                  .collect(Collectors.toList())
          )))
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(5));

      System.out.println("Relocate to Folders");
      admin.moveToFolders(cluster.replicas()
          .stream()
          .collect(Collectors.toUnmodifiableMap(Replica::topicPartitionReplica, Replica::path)))
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(5));

      System.out.println("Leader election");
      admin.preferredLeaderElection(cluster.topicPartitions());
      Utils.sleep(Duration.ofSeconds(5));

      admin.topicNames(true)
          .thenCompose(admin::clusterInfo)
          .toCompletableFuture()
          .join()
          .replicas()
          .stream()
          .filter(x -> x.isFuture() || x.isAdding() || x.isRemoving())
          .forEach(System.out::println);
    }
  }
}
