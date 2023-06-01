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
package org.astraea.app.web;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.distribution.ParetoDistribution;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;
import org.apache.commons.math3.random.Well19937c;
import org.apache.commons.math3.util.Pair;
import org.astraea.common.Configuration;
import org.astraea.common.DataRate;
import org.astraea.common.DataSize;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;

/**
 * This class build up an imbalance scenario that one of the topic has significant more produce load
 * than the others.
 */
public class BackboneImbalanceScenario implements Scenario<BackboneImbalanceScenario.Result> {

  public static final String CONFIG_RANDOM_SEED = "seed";
  public static final String CONFIG_TOPIC_COUNT = "topicCount";
  public static final String CONFIG_TOPIC_DATA_RATE_PARETO_SCALE = "topicRateParetoScale";
  public static final String CONFIG_TOPIC_DATA_RATE_PARETO_SHAPE = "topicRateParetoShape";
  public static final String CONFIG_TOPIC_CONSUMER_FANOUT_SERIES = "consumerFanoutSeries";
  public static final String CONFIG_PARTITION_COUNT_MIN = "partitionCountMin";
  public static final String CONFIG_PARTITION_COUNT_MAX = "partitionCountMax";
  public static final String CONFIG_BACKBONE_DATA_RATE = "backboneDataRate";
  public static final String CONFIG_PERF_CLIENT_COUNT = "performanceClientCount";
  public static final String CONFIG_PERF_KEY_TABLE_SEED = "performanceKeyTableSeed";
  public static final String CONFIG_PERF_ZIPFIAN_EXPONENT = "performanceZipfianExponent";

  private static final String backboneTopicName = "backbone";

  @Override
  public CompletionStage<Result> apply(Admin admin, Configuration scenarioConfig) {
    final var config = new Config(scenarioConfig);
    final var rng = new Well19937c(config.seed());
    final var topicDataRateDistribution =
        new ParetoDistribution(rng, config.topicRateParetoScale(), config.topicRateParetoShape());
    final var backboneDataRateDistribution =
        new UniformRealDistribution(
            rng, config.backboneDataRate() * 0.8, config.backboneDataRate() * 0.81);
    final var topicPartitionCountDistribution =
        new UniformIntegerDistribution(rng, config.partitionMin(), config.partitionMax());
    final var topicConsumerFanoutDistribution =
        new EnumeratedDistribution<>(
            rng,
            config.consumerFanoutSeries().stream()
                .map(x -> Pair.create(x, 1.0))
                .collect(Collectors.toUnmodifiableList()));

    return CompletableFuture.supplyAsync(
        () -> {
          final var topicNames =
              IntStream.range(0, config.topicCount())
                  .mapToObj(index -> "topic_" + index)
                  .collect(Collectors.toUnmodifiableSet());

          // create topics
          var normalTopics =
              topicNames.stream()
                  .map(
                      name ->
                          admin
                              .creator()
                              .topic(name)
                              .numberOfPartitions(topicPartitionCountDistribution.sample())
                              .numberOfReplicas((short) 1)
                              .run());
          var backboneTopic =
              Stream.generate(
                      () ->
                          admin
                              .creator()
                              .topic(backboneTopicName)
                              .numberOfPartitions(24)
                              .numberOfReplicas((short) 1)
                              .run())
                  .limit(1);

          Stream.concat(normalTopics, backboneTopic)
              .map(CompletionStage::toCompletableFuture)
              .peek(
                  stage ->
                      stage.whenComplete(
                          (done, err) -> {
                            if (err != null) err.printStackTrace();
                          }))
              .forEach(CompletableFuture::join);
          Utils.sleep(Duration.ofSeconds(1));

          // gather info and generate necessary variables
          var allTopics =
              Stream.concat(topicNames.stream(), Stream.of(backboneTopicName))
                  .collect(Collectors.toUnmodifiableSet());
          var clusterInfo = admin.clusterInfo(allTopics).toCompletableFuture().join();
          var topicDataRate =
              allTopics.stream()
                  .collect(
                      Collectors.toUnmodifiableMap(
                          x -> x,
                          x ->
                              DataRate.Byte.of(
                                  (long)
                                      (x.equals(backboneTopicName)
                                          ? backboneDataRateDistribution.sample()
                                          : topicDataRateDistribution.sample()))));
          var topicPartitionDataRate =
              clusterInfo.topicNames().stream()
                  .filter(topic -> !topic.equals(backboneTopicName))
                  .flatMap(
                      topic -> {
                        var partitionWeight =
                            clusterInfo.replicas(topic).stream()
                                .map(Replica::topicPartition)
                                .distinct()
                                .collect(
                                    Collectors.toUnmodifiableMap(tp -> tp, tp -> rng.nextDouble()));
                        var totalDataRate = topicDataRate.get(topic).byteRate();
                        var totalWeight =
                            partitionWeight.values().stream().mapToDouble(x -> x).sum();

                        return partitionWeight.entrySet().stream()
                            .map(
                                e ->
                                    Map.entry(
                                        e.getKey(),
                                        DataRate.Byte.of(
                                            (long) (totalDataRate * e.getValue() / totalWeight))));
                      })
                  .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
          var backboneTopicBandwidth = topicDataRate.get(backboneTopicName);
          var nodeWeight =
              IntStream.range(1, clusterInfo.brokers().size())
                  .boxed()
                  .collect(
                      Collectors.toMap(
                          index -> clusterInfo.brokers().get(index).id(),
                          index -> rng.nextInt(100)));
          nodeWeight.put(
              clusterInfo.brokers().get(0).id(),
              nodeWeight.values().stream().mapToInt(x -> x).sum());

          clusterInfo.replicas(backboneTopicName).stream()
              .collect(Collectors.groupingBy(x -> x.brokerId()))
              .forEach(
                  (nodeId, replicas) -> {
                    var weight = nodeWeight.get(nodeId);
                    var weightSum = nodeWeight.values().stream().mapToInt(x -> x).sum();
                    var nodeDataRate = backboneTopicBandwidth.byteRate() * weight / weightSum;
                    var replicaDataRate = nodeDataRate / replicas.size();
                    replicas.forEach(
                        replica ->
                            topicPartitionDataRate.put(
                                replica.topicPartition(),
                                DataRate.Byte.of((long) replicaDataRate)));
                  });

          var consumerFanoutMap =
              allTopics.stream()
                  .collect(
                      Collectors.toUnmodifiableMap(
                          x -> x,
                          x ->
                              x.equals(backboneTopicName)
                                  ? 1
                                  : topicConsumerFanoutDistribution.sample()));

          return new Result(
              config,
              clusterInfo,
              allTopics,
              topicDataRate,
              topicPartitionDataRate,
              consumerFanoutMap);
        });
  }

  public static class Result {

    @JsonIgnore private final Config config;
    @JsonIgnore private final ClusterInfo clusterInfo;
    @JsonIgnore private final Set<String> topics;
    @JsonIgnore private final Map<String, DataRate> topicDataRates;
    @JsonIgnore private final Map<TopicPartition, DataRate> topicPartitionDataRates;
    @JsonIgnore private final Map<String, Integer> topicConsumerFanout;

    public Result(
        Config config,
        ClusterInfo clusterInfo,
        Set<String> topics,
        Map<String, DataRate> topicDataRates,
        Map<TopicPartition, DataRate> topicPartitionDataRates,
        Map<String, Integer> topicConsumerFanout) {
      this.config = config;
      this.clusterInfo = clusterInfo;
      this.topics = topics;
      this.topicDataRates = topicDataRates;
      this.topicPartitionDataRates = topicPartitionDataRates;
      this.topicConsumerFanout = topicConsumerFanout;
    }

    @JsonProperty
    public long totalTopics() {
      return topics.size();
    }

    @JsonProperty
    public long totalPartitions() {
      return clusterInfo.replicaStream().filter(r -> topics.contains(r.topic())).count();
    }

    @JsonProperty
    public String totalProduceRate() {
      var sum = topicDataRates.values().stream().mapToDouble(DataRate::byteRate).sum();
      return DataRate.Byte.of((long) sum).toString();
    }

    @JsonProperty
    public String totalConsumeRate() {
      var sum =
          topicDataRates.entrySet().stream()
              .mapToDouble(e -> e.getValue().byteRate() * topicConsumerFanout.get(e.getKey()))
              .sum();
      return DataRate.Byte.of((long) sum).toString();
    }

    @JsonProperty
    public double consumerFanoutAverage() {
      return config.consumerFanoutSeries().stream().mapToInt(x -> x).average().orElse(0);
    }

    @JsonProperty
    public Map<String, String> topicDataRate() {
      return topicDataRates.entrySet().stream()
          .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, x -> x.getValue().toString()));
    }

    @JsonProperty
    public Map<String, String> topicDataRateHistogram() {
      var byteRates =
          topicDataRates.values().stream()
              .map(DataRate::byteRate)
              .sorted(Double::compareTo)
              .collect(Collectors.toUnmodifiableList());
      var totalRates = byteRates.size();
      // For all the data rates, we use 1/4 portion of the data rates as one histogram bin. And the
      // rest of the 3/4 will be used for the rest of the other bins. This process continues
      // recursively until no more rate for a single bin.
      var histogramBins =
          Stream.iterate(
                  Map.entry(totalRates, totalRates / 4),
                  e -> e.getKey() > 0,
                  (e) ->
                      Map.entry(
                          (e.getKey() - e.getValue()),
                          Math.max(1, (e.getKey() - e.getValue()) / 4)))
              .map(
                  e -> {
                    var taken = totalRates - e.getKey();
                    var takes = e.getValue();
                    return byteRates.subList(taken, taken + takes);
                  })
              .collect(Collectors.toUnmodifiableList());
      var rendered =
          histogramBins.stream()
              .map(
                  binContent -> {
                    var first = DataRate.Byte.of(binContent.get(0).longValue());
                    var last = DataRate.Byte.of(binContent.get(binContent.size() - 1).longValue());
                    var key = String.format("[%s, %s]", first, last);
                    var value = Integer.toString(binContent.size());
                    return Map.entry(key, value);
                  })
              .collect(Collectors.toUnmodifiableList());
      var orderMap =
          IntStream.range(0, rendered.size())
              .boxed()
              .collect(
                  Collectors.toUnmodifiableMap(
                      x -> rendered.get(x).getKey(), x -> x, Integer::sum));
      var sortedMap = new TreeMap<String, String>(Comparator.comparingInt(orderMap::get));
      rendered.forEach(e -> sortedMap.put(e.getKey(), e.getValue()));
      return sortedMap;
    }

    @JsonProperty
    public Map<String, Integer> topicConsumerFanout() {
      return topicConsumerFanout;
    }

    @JsonProperty
    public List<Map<String, String>> perfCommands() {
      class PerfClient {
        long consumeRate = 0;
        long produceRate = 0;
        final Set<String> topics = new HashSet<>();
        String keyDistribution;
        final Map<String, String> keyDistributionConfig = new HashMap<>();
      }
      var clientCount = config.performanceClientCount();
      if (clientCount < 3)
        throw new IllegalArgumentException("At least three perf clients required");
      var clients =
          IntStream.range(0, clientCount)
              .mapToObj(i -> new PerfClient())
              .collect(Collectors.toUnmodifiableList());

      // allocate topics to all the performance clients evenly
      for (var topic : topics) {
        var dataRate = (long) topicDataRates.get(topic).byteRate();
        var fanout = (int) topicConsumerFanout.get(topic);
        for (int i = 0; i < fanout; i++) {
          if (topic.equals(BackboneImbalanceScenario.backboneTopicName)) {
            // separate the processing of produce/consume to two individual clients.
            // see https://github.com/skiptests/astraea/issues/1567
            var produceClient = clients.get(0);
            produceClient.produceRate += dataRate;
            produceClient.topics.add(topic);
            var consumeClient = clients.get(1);
            consumeClient.consumeRate += dataRate;
            consumeClient.topics.add(topic);
          } else {
            var nextClient =
                clients.stream()
                    .skip(2)
                    .filter(x -> !x.topics.contains(topic))
                    .min(Comparator.comparing(x -> x.consumeRate))
                    .orElseThrow();
            nextClient.consumeRate += dataRate;
            nextClient.produceRate += dataRate / fanout;
            nextClient.topics.add(topic);
          }
        }
      }
      for (var client : clients) {
        var zipfian = client.topics.equals(Set.of(BackboneImbalanceScenario.backboneTopicName));
        client.keyDistribution = zipfian ? "zipfian" : "uniform";
        if (zipfian) {
          client.keyDistributionConfig.put("exponent", Double.toString(config.zipfianExponent()));
        }
      }

      // render the argument
      return clients.stream()
          .map(
              client -> {
                var isBackbone = client.topics.equals(Set.of(backboneTopicName));
                var consumeRate = DataRate.Byte.of(client.consumeRate);
                var produceRate = DataRate.Byte.of(client.produceRate);
                var throttle =
                    client.topics.stream()
                        .flatMap(
                            topic ->
                                clusterInfo
                                    .replicaStream(topic)
                                    .map(Replica::topicPartition)
                                    .distinct())
                        .flatMap(
                            tp -> {
                              // backbone partition bandwidth is unknown before performance start.
                              if (!topicPartitionDataRates.containsKey(tp)) return Stream.of();
                              var bytes =
                                  topicPartitionDataRates
                                      .get(tp)
                                      .dataSize()
                                      .divide(topicConsumerFanout.get(tp.topic()))
                                      .bytes();
                              // TopicPartitionDataRateMapField support only integer measurement
                              // and no space allowed. So we can't just toString the DataRate
                              // object :(
                              return Stream.of(String.format("%s:%sByte/second", tp, bytes));
                            })
                        .collect(Collectors.joining(","));
                var throughput = String.format("%dByte/second", (long) produceRate.byteRate());
                var keyDistConfigString =
                    client.keyDistributionConfig.entrySet().stream()
                        .map(e -> String.format("%s=%s", e.getKey(), e.getValue()))
                        .collect(Collectors.joining());
                return Map.ofEntries(
                    Map.entry("backbone", Boolean.toString(isBackbone)),
                    Map.entry("topics", String.join(",", client.topics)),
                    Map.entry("throughput", throughput),
                    Map.entry("throttle", throttle),
                    Map.entry("key_distribution", client.keyDistribution),
                    Map.entry("key_distribution_config", keyDistConfigString),
                    Map.entry("key_table_seed", Integer.toString(config.keyTableSeed())),
                    Map.entry("no_consumer", Boolean.toString(consumeRate.byteRate() == 0)),
                    Map.entry("no_producer", Boolean.toString(produceRate.byteRate() == 0)),
                    Map.entry("consume_rate", consumeRate.toString()),
                    Map.entry("produce_rate", produceRate.toString()));
              })
          .collect(Collectors.toUnmodifiableList());
    }

    @JsonProperty
    public int seed() {
      return config.seed();
    }
  }

  public static class Config {

    private final Configuration scenarioConfig;
    private final int defaultRandomSeed = ThreadLocalRandom.current().nextInt();
    private final int defaultPerfKeyTableSeed = new Random(defaultRandomSeed).nextInt();

    public Config(Configuration scenarioConfig) {
      this.scenarioConfig = scenarioConfig;

      int maxFanout = consumerFanoutSeries().stream().mapToInt(x -> x).max().orElseThrow();
      if (maxFanout > performanceClientCount())
        throw new IllegalArgumentException(
            "The number of client is less than the max topic fanout: "
                + maxFanout
                + " <= "
                + performanceClientCount());
    }

    int seed() {
      return scenarioConfig
          .string(CONFIG_RANDOM_SEED)
          .map(Integer::parseInt)
          .orElse(defaultRandomSeed);
    }

    int topicCount() {
      return scenarioConfig.string(CONFIG_TOPIC_COUNT).map(Integer::parseInt).orElse(1000);
    }

    int partitionMin() {
      return scenarioConfig.string(CONFIG_PARTITION_COUNT_MIN).map(Integer::parseInt).orElse(5);
    }

    int partitionMax() {
      return scenarioConfig.string(CONFIG_PARTITION_COUNT_MAX).map(Integer::parseInt).orElse(15);
    }

    List<Integer> consumerFanoutSeries() {
      return scenarioConfig
          .string(CONFIG_TOPIC_CONSUMER_FANOUT_SERIES)
          .filter(s -> !s.isEmpty())
          .map(
              seriesString ->
                  Arrays.stream(seriesString.split(","))
                      .map(Integer::parseInt)
                      .collect(Collectors.toUnmodifiableList()))
          .orElse(List.of(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 3, 5));
    }

    double topicRateParetoScale() {
      return scenarioConfig
          .string(CONFIG_TOPIC_DATA_RATE_PARETO_SCALE)
          .map(Double::parseDouble)
          .orElse(DataRate.MB.of(1).byteRate());
    }

    double topicRateParetoShape() {
      return scenarioConfig
          .string(CONFIG_TOPIC_DATA_RATE_PARETO_SHAPE)
          .map(Double::parseDouble)
          .orElse(3.0);
    }

    long backboneDataRate() {
      return scenarioConfig
          .string(CONFIG_BACKBONE_DATA_RATE)
          .map(Long::parseLong)
          .orElse(DataSize.MB.of(950).bytes());
    }

    int performanceClientCount() {
      return scenarioConfig.string(CONFIG_PERF_CLIENT_COUNT).map(Integer::parseInt).orElse(7);
    }

    int keyTableSeed() {
      return scenarioConfig
          .string(CONFIG_PERF_KEY_TABLE_SEED)
          .map(Integer::parseInt)
          .orElse(defaultPerfKeyTableSeed);
    }

    double zipfianExponent() {
      return scenarioConfig
          .string(CONFIG_PERF_ZIPFIAN_EXPONENT)
          .map(Double::parseDouble)
          .orElse(1.0);
    }
  }
}
