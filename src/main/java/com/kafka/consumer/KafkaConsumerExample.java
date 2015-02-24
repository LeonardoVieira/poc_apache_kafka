package com.kafka.consumer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import com.kafka.Constants;

public class KafkaConsumerExample {

	private List<String> replicaBrokers = new ArrayList<String>();

	public static void main(String args[]) {
		KafkaConsumerExample example = new KafkaConsumerExample();
		
		List<String> seeds = new ArrayList<String>();
		seeds.add(Constants.BROKER);

		try {
			example.run(Constants.MAX_READS, Constants.TOPIC_NAME, Constants.PARTITION, seeds, Constants.PORT);
		} catch (Exception e) {
			System.out.println("Error: " + e);
			e.printStackTrace();
		}
	}

	public KafkaConsumerExample() {
		replicaBrokers = new ArrayList<String>();
	}

	public void run(long maxReads, String topic, int partition,
			List<String> seedBrokers, int port) throws Exception {
		// find the meta data about the topic and partition we are interested in
		PartitionMetadata metadata = findLeader(seedBrokers, port, topic, partition);
		if (metadata == null) {
			System.out.println(Constants.ErrorMessages.METADATA_NOT_FIND);
			return;
		}
		if (metadata.leader() == null) {
			System.out.println(Constants.ErrorMessages.LEADER_NOT_FIND);
			return;
		}

		String leadBroker = metadata.leader().host();
		String clientName = "Client_" + topic + "_" + partition;

		SimpleConsumer consumer = new SimpleConsumer(leadBroker, port, 100000, 64 * 1024, clientName);
		long readOffset = getLastOffset(consumer, topic, partition,	kafka.api.OffsetRequest.EarliestTime(), clientName);

		int numErrors = 0;

		while (maxReads > 0) {
			if (consumer == null) {
				consumer = new SimpleConsumer(leadBroker, port, 100000, 64 * 1024, clientName);
			}

			FetchRequest req = new FetchRequestBuilder().clientId(clientName).addFetch(topic, partition, readOffset, 100000).build();
			FetchResponse fetchResponse = consumer.fetch(req);

			if (fetchResponse.hasError()) {
				numErrors++;
				short code = fetchResponse.errorCode(topic, partition);
				System.out.println(String.format("Error fetching data from the Broker: %s, Reason: %s", leadBroker, code));

				if (numErrors > 5) {
					break;
				}

				if(code == ErrorMapping.OffsetOutOfRangeCode()) {
					readOffset = getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime(), clientName);
					continue;
				}

				consumer.close();
				consumer = null;
				leadBroker = findNewLeader(leadBroker, topic, partition, port);

				continue;
			}

			numErrors = 0;

			long numRead = 0;

			for(MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition)) {
				long currentOffset = messageAndOffset.offset();
				if(currentOffset < readOffset) {
					System.out.println(String.format("Found an old offset: %s, Expecting: %s", currentOffset, readOffset));
					continue;
				}

				readOffset = messageAndOffset.nextOffset();
				ByteBuffer payload = messageAndOffset.message().payload();

				byte[] bytes = new byte[payload.limit()];
				payload.get(bytes);
				System.out.println(String.valueOf(messageAndOffset.offset()) + ": " + new String(bytes, "UTF-8"));
				numRead++;
				maxReads--;
			}

			if(numRead == 0) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ie) {
				}
			}
		}

		if(consumer != null) {
			consumer.close();
		}
	}

	public static long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime, String clientName) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
		OffsetRequest request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
		OffsetResponse response = consumer.getOffsetsBefore(request);

		if(response.hasError()) {
			System.out.println(String.format("Error fetching data Offset Data the Broker. Reason: %s", response.errorCode(topic, partition)));
			return 0;
		}

		long[] offsets = response.offsets(topic, partition);
		return offsets[0];
	}

	private String findNewLeader(String oldLeader, String topic, int partition, int port) throws Exception {
		for (int i = 0; i < 3; i++) {
			boolean goToSleep = false;
			PartitionMetadata metadata = findLeader(replicaBrokers, port, topic, partition);

			if(metadata == null) {
				goToSleep = true;
			} else if(metadata.leader() == null) {
				goToSleep = true;
			} else if(oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
				goToSleep = true;
			} else {
				return metadata.leader().host();
			}

			if(goToSleep) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ie) {
				}
			}
		}

		System.out.println(Constants.ErrorMessages.NEW_LEADER_NOT_FIND);
		throw new Exception(Constants.ErrorMessages.NEW_LEADER_NOT_FIND);
	}

	private PartitionMetadata findLeader(List<String> seedBrokers, int port, String topic, int partition) {
		PartitionMetadata returnMetaData = null;
		loop: for(String seed : seedBrokers) {
			SimpleConsumer consumer = null;

			try {
				consumer = new SimpleConsumer(seed, port, 100000, 64 * 1024, "leaderLookup");
				List<String> topics = Collections.singletonList(topic);
				TopicMetadataRequest req = new TopicMetadataRequest(topics);
				kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

				List<TopicMetadata> metaData = resp.topicsMetadata();

				for(TopicMetadata item : metaData) {
					for(PartitionMetadata part : item.partitionsMetadata()) {
						if(part.partitionId() == partition) {
							returnMetaData = part;
							break loop;
						}
					}
				}
			} catch(Exception e) {
				System.out.println("Error communicating with Broker [" + seed + "] to find Leader for [" + topic + ", " + partition + "] Reason: " + e);
			} finally {
				if (consumer != null) {
					consumer.close();
				}
			}
		}

		if(returnMetaData != null) {
			replicaBrokers.clear();

			for(kafka.cluster.Broker replica : returnMetaData.replicas()) {
				replicaBrokers.add(replica.host());
			}
		}

		return returnMetaData;
	}
}