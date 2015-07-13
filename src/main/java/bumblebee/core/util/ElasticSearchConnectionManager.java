package bumblebee.core.util;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class ElasticSearchConnectionManager {
	private static Config consumerConf;

	public static void loadConfig() {
		loadConfig(ConfigFactory.defaultApplication());
	}

	public static void loadConfig(Config config) {
		consumerConf = config.getConfig("destination");
	}

	public static String getConsumerHost() {
		return consumerConf.getString("host");
	}

	public static int getConsumerPort() {
		return consumerConf.getInt("port");
	}

	public static String getConsumerCluster() {
		return consumerConf.getString("cluster");
	}

	public static Client getConsumerClient() {
		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", getConsumerCluster()).build();
		@SuppressWarnings("resource")
		TransportClient transportClient = new TransportClient(settings);
		transportClient = transportClient.addTransportAddress(new InetSocketTransportAddress(getConsumerHost(), getConsumerPort()));
		return (Client) transportClient;
	}
}
