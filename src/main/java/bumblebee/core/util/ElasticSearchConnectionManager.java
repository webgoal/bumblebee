package bumblebee.core.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class ElasticSearchConnectionManager {
	private static Config consumerConf;

	public static void loadConfig(String configPath) {
		Config config = ConfigFactory.load(configPath);
		loadConfig(config);
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

	@SuppressWarnings("resource")
	public static Client getConsumerClient() {
		
		try {
			System.out.println("Host: " + getConsumerCluster());
			Settings settings = Settings.builder().put("cluster.name", getConsumerCluster()).build();
			System.out.println("Host: " + getConsumerHost());
			System.out.println("Host: " + getConsumerPort());
			InetSocketTransportAddress transportAddress = new InetSocketTransportAddress(InetAddress.getByName(getConsumerHost()), getConsumerPort());
			TransportClient transportClient = new PreBuiltTransportClient(settings).addTransportAddress(transportAddress);
			return (Client) transportClient;
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return null;
	}
}
