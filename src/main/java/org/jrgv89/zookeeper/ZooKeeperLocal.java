package org.jrgv89.zookeeper;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;

public class ZooKeeperLocal {

	private ZooKeeperServerMain zooKeeperServer;

	private ZooKeeperLocal(Properties zkProperties) throws FileNotFoundException, IOException {
		QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
		try {
			quorumConfiguration.parseProperties(zkProperties);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		zooKeeperServer = new ZooKeeperServerMain();
		final ServerConfig configuration = new ServerConfig();
		configuration.readFrom(quorumConfiguration);
		new Thread() {
			public void run() {
				try {
					zooKeeperServer.runFromConfig(configuration);
				} catch (IOException e) {
					System.out.println("ZooKeeper Failed");
					e.printStackTrace(System.err);
				}
			}
		}.start();
	}

	public static void main(String[] args) {
		Properties properties = new Properties();
		properties.setProperty("dataDir", "logs/zookeeper/" + new Date().getTime() + ".txt");
		properties.setProperty("clientPort", "2181");
		try {
			new ZooKeeperLocal(properties);
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("done");
	}



}