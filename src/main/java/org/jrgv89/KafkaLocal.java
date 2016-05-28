package org.jrgv89;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.eclipse.jetty.websocket.api.StatusCode;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.glassfish.tyrus.client.ClientManager;
import java.net.URI;
import javax.websocket.ClientEndpoint;
import javax.websocket.CloseReason;
import javax.websocket.ContainerProvider;
import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.WebSocketContainer;
import javax.websocket.*;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
//Improve

//TODO. create topic 
public class KafkaLocal {
	private String KAFKA_DIR = "logs/kafka/" + new Date().getTime();
	private static final int BATCH_SIZE = 10;
	private static final int MAX_MESSAGE_SIZE = 500;
	private static final int GOOD_MESSAGE_SIZE = 100;
	private static final int BAD_MESSAGE_SIZE = 1000;
	private static final int KAFKA_BROKER_ID = 0;
	private static final int KAFKA_BROKER_PORT = 9092;
	private static final String KAFKA_TOPIC = "test";
	private KafkaServerStartable server;

	public static interface MessageHandler {

		public void handleMessage(String message);
	}
	KafkaLocal() {
		Properties props = new Properties();
		props.setProperty("hostname", "localhost");
		props.setProperty("port", "9092");
		props.setProperty("broker.id", "0");
		props.setProperty("group.id", "0");
		props.setProperty("log.dir", "logs/kafka/" + new Date().getTime());
		props.setProperty("zookeeper.connect", "localhost:2181");
		props.setProperty("zookeeper.connection.timeout.ms", "6000");
		props.setProperty("log.flush.interval", "1");
		props.setProperty("log.default.flush.scheduler.interval.ms", "1");
		props.setProperty("num.partitions", "1");
		KafkaConfig kafkaConfig = new KafkaConfig(props);
		server = new KafkaServerStartable(kafkaConfig);
		server.startup();
	}

	private void stopServer() {
		if (server != null) {
			server.shutdown();
			server.awaitShutdown();
			server = null;
		}
	}

	public void shutdown() {
		System.out.println("After tests, kafka dir still exists? " + new File(KAFKA_DIR).exists());
		stopServer();
	}


	public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
		//		import java.io.IOException;


//		final CountDownLatch messageLatch;
//		try {
//			messageLatch = new CountDownLatch(10);
//
//			final ClientEndpointConfig cec = ClientEndpointConfig.Builder.create().build();
//
//			ClientManager client = ClientManager.createClient();
//			client.connectToServer(new Endpoint() {
//
//				@Override
//				public void onOpen(Session session, EndpointConfig config) {
//					try {
//						session.addMessageHandler(new MessageHandler.Whole<String>() {
//
//							public void onMessage(String message) {
//								System.out.println("Received message: " + message);
//								messageLatch.countDown();
//							}
//						});
//						session.getBasicRemote().sendText("");
//						System.out.println("Send Blank");
//					} catch (IOException e) {
//						e.printStackTrace();
//					}
//				}
//			}, cec, new URI("ws://stream.meetup.com/2/rsvps"));
//			messageLatch.await(1, TimeUnit.SECONDS);
//			System.out.println("Exit");
//		} catch (Exception e) {
//			e.printStackTrace();
//		}

//		try {
//			// open websocket
//			final WebsocketClientEndpoint clientEndPoint = new WebsocketClientEndpoint(new URI("ws://stream.meetup.com/2/rsvps"));
//
//			// add listener
//			clientEndPoint.addMessageHandler(new MessageHandler() {
//				public void handleMessage(String message) {
//					System.out.println(message);
//				}
//			});
//
//			// send message to websocket
//			clientEndPoint.sendMessage("{'event':'addChannel','channel':'ok_btccny_ticker'}");
//
//			// wait 5 seconds for messages from websocket
//			Thread.sleep(5000);
//
//		} catch (InterruptedException ex) {
//			System.err.println("InterruptedException exception: " + ex.getMessage());
//		} catch (URISyntaxException ex) {
//			System.err.println("URISyntaxException exception: " + ex.getMessage());
//		}
		String destUri = "ws://echo.websocket.org";
		if (args.length > 0)
		{
			destUri = args[0];
		}

		WebSocketClient client = new WebSocketClient();
		SimpleEchoSocket socket = new SimpleEchoSocket();
		try
		{
			client.start();

			URI echoUri = new URI(destUri);
			ClientUpgradeRequest request = new ClientUpgradeRequest();
			client.connect(socket,echoUri,request);
			System.out.printf("Connecting to : %s%n",echoUri);

			// wait for closed socket connection.
			socket.awaitClose(5,TimeUnit.SECONDS);
		}
		catch (Throwable t)
		{
			t.printStackTrace();
		}
		finally
		{
			try
			{
				client.stop();
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}

	}

	@WebSocket(maxTextMessageSize = 64 * 1024)
	public static class SimpleEchoSocket
	{
		private final CountDownLatch closeLatch;
		@SuppressWarnings("unused")
		private Session session;

		public SimpleEchoSocket()
		{
			this.closeLatch = new CountDownLatch(1);
		}

		public boolean awaitClose(int duration, TimeUnit unit) throws InterruptedException
		{
			return this.closeLatch.await(duration,unit);
		}

		@OnWebSocketClose
		public void onClose(int statusCode, String reason)
		{
			System.out.printf("Connection closed: %d - %s%n",statusCode,reason);
			this.session = null;
			this.closeLatch.countDown(); // trigger latch
		}

		@OnWebSocketConnect
		public void onConnect(Session session)
		{
			System.out.printf("Got connect: %s%n",session);
			this.session = session;
//			try
//			{
//				Future<Void> fut;
//				fut = session.getRemote().sendStringByFuture("Hello");
//				fut.get(2,TimeUnit.SECONDS); // wait for send to complete.
//
//				fut = session.getRemote().sendStringByFuture("Thanks for the conversation.");
//				fut.get(2,TimeUnit.SECONDS); // wait for send to complete.
//
//				session.close(StatusCode.NORMAL,"I'm done");
//			}
//			catch (Throwable t)
//			{
//				t.printStackTrace();
//			}
		}

		@OnWebSocketMessage
		public void onMessage(String msg)
		{
			System.out.printf("Got msg: %s%n",msg);
		}
	}



//
//	@ClientEndpoint
//	public static class WebsocketClientEndpoint {
//
//		Session userSession = null;
//		private MessageHandler messageHandler;
//
//		public WebsocketClientEndpoint(URI endpointURI) {
//			try {
//				WebSocketContainer container = ContainerProvider.getWebSocketContainer();
//				container.connectToServer(this, endpointURI);
//			} catch (Exception e) {
//				throw new RuntimeException(e);
//			}
//		}
//
//		/**
//		 * Callback hook for Connection open events.
//		 *
//		 * @param userSession the userSession which is opened.
//		 */
//		@OnOpen
//		public void onOpen(Session userSession) {
//			System.out.println("opening websocket");
//			this.userSession = userSession;
//		}
//
//		/**
//		 * Callback hook for Connection close events.
//		 *
//		 * @param userSession the userSession which is getting closed.
//		 * @param reason the reason for connection close
//		 */
//		@OnClose
//		public void onClose(Session userSession, CloseReason reason) {
//			System.out.println("closing websocket");
//			this.userSession = null;
//		}
//
//		/**
//		 * Callback hook for Message Events. This method will be invoked when a client send a message.
//		 *
//		 * @param message The text message
//		 */
//		@OnMessage
//		public void onMessage(String message) {
//			if (this.messageHandler != null) {
//				this.messageHandler.handleMessage(message);
//			}
//		}
//
//		/**
//		 * register message handler
//		 *
//		 * @param msgHandler
//		 */
//		public void addMessageHandler(MessageHandler msgHandler) {
//			this.messageHandler = msgHandler;
//		}
//
//		/**
//		 * Send a message.
//		 *
//		 * @param message
//		 */
//		public void sendMessage(String message) {
//			this.userSession.getAsyncRemote().sendText(message);
//		}
//
//		/**
//		 * Message handler.
//		 *
//		 * @author Jiji_Sasidharan
//		 */
//
//	}
}


