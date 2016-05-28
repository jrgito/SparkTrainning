package org.jrgv89;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Calendar;
import java.util.Random;

public class SocketServer {
	public static void main(String[] args) throws IOException {
		try {
			//			(
			ServerSocket serverSocket = new ServerSocket(9999);
			Socket clientSocket = serverSocket.accept();
			PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
			int i = 0;
			Random millisecondsGenerator = new Random();
			Random languageGenerator = new Random();
			Random nameGenerator = new Random();
			int milliseconds = 1000;
			String language = "es";
			while (true) {
				Thread.sleep(500);
//				out.println(String.format("%d:%d %d %s %s",Calendar.getInstance().get(Calendar.MINUTE), Calendar.getInstance().get(Calendar.SECOND), ++i,milliseconds,language));
				out.println(String.format("%d:%d",Calendar.getInstance().get(Calendar.MINUTE), Calendar.getInstance().get(Calendar.SECOND)));
				milliseconds = millisecondsGenerator.nextInt(4) * 250;
				language = ((languageGenerator.nextInt(2)) % 2 == 0) ? "es" : "en";
				System.out.println(String.format("%d:%d %d %s %s",Calendar.getInstance().get(Calendar.MINUTE), Calendar.getInstance().get(Calendar.SECOND), i,milliseconds,language));
			}
		} catch (IOException e) {
			System.out.println("Exception caught when trying to listen on port " + 9999 + " or listening for a connection");
			System.out.println(e.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}