package org.jrgv89.zookeeper;

/**
 * Created by JRGv89 on 25/5/16.
 */
//
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import java.net.*;
import java.io.*;
public class Test {
static String rsvps ="http://stream.meetup.com/2/rsvps";
	private static final String SENT_MESSAGE = "Hello World";

	public static void main(String[] args) throws URISyntaxException, IOException {
		CloseableHttpClient httpclient = HttpClientBuilder.create().build();
		HttpGet httpget = new HttpGet(rsvps);
		HttpResponse response = httpclient.execute(httpget);
		HttpEntity entity = response.getEntity();
		BufferedReader rd = new BufferedReader(
				new InputStreamReader(entity.getContent()));
		String line;
		while ((line = rd.readLine()) != null) {
			System.out.println(line);
		}

	}
	//
}
