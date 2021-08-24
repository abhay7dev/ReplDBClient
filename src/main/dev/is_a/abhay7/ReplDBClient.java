/*

MIT License

Copyright (c) 2021 Abhay

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

*/

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;

import java.nio.charset.StandardCharsets;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class ReplDBClient {

	// Variables

	private String url;
	private boolean encoded;
	private boolean cached;
	private boolean debug;
	private final HttpClient httpClient = HttpClient.newBuilder().version(HttpClient.Version.HTTP_2).build();

	// Constructors

	public ReplDBClient() {
		this(System.getenv("REPLIT_DB_URL"), true, true, false);
	}

	public ReplDBClient(String url) {
		this(url, true, true, false);
	}

	public ReplDBClient(String url, boolean encoded) {
		this(url, encoded, true, false);
	}

	public ReplDBClient(String url, boolean encoded, boolean cached) {
		this(url, encoded, cached, false);
	}

	public ReplDBClient(boolean encoded) {
		this(System.getenv("REPLIT_DB_URL"), encoded, true, false);
	}

	public ReplDBClient(boolean encoded, boolean cached) {
		this(System.getenv("REPLIT_DB_URL"), encoded, cached, false);
	}

	public ReplDBClient(boolean encoded, boolean cached, boolean debug) {
		this(System.getenv("REPLIT_DB_URL"), encoded, cached, debug);
	}

	public ReplDBClient(String url, boolean encoded, boolean cached, boolean debug) {

		this.encoded = encoded;
		this.cached = cached;
		this.debug = debug;
		
		try {

			URI uri = new URI(url);

			if(!uri.getHost().equals("kv.replit.com")) {
				throw new Exception("Invalid host for provided database URL");
			}

			if(!uri.getScheme().equals("https")) {
				throw new Exception("Provided URL is not https");
			}

		} catch(Exception e) {
			if (e.getMessage() != null) System.out.println(e.getMessage());
			System.err.println("Invalid URL provided for ReplDBClient");
			System.exit(1);
		}

		this.url = url;
		
	}

	// Initialize

	public void init() throws IOException, InterruptedException {
		this.cache = this.getAllStart();
		// this.print();
	}

	// Set Value(s)

	public void set(String key, String value) throws IOException, InterruptedException {

		Map<Object, Object> data = new HashMap<>();

		if (encoded) {
			key = encode(key);
			value = encode(value);
		}
		
		data.put(key, value);
		cache.put(key, value);

		HttpRequest request = HttpRequest.newBuilder()
			.POST(ofFormData(data))
			.uri(URI.create(this.url))
			.setHeader("Content-Type", "application/x-www-form-urlencoded")
			.build();

		httpClient.send(request, HttpResponse.BodyHandlers.ofString());
	}

	public void set(Map<String, String> pairs) throws IOException, InterruptedException {
		for (Map.Entry<String, String> entry : pairs.entrySet()){
			this.set(entry.getKey(), entry.getValue());
		}
	}

	// Get Value(s)

	public String get(String key) {
		
		if (encoded) key = encode(key);

		if(cache != null) {
			return cache.get(key);
		} else {

			HttpRequest request = HttpRequest.newBuilder()
				.GET()
				.uri(URI.create(this.url + "/" + key))
				.build();

			try {

				HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

				if (encoded) {
					return decode(response.body());
				} else {
					return response.body();
				}

			} catch(Exception e) {
				System.out.println("Error getting value");
			}

		}

		return null;

	}

	public String[] get(String... keys) {
		String[] toRet = new String[keys.length];

		for (int i = 0; i < toRet.length; i++) {
			toRet[i] = get(keys[i]);
		}

		return toRet;
	}

	public String[] list()  {

		return cache.keySet().toArray(new String[cache.size()]);

	}

	public String[] list(String pre) throws IOException, InterruptedException {

		HttpRequest request = HttpRequest.newBuilder()
			.GET()
			.uri(URI.create(this.url + "?prefix=" + pre))
			.build();

		HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

		String[] tokens = new String[0];
		if(encoded) {
			tokens = decode(response.body()).split("\n");
		} else {
			tokens = response.body().split("\n");
		}

		return tokens;

	}

	public Map<String, String> getAll() {
		return this.cache;
	}

	private Map<String, String> getAllStart() throws IOException, InterruptedException {

		Map<String, String> toRet = new HashMap<String, String>();

		for (String k: this.list("")) {
			if (encoded) {
				toRet.put(
					decode(k), 
					this.get(k)
				);
			} else {
				toRet.put(k, get(k));
			}
		}

		return toRet;
	}

	// Delete Value(s)

	public void delete(String key) throws IOException, InterruptedException {

		if(encoded) {
			key = encode(key);
		}

		HttpRequest request = HttpRequest.newBuilder()
			.DELETE()
			.uri(URI.create(this.url + "/" + key))
			.build();

		httpClient.send(request, HttpResponse.BodyHandlers.ofString());

		cache.remove(key);

	}

	public void delete(String... keys) throws IOException, InterruptedException {

		for(String key: keys) {
			delete(key);  
		}

	}

	public void empty() throws IOException, InterruptedException {

		for(String key: getAll().keySet()) {
			delete(key);
		}

	}

	// Utilities

	private static HttpRequest.BodyPublisher ofFormData(Map<Object, Object> data) {

		StringBuilder builder = new StringBuilder();

		for (Map.Entry<Object, Object> entry : data.entrySet()) {

			if (builder.length() > 0) {
				builder.append("&");
			}

			builder.append(URLEncoder.encode(entry.getKey().toString(), StandardCharsets.UTF_8));

			builder.append("=");

			builder.append(URLEncoder.encode(entry.getValue().toString(), StandardCharsets.UTF_8));

		}

		return HttpRequest.BodyPublishers.ofString(builder.toString());
	}

	private static String encode(String encodeMe) {

		if (encodeMe == null) {
			return "";
		}

		try {

			String encoded = URLEncoder.encode(encodeMe, StandardCharsets.UTF_8.name());

			return encoded;

		} catch (UnsupportedEncodingException ex) {
			throw new RuntimeException(ex);
		}
	}

	private String decode(String s) {
		if (s == null) {
			return "";
		}

		try {
			
			String decoded = URLDecoder.decode(s, StandardCharsets.UTF_8.name());
			return decoded;

		} catch (UnsupportedEncodingException ex) {
			throw new RuntimeException(ex);
		}
	}

	public void print() {

		System.out.println("Printing");

		if(this.cache != null) {

			for (Map.Entry<String, String> entry : this.cache.entrySet()) {
				System.out.println(decode(entry.getKey()) + ": " + decode(entry.getValue().toString()));
			}

		} else {
			System.out.println("Map is currently set to null");
		}

	}
	
	// End Utilities

}