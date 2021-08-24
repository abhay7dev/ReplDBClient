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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class ReplDBClient {

	// Variables

	private String url;
	private boolean encoded;
	private boolean cached;
	private static Map<String, String> cache = null;
	private boolean debug;
	private final HttpClient httpClient = HttpClient.newBuilder().version(HttpClient.Version.HTTP_2).build();

	// Constructors

	// TODO - Use boolean... bools with one constructor rather than making it redundant with a bunch

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
				throw new Exception("Invalid host \"" + uri.getHost() + "\"for provided database URL. Please provide one with the host \"kv.replit.com\"");
			}

			if(!uri.getScheme().equals("https")) {
				throw new Exception("Provided URL scheme is not https");
			}

		} catch(Exception e) {
			if (e.getMessage() != null) System.out.println("[REPLDB] " + e.getMessage());
			System.err.println("Invalid URL provided for ReplDBClient");
			System.exit(1);
		}

		this.url = url;

		if(this.cached && ReplDBClient.cache == null) {
			ReplDBClient.cache = new HashMap<String, String>();
			for(String s: this.list(false)) {
				ReplDBClient.cache.put(s, this.get(s));
			}
			if(this.debug) {
				System.out.println("[REPLDB] Initialized cache: " + ReplDBClient.cache);
			}
		}

		if(this.debug) {
			System.out.println("[REPLDB] Initialized ReplDBClient with the following values\n\tencoded = " + this.encoded + "\n\turl = " + this.url + "\n\tcached = " + this.cached);
		}

	}

	// Get value by key

	public String get(String keyRaw) {
		return this.get(keyRaw, this.cached);
	}

	public String get(String key, boolean cached) {

		key = this.encoded ? encode(key) : key; // Encode key if encoded

		if(cached && ReplDBClient.cache.containsKey(key)) {
			// Check if cached, if so, return the value at cache
			String valueRaw = ReplDBClient.cache.get(key);
			String value = this.encoded ? decode(valueRaw) : valueRaw;
			if(this.debug) {
				System.out.println("[REPLDB] Got \"" + value + "\" from \"" + key + "\" from cache");
			}
			return value;
		}

		HttpRequest request = HttpRequest.newBuilder().GET().uri(URI.create(this.url + "/" + key)).build(); // Create HTTP Request

		HttpResponse<String> response = null;

		try {
			response = httpClient.send(request, HttpResponse.BodyHandlers.ofString()); // Send HTTP request
		} catch(Exception e) {
			if(e.getMessage() != null && e.getMessage() != "") {
				System.out.println("[REPLDB] " + e.getMessage());
			}
			e.printStackTrace();
			System.err.println("[REPLDB] Failed to GET \"" + key + "\"");
			return null;
		}

		if(response != null) {

			if(this.debug) {
				System.out.println("[REPLDB] Recieved response \"" + response.body() + "\" through GET HTTP Request");
			}

			// Return value
			String value = this.encoded ? decode(response.body()) : response.body();
			return value;
		}

		System.out.println("[REPLDB] Failed to get. Returning null");
		return null;
	}

	public String[] get(String... keys) {
		return this.get(this.cached, keys);
	}

	public String[] get(boolean cached, String... keys) {
		String[] toRet = new String[keys.length];

		for (int i = 0; i < toRet.length; i++) {
			toRet[i] = this.get(keys[i], cached);
		}

		return toRet;
	}

	// Set key to value

	public boolean set(String key, String value) {

		if(this.encoded) {
			key = this.encode(key);
			value = this.encode(value);
		}

		HttpRequest request = HttpRequest.newBuilder().POST(getPostData(key, value)).uri(URI.create(this.url)).setHeader("Content-Type", "application/x-www-form-urlencoded").build();

		try {
			httpClient.send(request, HttpResponse.BodyHandlers.ofString()); // Send HTTP request
		} catch(Exception e) {
			if(e.getMessage() != null && e.getMessage() != "") {
				System.out.println("[REPLDB] " + e.getMessage());
			}
			e.printStackTrace();
			System.err.println("[REPLDB] Failed to SET \"" + key + "\" to \"" + value + "\"");
			return false;
		}

		if(this.cached && !ReplDBClient.cache.containsKey(key)) {
			ReplDBClient.cache.put(key, value);
		}

		if(this.debug) {
			System.out.println("[REPLDB] Set \"" + key + "\" to \"" + value + "\"");
		}

		return true;	

	}

	public boolean[] set(String[] keys, String[] pairs) {
		boolean[] toRet = new boolean[Math.min(keys.length, pairs.length)];
		for(int i = 0; i < toRet.length; i++) {
			toRet[i] = this.set(keys[i], pairs[i]);
		}
		return toRet;
	}

	public boolean[] set(Map<String, String> pairs) {
		boolean[] toRet = new boolean[pairs.size()];
		int nextIndex = 0;
		for (Map.Entry<String, String> entry : pairs.entrySet()){
			toRet[nextIndex] = this.set(entry.getKey(), entry.getValue());
			nextIndex++;
		}
		return toRet;
	}

	// Delete key/value pair

	public boolean delete(String key) {

		key = this.encoded ? encode(key) : key;

		HttpRequest request = HttpRequest.newBuilder().DELETE().uri(URI.create(this.url + "/" + key)).build();

		try {
			httpClient.send(request, HttpResponse.BodyHandlers.ofString()); // Send HTTP request
		} catch(Exception e) {
			if(e.getMessage() != null && e.getMessage() != "") {
				System.out.println("[REPLDB] " + e.getMessage());
			}
			e.printStackTrace();
			System.err.println("[REPLDB] Failed to DELETE \"" + key + "\"");
			return false;
		}

		if(this.cached && ReplDBClient.cache.containsKey(key)) {
			ReplDBClient.cache.remove(key);
		}

		if(this.debug) {
			System.out.println("[REPLDB] Deleted \"" + key + "\"");
		}

		return true;

	}

	public boolean[] delete(String... keys) {
		boolean[] bools = new boolean[keys.length];
		for(int i = 0; i < keys.length; i++) {
			bools[i] = this.delete(keys[i]);
		}
		return bools;
	}

	public boolean[] empty() {
		return this.delete(this.list());
	}

	// List keys

	public String[] list()  {
		return this.list("");
	}

	public String[] list(String prefix) {
		return this.list(prefix, this.cached);
	}

	public String[] list(boolean cached) {
		return this.list("", cached);
	}

	public String[] list(String prefix, boolean cached)  {

		prefix = this.encoded ? this.encode(prefix) : prefix;
		if(prefix == null) prefix = "";

		if(cached && this.cache != null) {

			ArrayList<String> keysWithPrefix = new ArrayList<String>();

			for(String k: this.cache.keySet()) {
				if(k.startsWith(prefix)) {
					keysWithPrefix.add(k);
				}
			}

			if(this.debug) {
				System.out.println("[REPLDB] Recieved list of keys " + keysWithPrefix + " with prefix \"" + prefix + "\" from cache.");
			}

			return keysWithPrefix.toArray(new String[0]);
			
		} else {
			
			String reqURL = this.url + "?prefix=" + prefix;
			HttpRequest request = HttpRequest.newBuilder().GET().uri(URI.create(reqURL)).build();

			HttpResponse<String> response = null;

			try {
				response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
			} catch(Exception e) {
				if(e.getMessage() != null && e.getMessage() != "") {
					System.out.println("[REPLDB] " + e.getMessage());
				}
				e.printStackTrace();
				System.err.println("[REPLDB] Failed to list keys");
				return null;
			}

			String[] tokens = new String[0];
			if(encoded) {
				tokens = decode(response.body()).split("\n");
			} else {
				tokens = response.body().split("\n");
			}

			if(this.debug) {
				System.out.println("[REPLDB] Recieved list of keys " + tokens + " with prefix \"" + prefix + "\" from http request.");
			}

			return tokens;
		}

	}

	// Utilities

	private HttpRequest.BodyPublisher getPostData(String keyRaw, String valueRaw) { // Convert key/value pair to urlencoded String
		return HttpRequest.BodyPublishers.ofString(
			this.encode(keyRaw) +
			"=" +
			this.encode(valueRaw)
		);
	}

	private String encode(String toEncode) { // equivalent to js's encodeURIComponent
		if (toEncode == null || toEncode == "") return "";
		return URLEncoder.encode(toEncode, StandardCharsets.UTF_8);
	}

	private String decode(String toDecode) { // equivalent to js's decodeURIComponent
		if (toDecode == null || toDecode == "")	return "";
		try {
			return URLDecoder.decode(toDecode, StandardCharsets.UTF_8.name());
		} catch(UnsupportedEncodingException uee) {
			if(uee.getMessage() != null && uee.getMessage() != "") {
				System.out.println("[REPLDB] " + uee.getMessage());
			}
			uee.printStackTrace();
			System.err.println("[REPLDB] Failed to decode \"" + toDecode + "\"");
			return "";
		}
	}
}