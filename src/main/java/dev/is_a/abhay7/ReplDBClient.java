/*

MIT License

Copyright (c) 2021 Abhay

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

*/

package dev.is_a.abhay7;

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

/**
* The ReplDBClient class is a single class Java Client for the Database available in a replit.com repl. It offers simplistic usage and is also safe, making sure applications using this do not crash
* 
* @author  Abhay
* @version 1.0-SNAPSHOT
* @see     <a href="https://replit.com">https://replit.com</a>
* @since   2021-10-09
*/
public class ReplDBClient {

	// Variables
	private final String url;
	private final boolean encoded;
	private final boolean cached;
	private static Map<String, String> cache = null;
	private boolean debug;
	private final HttpClient httpClient = HttpClient.newBuilder().version(HttpClient.Version.HTTP_2).build();

	// Constructors

	/**
	* Class Constructor with default values passed
	*/
	public ReplDBClient() {
		this(System.getenv("REPLIT_DB_URL"), true, true, false);
	}

	/**
	* Class Constructor with database url provided
	* @param url The URL of the database
	*/
	public ReplDBClient(String url) {
		this(url, true, true, false);
	}

	/**
	* Class Constructor with database url and option of cache provided
	* @param url    The URL of the database
	* @param cached Whether to cache the database in memory or not
	*/
	public ReplDBClient(String url, boolean cached) {
		this(url, cached, true, false);
	}

	/**
	* Class Constructor with database url and option of cache provided
	* @param url     The URL of the database
	* @param cached  Whether to cache the database in memory or not
	* @param encoded Whether to double encode pairs in the database or not
	*/
	public ReplDBClient(String url, boolean cached, boolean encoded) {
		this(url, cached, encoded, false);
	}

	/**
	* Class Constructor with database url and option of cache provided
	* @param cached Whether to cache the database in memory or not
	*/
	public ReplDBClient(boolean cached) {
		this(System.getenv("REPLIT_DB_URL"), cached, true, false);
	}

	/**
	* Class Constructor with database url and option of cache provided
	* @param cached  Whether to cache the database in memory or not
	* @param encoded Whether to double encode pairs in the database or not
	*/
	public ReplDBClient(boolean cached, boolean encoded) {
		this(System.getenv("REPLIT_DB_URL"), cached, encoded, false);
	}

	/**
	* Class Constructor with database url and option of cache provided
	* @param cached  Whether to cache the database in memory or not
	* @param encoded Whether to double encode pairs in the database or not
	* @param debug   Whether to enable debug mode in the client
	*/
	public ReplDBClient(boolean cached, boolean encoded, boolean debug) {
		this(System.getenv("REPLIT_DB_URL"), cached, encoded, debug);
	}

	/**
	* Class Constructor with database url and option of cache provided
	* @param url     The URL of the database
	* @param cached  Whether to cache the database in memory or not
	* @param encoded Whether to double encode pairs in the database or not
	* @param debug   Whether to enable debug mode in the client
	*/
	public ReplDBClient(String url, boolean cached, boolean encoded, boolean debug) {

		this.cached = cached;
		this.encoded = encoded;
		this.debug = debug;

		try {

			URI uri = new URI(url);

			if(!uri.getHost().equals("kv.replit.com")) {
				throw new IllegalArgumentException("Invalid host \"" + uri.getHost() + "\"for provided database URL. Please provide one with the host \"kv.replit.com\"");
			}

			if(!uri.getScheme().equals("https")) {
				throw new IllegalArgumentException("Provided URL scheme is not https");
			}

		} catch(Exception e) {
			if (e.getMessage() != null) System.out.println("[REPLDB] " + e.getMessage());
			System.err.println("Invalid URL provided for ReplDBClient");
			throw new IllegalArgumentException("Invalid URL provided for ReplDBClient");
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

	/**
	* Gets the value associated with a key from the database or cache
	* @param keyRaw The raw key which to get the value from
	* @returns The value in the database associated with the passed in key
	*/
	public String get(String keyRaw) {
		return this.get(keyRaw, this.cached);
	}

	/**
	* Gets the value associated with a key from the database or cache
	* @param keyRaw The raw key which to get the value from
	* @param cached Whether to get the item from cache or through the network. If cache has been disabled,
	* @returns The value in the database associated with the passed in key
	*/
	public String get(String key, boolean cached) {

		key = this.encoded ? encode(key) : key; // Encode key if encoded

		if(this.cached && cached && ReplDBClient.cache.containsKey(key)) {
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

	/**
	* Gets the values associated with a keys from the database or cache
	* @param keys They raw keys to get the value from
	* @returns The values in the database associated with the passed in keys
	* @see #get(String, boolean) get
	*/
	public String[] get(String... keys) {
		return this.get(this.cached, keys);
	}

	/**
	* Gets the values associated with a keys from the database or cache
	* @param cached Whether to get the keys from the cache or network
	* @param keys They raw keys to get the value from
	* @returns The values in the database associated with the passed in keys
	* @see #get(String, boolean) get
	*/
	public String[] get(boolean cached, String... keys) {
		String[] toRet = new String[keys.length];

		for (int i = 0; i < toRet.length; i++) {
			toRet[i] = this.get(keys[i], cached);
		}

		return toRet;
	}

	// Set key to value

	/**
	* Sets a key to a value in the database
	* @param key   The key for the pair
	* @param value They value for the pair
	* @returns Whether setting the pair was a success
	*/
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

	/**
	* Sets keys to values in the database
	* @param keys  The keys for the pairs
	* @param value They value for the pair
	* @returns A boolean array highlighting whether each pair was succesfully added
	* @see #set(String, String) set
	*/
	public boolean[] set(String[] keys, String[] values) {
		boolean[] toRet = new boolean[Math.min(keys.length, values.length)];
		for(int i = 0; i < toRet.length; i++) {
			toRet[i] = this.set(keys[i], values[i]);
		}
		return toRet;
	}

	/**
	* Sets keys to values in the database
	* @param pairs The key/value pairs to set in the database
	* @returns A boolean array highlighting whether each pair was succesfully added
	* @see #set(String, String) set
	*/
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

	/**
	* Deletes a pair with the associated key in the database
	* @param key The key of the pair to delete
	* @returns Whether the pair was succesfully deleted in the database
	*/
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

	/**
	* Deletes the pair with the associated keys in the database
	* @param keys The keys of the pairs to delete
	* @returns Which pairs were succesfully deleted in the database
	* @see #delete(String) delete
	*/
	public boolean[] delete(String... keys) {
		boolean[] bools = new boolean[keys.length];
		for(int i = 0; i < keys.length; i++) {
			bools[i] = this.delete(keys[i]);
		}
		return bools;
	}

	/**
	* Empties the database (Deletes all keys)
	* @returns Which keys were succesfully deleted
	* @see #delete(String) delete
	*/
	public boolean[] empty() {
		return this.delete(this.list());
	}

	// List keys

	/**
	* Lists the keys in the database
	* @returns The keys
	* @see #list(String, boolean) list
	*/
	public String[] list()  {
		return this.list("");
	}

	/**
	* Lists the keys in the database
	* @param prefix The prefix of the keys to list
	* @returns The keys
	* @see #list(String, boolean) list
	*/
	public String[] list(String prefix) {
		return this.list(prefix, this.cached);
	}

	/**
	* Lists the keys in the database
	* @param cached Whether to get the keys from the cache
	* @see #list(String, boolean) list
	*/
	public String[] list(boolean cached) {
		return this.list("", cached);
	}

	/**
	* Lists the keys in the database
	* @param prefix The prefix of the keys to list
	* @param cached Whether to get the keys from the cache
	* @returns The keys
	*/
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

	/**
	* Returns the database url
	* @returns The url
	*/
	public String getURL() {
		return this.url;
	}

	/**
	* Returns whether the database is cached
	* @returns Whether it is cached
	*/
	public boolean isCached() {
		return this.cached;
	}

	/**
	* Returns whether the database is encoded
	* @returns Whether it is encoded
	*/
	public boolean isEncoded() {
		return this.encoded;
	}

	/**
	* Returns whether the client is in debug mode
	* @returns Whether it is in debug mode
	*/
	public boolean isDebug() {
		return this.debug;
	}

	/**
	* Sets whether the client is in debug mode
	* @param Set it in debug mode or not
	*/
	public void setDebug(boolean debug) {
		this.debug = debug;
	}

}