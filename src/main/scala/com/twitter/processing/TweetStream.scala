/*
  (c) copyright
  
  This library is free software; you can redistribute it and/or
  modify it under the terms of the GNU Lesser General Public
  License as published by the Free Software Foundation; either
  version 2.1 of the License, or (at your option) any later version.

  This library is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  Lesser General Public License for more details.

  You should have received a copy of the GNU Lesser General
  Public License along with this library; if not, write to the
  Free Software Foundation, Inc., 59 Temple Place, Suite 330,
  Boston, MA  02111-1307  USA
 */

package com.twitter.processing;


import _root_.processing.core.PApplet

import java.io.{BufferedReader, InputStream, InputStreamReader, IOException}
import java.lang.reflect.Method
import java.net.{ConnectException, SocketException}

import org.apache.http.{HttpException, HttpResponse}
import org.apache.http.auth.{UsernamePasswordCredentials, AuthScope}
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.params.{BasicHttpParams, HttpConnectionParams}


/**
 * Provides simple (hacky) logging.  You can comment out the printlns to suppress output
 */
trait Log {
  def debug(msg: String, args: Any*):Unit = {
    //println(msg.format(args:_*))
  }
  def trace(msg: String, args: Any*):Unit = {
    //println(msg.format(args:_*))
  }
  def info(msg: String, args: Any*):Unit = {
    println(msg.format(args:_*))
  }
  def warning(msg: String, args: Any*):Unit = {
    println(msg.format(args:_*))
  }
  def error(msg: String, args: Any*):Unit = {
    println(msg.format(args:_*))
  }
}
/**
 * Provides a Processing interface to the Twitter streaming API.
 * Note that this interface will <b>ONLY</b> work with JSON formatted streams.
 * 
 * @example SimpleStream
 * @author Mark McBride
 * 
 */
class TweetStream(parent: PApplet, host: String, port: Int, path: String, username: String, password: String) extends Log{

  /** the version of the library */
	val VERSION = "0.1.0"
	/** the full URI of the request */
	val name = "http://" + host + ":" + port + "/" + path

  /** 
   * A structural type that defines a tweet(status: Status) method.  
   * If our parent conforms to this type we can call back to it 
   */
  type T = {def tweet(status: Status)}
  /** 
   * We'll attempt to cast parent and set callbackApplet.  
   * If it stays null we don't callback.  Note that this
   * makes initializing a TweetStream object sorta pointless.
   */
  var callbackApplet: T = null

  // various metrics/counters

  /** how long we've spent connecting */
  var connectTimeMs = 0L
  /** how long we've spent in a read loop */
  var readLoopTimeMs = 0L
  /** how long we've spent disconnecting */
  var disconnectTimeMs = 0L
  /** 
   * how many idle probes we've received from the server.  These are empty
   * lines sent to keep the stream open.
   */
  var idleProbe = 0
  /** how many times we've stalled trying to read length delimited content */
  var delimitedReadStall = 0
  /** the time at which we last received a tweet */
  var lastReceivedMs = 0L

  // HTTP connection parameters
  /** how long to wait for a connection */
  var connectTimeoutMs = 10000;
  /** how long to wait for more input on the stream */
  var socketTimeoutMs = 15000;
  // the HTTP client to use
  /** an Apache commons http client to use for communication */
  var client:DefaultHttpClient = _
  /** the content stream from our connection */
  var stream: InputStream = _
  /** the reader for the stream for our connection */
  var reader: BufferedReader = _
  // control flags
  /** set when we want processing to terminate */
  var halt:Boolean = false
  /** whether we're using delimited length mode */
  val delimitedLength:Boolean = name.indexOf("delimited=length") > 0
  /** how many times we'll try to reconnect when network hiccups occur */
  val maxReconnectTries = 10

  /**
   * Connect to the streaming API and start received tweets.
   * Under the covers this calls setup, then forks a thread that
   * calls runLoop.
   */
  def go() {
    setup()
    new Thread {
      override def run() = {
        runLoop()
      }
    }.start
  }

  /**
   * Set up our HTTP connection parameters, 
   * and try to cast our parent to a callbackable form.
   */
  def setup() = {
    val connectionParams = new BasicHttpParams()
    if (connectTimeoutMs > 0) {
      HttpConnectionParams.setConnectionTimeout(connectionParams, connectTimeoutMs)
    }
    if (socketTimeoutMs >= 0) {
      HttpConnectionParams.setSoTimeout(connectionParams, socketTimeoutMs)
    }
    client = new DefaultHttpClient(connectionParams)
    // get basic auth set up
    client.getCredentialsProvider().
      setCredentials(new AuthScope(host, port, AuthScope.ANY_REALM),
                    new UsernamePasswordCredentials(username, password));

    // find applet callback
    try {
      callbackApplet = parent.asInstanceOf[T]
    } catch {
      case e => {
        e.printStackTrace()
      }
    }
  }

  /**
   * while halt isn't set, perform the following
   * <ol>
   * <li>call reconnect (establishing a connection)</li>
   * <li>call readLoop (read tweets)</li>
   * </ol>
   * if an exception is caught, try to repeat
   */
  def runLoop() {
    while(!halt) {
      try {
        // reconnect tries to connect several times
        reconnect()
        readLoop()
      } catch {
        case ex: ConnectException => {
          warning("%s connection refused %s", name, ex.toString())
        }
        case ex: IOException => {
          warning("%s caught IOexception %s", name, ex.toString())
        }
        case ex: HttpException => {
          warning("%s caught HttpException %s", name, ex.toString())
        }
        case ex: SocketException => {
          warning("%s caught SocketException %s", name, ex.toString())
        }
        case e => {
          error("%s unexpected exception in read loop %s", name, e.toString())
          e.printStackTrace()
        }
      }
      disconnect()
    }
  }

  /**
   * Try up to maxRecconnectTries times to connect, with exponentially
   * increasing backoff times (tries*tries*1000 ms)
   */ 
  def reconnect() = {
    var tries = 1
    setup()
    while(!connect() && tries < maxReconnectTries) {
      // exponential backoff on reconnect attempts
      val sleepTime = tries*tries*1000
      info("reconnect failed. sleeping for %d ms before attempting next reconnect", sleepTime)
      Thread.sleep(sleepTime)
      tries += 1
    }
  }

  /**
   * Establish a connection.  This tries only once (reconnect wraps this in retry logic)
   * Returns true if a connection was established, false otherwise.  Note that this
   * also initializes the stream and reader fields, used by readLoop
   */
  def connect(): Boolean = {
    try {
      val start = System.currentTimeMillis()
      info("connecting to twitter stream %s", name)
      val method = new HttpGet(name)
      val response = client.execute(method)
      val responseCode = response.getStatusLine().getStatusCode()

      debug("got response %s", response.getStatusLine())
      if (responseCode != 200) {
        warning("Got non-200 response %d from connect", responseCode)
        // unsuccessful connect.  We have to dispose of our client and recreate,
        // as we will have an open stream at this point
        disconnect()
        setup()
        false
      } else {
        stream = response.getEntity().getContent()
        if (stream == null) {
          warning("Could not open a response stream for %s", name)
          false
        } else {
          reader = new BufferedReader(new InputStreamReader(stream))
          val end = System.currentTimeMillis()
          connectTimeMs = end - start
          info("connected to twitter stream %s", name)
          if (delimitedLength) {
            info("using length-delimited format")
          }
          true
        }
      }
    } catch {
      case e => {
        error("couldn't connect to %s: %s", name, e.toString())
        e.printStackTrace()
        false
      }
    }
  }
  /**
   * Read tweets until either halt is set or we get an exception
   */
  def readLoop() {
    readLoopTimeMs = 0L
    val start = System.currentTimeMillis()
    try {
      while (!halt) {
        val line = readLine()
        if (!halt) {
          if (line.length() == 0) {
            idleProbe += 1
          } else {
            deliver(line)
          }
        }
      }
    } finally {
      debug("readLoop done")
      val end = System.currentTimeMillis()
      readLoopTimeMs = end - start
    }
  }

  /**
   * If your callback is set, call tweet() on it.  Otherwise noop
   */ 
  def deliver(tweet: String) = {
    // deliver the tweet
    //println("got tweet: %s", tweet)
    if (callbackApplet != null) {
      Status.fromJson(tweet) match {
        case (Some(status)) => {
          callbackApplet.tweet(status)
        }
        case _ => null
      }
    }
  }

  /**
   * Read a line of input from the stream.  
   * This line should either be a blank line (idle probe) or a full tweet
   * readLine will block until more data arrives. Not interruptable.
   */
  def readLine(): String = {
    var rv: String = null
    var haltRead: Boolean = false
    // this is the recommended (safer) mode of operation
    if (delimitedLength) {
      val lengthStr = reader.readLine
      if (lengthStr == null) {
        trace("%s delimited read null, end of stream", name)
        // will halt
      } else if (lengthStr.length == 0) {
        // Probably idle probe, account for below
        trace("%s delimited read idle probe", name)
        rv = ""
      } else {
        try {
          // Expect: Integer\n
          val length = Integer.parseInt(lengthStr)
          trace("%s delimited read length=%d", name, length)
          val buf = new Array[Char](length)
          var position = 0
          var recv = 0
          while (length - position > 0) {
            recv = reader.read(buf, position, length - position)
            if (recv == -1) {
              haltRead = true
              position = length // terminate while loop
            } else {
              position += recv
              if (position < length) {
                delimitedReadStall += 1
                trace("%s delimited read stall", name)
                Thread.sleep(25)
              }
            }
          }
          if (haltRead == false) rv = new String(buf).trim
        } catch {
          case ex: NumberFormatException => {
            error("%s halt due to length parse error on %s", name, lengthStr)
            // will halt
          }
        }
      }
    } else {
      rv = reader.readLine()
    }

    if (rv == null) {
      haltRead = true
    } else {
      lastReceivedMs = System.currentTimeMillis()
    }
    rv
  }

  /**
   * Stop our read loop, don't process anything else.
   */
  def stop() = {
    halt = true;
  }

  /**
   * Dissconnect from our source
   */
  def disconnect() {
    info("disconnecting %s", name)
    disconnectTimeMs = 0L
    val start = System.currentTimeMillis()
    try {
      stream.close()
    } catch {
      case _ => null
    }
    client = null
    stream = null
    reader = null
    val end = System.currentTimeMillis()
    disconnectTimeMs = end - start
  }

}
