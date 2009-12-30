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

package com.twitter.processing

import com.google.gson._

/**
* Methods to avoid the tedium of getting a value, checking to see if 
* it's null, and then performing some function on it.  Used like
* <code>
* ss(jsonObj, "foo") { 
*   myObj.foo = _
* }
* </code>
}
*/
trait JsonSugar {
  /**
  * Apply a function to a String value held in a JSON Object (if it's non-null)
  */
  def ss(json: JsonObject, key: String)(f: (String) => Unit) = {
    if (!json.get(key).isJsonNull) f(json.get(key).getAsString())
  }
  /**
  * Apply a function to a Boolean value held in a JSON Object (if it's non-null)
  */
  def sb(json: JsonObject, key: String)(f: (Boolean) => Unit) = {
    if (!json.get(key).isJsonNull) f(json.get(key).getAsBoolean())
  }
  /**
  * Apply a function to a Long value held in a JSON Object (if it's non-null)
  */
  def sl(json: JsonObject, key: String)(f: (Long) => Unit) = {
    if (!json.get(key).isJsonNull) f(json.get(key).getAsLong())
  }
  /**
  * Apply a function to an Int value held in a JSON Object  (if it's non-null)
  */
  def si(json: JsonObject, key: String)(f: (Int) => Unit) = {
    if (!json.get(key).isJsonNull) f(json.get(key).getAsInt())
  }
}

/**
 * Represents a Tweet
 */
class Status {
  var favorited = false;
  var text = "";
  var inReplyToUserId = -1L;
  var inReplyToStatusId = -1L;
  var inReplyToScreenName = "";
  var geo:Geo = null;
  var source = "";
  var createdAt = "";
  var user:User = null;
  var truncated = false;
  var id = -1L;
}

/**
 * Provides methods for unmarshalling a status from Json
 */
object Status extends JsonSugar{
  /**
   * One Json parser to use.  Note: this makes it not thread safe.
   */
  val parser = new JsonParser();
  /**
   * Get Some(Status) from a Json String, 
   * or None if we can't unmarshal from the given String
   */
  def fromJson(json: String): Option[Status] = {
    fromJson(parser.parse(json).getAsJsonObject())
  }

  /**
   * Get Some(Status) from a Json String, 
   * or None if the given json object isn't a status
   */
  def fromJson(rootElem: JsonObject): Option[Status] = {
    if(rootElem.has("delete") || rootElem.has("limit")) {
      None
    } else if(rootElem.has("text") && rootElem.has("id")){
      val status = new Status()
      status.favorited = rootElem.get("favorited").getAsBoolean()
      status.text = rootElem.get("text").getAsString()
      sl(rootElem, "in_reply_to_user_id") {status.inReplyToUserId = _}
      sl(rootElem, "in_reply_to_status_id") {status.inReplyToStatusId = _}
      ss(rootElem, "in_reply_to_screen_name") {status.inReplyToScreenName = _}
      if (!rootElem.get("geo").isJsonNull) status.geo = Geo.fromJson(rootElem.get("geo").getAsJsonObject())
      status.source = rootElem.get("source").getAsString()
      status.createdAt = rootElem.get("created_at").getAsString()
      status.user = User.fromJson(rootElem.get("user").getAsJsonObject)
      status.truncated = rootElem.get("truncated").getAsBoolean()
      status.id = rootElem.get("id").getAsLong()
      Some(status)
    } else {
      None
    }
  }
}

/**
 *  Represents a geo coordinate
 */
class Geo() {
  var latitude: Double = 0.0;
  var longitude: Double = 0.0;
}

/**
 * Provides methods for unmarshalling a Geo from Json
 */
object Geo {
  /**
   * One Json parser to use.  Note: this makes it not thread safe.
   */
  val parser = new JsonParser();
  /**
   * Get a geo object from the given string.
   * Note that we don't return options here, as we assume that basic
   * verification is done by the Status object
   */
  def fromJson(json: String): Geo = {
    fromJson(parser.parse(json).getAsJsonObject())
  }
  /**
   * Get a geo object from the given Json object
   * Note that we don't return options here, as we assume that basic
   * verification is done by the Status object
   */
  def fromJson(json: JsonObject): Geo = {
    val arr = json.get("coordinates").getAsJsonArray()
    val geo = new Geo()
    geo.latitude = arr.get(0).getAsDouble()
    geo.longitude = arr.get(1).getAsDouble()
    geo
  }
}

/**
 * Represents a Twitter User
 */ 
class User {
  var profileBackgroundTile = true;
  var profileSidebarBorderColor = "";
  var url = "";
  var verified = false;
  var followersCount = -1L;
  var friendsCount = -1L;
  var description = "";
  var profileBackgroundColor = "";
  var geoEnabled = false;
  var favouritesCount = -1L;
  var notifications: String = null;
  var createdAt = "";
  var profileTextColor = "";
  var timeZone = "";
  var isProtected = false;
  var profileImageURL = "";
  var statusesCount = -1L;
  var profileLinkColor = "";
  var location = "";
  var name = "";
  var following = "";
  var profileBackgroundImageURL = "";
  var screenName = "";
  var id = -1L;
  var utcOffset = 0;
  var profileSidebarFillColor = "";
}

/**
 * Provides methods for unmarshalling a User from Json
 */
object User extends JsonSugar{
  /**
   * One Json parser to use.  Note: this makes it not thread safe.
   */
  val parser = new JsonParser();
  /**
   * Get a User object from the given string.
   * Note that we don't return options here, as we assume that basic
   * verification is done by the Status object
   */
  def fromJson(json: String): User = {
    fromJson(parser.parse(json).getAsJsonObject())
  }

  /**
   * Get a geo object from the given Json object.
   * Note that we don't return options here, as we assume that basic
   * verification is done by the Status object
   */
  def fromJson(rootElem: JsonObject): User = {
    val user = new User()
    user.profileBackgroundTile = rootElem.get("profile_background_tile").getAsBoolean()
    user.profileSidebarBorderColor = rootElem.get("profile_sidebar_border_color").getAsString()
    ss(rootElem, "url") {user.url = _}
    user.verified = rootElem.get("verified").getAsBoolean()
    user.followersCount = rootElem.get("followers_count").getAsLong()
    user.friendsCount = rootElem.get("friends_count").getAsLong()
    ss(rootElem, "description") { user.description = _ }
    user.profileBackgroundColor = rootElem.get("profile_background_color").getAsString()
    user.geoEnabled = rootElem.get("geo_enabled").getAsBoolean()
    user.favouritesCount = rootElem.get("favourites_count").getAsLong()
    ss(rootElem, "notifications") {user.notifications = _ }
    user.createdAt = rootElem.get("created_at").getAsString()
    user.profileTextColor = rootElem.get("profile_text_color").getAsString()
    ss(rootElem, "time_zone") {user.timeZone = _}
    user.isProtected = rootElem.get("protected").getAsBoolean()
    user.profileImageURL = rootElem.get("profile_image_url").getAsString()
    user.statusesCount = rootElem.get("statuses_count").getAsLong()
    user.profileLinkColor = rootElem.get("profile_link_color").getAsString()
    ss(rootElem, "location") {user.location = _}
    user.name = rootElem.get("name").getAsString()
    ss(rootElem, "following") {user.following = _}
    user.profileBackgroundImageURL = rootElem.get("profile_background_image_url").getAsString()
    user.screenName = rootElem.get("screen_name").getAsString()
    user.id = rootElem.get("id").getAsLong()
    si(rootElem, "utc_offset") {user.utcOffset = _}
    user.profileSidebarFillColor = rootElem.get("profile_sidebar_fill_color").getAsString()
    user
  }
}

