package com.campudus.vertx.sessionmanager.scala

import scala.annotation.implicitNotFound
import scala.collection.JavaConversions.asScalaSet

import org.jboss.netty.handler.codec.http.CookieDecoder
import org.jboss.netty.handler.codec.http.CookieEncoder
import org.vertx.java.core.eventbus.Message
import org.vertx.java.core.http.HttpServerRequest
import org.vertx.java.core.json.JsonArray
import org.vertx.java.core.json.JsonObject
import org.vertx.java.core.Handler
import org.vertx.java.core.Vertx

class SessionHelper(val sessionManagerAddress: String, val cookieField: String, private val vertx: Vertx) {
  def this(smAddress: String, vertx: Vertx) = this(smAddress, SessionHelper.defaultCookieField, vertx)
  def this(vertx: Vertx) = this(SessionHelper.defaultSmAddress, SessionHelper.defaultCookieField, vertx)

  import scala.collection.JavaConversions._

  val eventBus = vertx.eventBus();

  trait CanBeSavedIntoJson[T] {
    def add(json: JsonArray, value: T): JsonArray
    def put(json: JsonObject, key: String, value: T): JsonObject
  }
  implicit object ArrayCanBeSavedIntoJson extends CanBeSavedIntoJson[JsonArray] {
    def add(json: JsonArray, value: JsonArray) = json.addArray(value)
    def put(json: JsonObject, key: String, value: JsonArray) = json.putArray(key, value)
  }
  implicit object BinaryCanBeSavedIntoJson extends CanBeSavedIntoJson[Array[Byte]] {
    def add(json: JsonArray, value: Array[Byte]) = json.addBinary(value)
    def put(json: JsonObject, key: String, value: Array[Byte]) = json.putBinary(key, value)
  }
  implicit object BooleanCanBeSavedIntoJson extends CanBeSavedIntoJson[Boolean] {
    def add(json: JsonArray, value: Boolean) = json.addBoolean(value)
    def put(json: JsonObject, key: String, value: Boolean) = json.putBoolean(key, value)
  }
  implicit object NumberCanBeSavedIntoJson extends CanBeSavedIntoJson[Number] {
    def add(json: JsonArray, value: Number) = json.addNumber(value)
    def put(json: JsonObject, key: String, value: Number) = json.putNumber(key, value)
  }
  implicit object ObjectCanBeSavedIntoJson extends CanBeSavedIntoJson[JsonObject] {
    def add(json: JsonArray, value: JsonObject) = json.addObject(value)
    def put(json: JsonObject, key: String, value: JsonObject) = json.putObject(key, value)
  }
  implicit object StringCanBeSavedIntoJson extends CanBeSavedIntoJson[String] {
    def add(json: JsonArray, value: String) = json.addString(value)
    def put(json: JsonObject, key: String, value: String) = json.putString(key, value)
  }

  /**
   * Puts multiple data fields into the session storage and retrieves the result of the storage
   * operation.
   *
   * @param req
   *            The http server request.
   * @param obj
   *            A JsonObject which includes key-value pairs to store.
   */
  def putSessionData(req: HttpServerRequest, obj: JsonObject) = withSessionId(req) {
    sessionId =>
      sendPutData(sessionId, obj)
  }

  /**
   * Puts multiple data fields into the session storage and retrieves the result of the storage
   * operation.
   *
   * @param req
   *            The http server request.
   * @param obj
   *            A JsonObject which includes key-value pairs to store.
   * @param doneHandler
   *            The handler to call after storing finished. It will receive the message sent by
   *            the session manager.
   */
  def putSessionDataWithResult(req: HttpServerRequest, obj: JsonObject)(doneHandler: JsonObject => Unit) = withSessionId(req) {
    sessionId =>
      sendPutData(sessionId, obj, doneHandler)
  }

  /**
   * Puts multiple data fields into the session storage and retrieves the result of the storage
   * operation.
   *
   * @param sessionId
   *            The session id.
   * @param obj
   *            A JsonObject which includes key-value pairs to store.
   */
  def putSessionData(sessionId: String, obj: JsonObject) = sendPutData(sessionId, obj)

  /**
   * Puts multiple data fields into the session storage and retrieves the result of the storage
   * operation.
   *
   * @param sessionId
   *            The session id.
   * @param obj
   *            A JsonObject which includes key-value pairs to store.
   * @param doneHandler
   *            The handler to call after storing finished. It will receive the message sent by
   *            the session manager.
   */
  def putSessionDataWithResult(sessionId: String, obj: JsonObject)(doneHandler: JsonObject => Unit) =
    sendPutData(sessionId, obj, doneHandler)

  /**
   * Puts data which can be saved into a JsonObject into the session storage without need of the result.
   *
   * @param req
   *            The http server request.
   * @param key
   *            The name of the field to put data into.
   * @param value
   *            The data to store.
   */
  def putSessionData[T: CanBeSavedIntoJson](req: HttpServerRequest, key: String, value: T) = withSessionId(req) {
    sessionId =>
      sendPutData(sessionId, implicitly[CanBeSavedIntoJson[T]].put(new JsonObject, key, value))
  }

  /**
   * Puts data which can be saved into a JsonObject into the session storage without need of the result.
   *
   * @param req
   *            The http server request.
   * @param key
   *            The name of the field to put data into.
   * @param value
   *            The data to store.
   * @param doneHandler
   *            The handler to call after storing finished. It will receive the message sent by
   *            the session manager.
   */
  def putSessionDataWithResult[T: CanBeSavedIntoJson](req: HttpServerRequest, key: String, value: T)(doneHandler: JsonObject => Unit) = withSessionId(req) {
    sessionId =>
      sendPutData(sessionId, implicitly[CanBeSavedIntoJson[T]].put(new JsonObject, key, value), doneHandler)
  }

  /**
   * Puts data which can be saved into a JsonObject into the session storage without need of the result.
   *
   * @param sessionId
   *            The session id.
   * @param key
   *            The name of the field to put data into.
   * @param value
   *            The data to store.
   */
  def putSessionData[T: CanBeSavedIntoJson](sessionId: String, key: String, value: T) =
    sendPutData(sessionId, implicitly[CanBeSavedIntoJson[T]].put(new JsonObject, key, value))

  /**
   * Puts data which can be saved into a JsonObject into the session storage without need of the result.
   *
   * @param sessionId
   *            The session id.
   * @param key
   *            The name of the field to put data into.
   * @param value
   *            The data to store.
   * @param doneHandler
   *            The handler to call after storing finished. It will receive the message sent by
   *            the session manager.
   */
  def putSessionDataWithResult[T: CanBeSavedIntoJson](sessionId: String, key: String, value: T)(doneHandler: JsonObject => Unit) =
    sendPutData(sessionId, implicitly[CanBeSavedIntoJson[T]].put(new JsonObject, key, value), doneHandler)

  private def sendPutData(sessionId: String, putObject: JsonObject, doneHandler: JsonObject => Unit = { dontCare => }): Unit = {
    val json = new JsonObject().putString("action", "put")
      .putString("sessionId", sessionId)
      .putObject("data", putObject)
    if (doneHandler != null) {
      eventBus.send(sessionManagerAddress, json, new Handler[Message[JsonObject]]() {
        override def handle(msg: Message[JsonObject]) {
          doneHandler(msg.body)
        }
      })
    } else {
      eventBus.send(sessionManagerAddress, json)
    }
  }

  /**
   * Does something with a session id. It will use the session id provided by the client inside
   * the http server request. If it cannot find it, it will create a new session.
   *
   * @param req
   *            The http server request.
   * @param handler
   *            The handler to call with the created or found session id.
   */
  def withSessionId(req: HttpServerRequest)(handler: String => Unit) {
    val value = req.headers().get("Cookie");
    if (value != null) {
      val cookies = new CookieDecoder().decode(value);
      for (cookie <- cookies) {
        if (cookie.getName == cookieField) {
          handler(cookie.getValue)
          return
        }
      }
    }

    startSession(req)(handler);
  }

  /**
   * Creates a new session for the specified request. It will provide the newly created session id
   * as a cookie.
   *
   * @param req
   *            The http server request, i.e. client, to create a session for.
   * @param handler
   *            A handler to use the created session id with.
   */
  def startSession(req: HttpServerRequest)(handler: String => Unit) {
    eventBus.send(sessionManagerAddress, new JsonObject().putString("action", "start"),
      new Handler[Message[JsonObject]]() {
        override def handle(event: Message[JsonObject]) {
          val sessionId = event.body.getString("sessionId");
          val ce = new CookieEncoder(true);
          ce.addCookie(cookieField, sessionId);
          req.response.putHeader("Set-Cookie", ce.encode());
          if (handler != null) {
            handler(sessionId);
          }
        }
      });
  }

  /**
   * Destroys a session and gives the result to a handler.
   *
   * @param sessionId
   *            The id of the session to destroy.
   * @param doneHandler
   *            The handler to call with the result of the session manager.
   */
  def destroySession(sessionId: String)(doneHandler: JsonObject => Unit) {
    eventBus.send(sessionManagerAddress,
      new JsonObject().putString("action", "destroy").putString("sessionId", sessionId),
      new Handler[Message[JsonObject]]() {
        override def handle(msg: Message[JsonObject]) {
          doneHandler(msg.body)
        }
      });
  }

  /**
   * Checks all sessions for a potential match of the specified JsonObject and returns the reply
   * of the session manager ({ matches : true/false, sessions: [...all matching sessions or empty
   * JsonArray...] }).
   *
   * @param json
   *            The JsonObject to check against all sessions.
   * @param doneHandler
   *            Handles the result of the session manager.
   */
  def checkAllSessionsForMatch(json: JsonObject)(doneHandler: JsonObject => Unit) {
    eventBus.send(sessionManagerAddress,
      new JsonObject().putString("action", "status").putString("report", "matches")
        .putObject("data", json), new Handler[Message[JsonObject]]() {
        override def handle(msg: Message[JsonObject]) {
          doneHandler(msg.body)
        }
      });

  }

  /**
   * Gets informations about open sessions and delivers this info to the specified handler.
   *
   * @param handler
   *            The handler to call after getting the results of the connections report.
   */
  def withConnectionStats(handler: JsonObject => Unit) {
    eventBus.send(sessionManagerAddress,
      new JsonObject().putString("action", "status").putString("report", "connections"),
      new Handler[Message[JsonObject]]() {
        override def handle(event: Message[JsonObject]) {
          handler(event.body);
        }
      });
  }
}

object SessionHelper {
  val defaultSmAddress = "campudus.session"
  val defaultCookieField = "sessionId"
}