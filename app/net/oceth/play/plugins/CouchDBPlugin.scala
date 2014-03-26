package net.oceth.play.plugins

import play.api._
import play.api.libs.ws.{Response, WS}
import java.net.{URLEncoder, URL}
import play.api.libs.json._
import scala.concurrent._
import scala.concurrent.duration._
import net.oceth.play.plugins.CouchDBPlugin.{DBAccess, Server, Authentication}
import scala.collection.JavaConversions._
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import java.util.Collections
import scala.collection.mutable
import scala.io.Source
import play.api.libs.ws.Response
import play.api.libs.json.JsString
import scala.Some
import play.api.libs.json.JsObject
import com.ning.http.client.Realm.AuthScheme

/**
 * This plugin provides the couch db api and checks the couch db views and updates or creates them if necessary.
 *
 * @author Gerry Gunzenhauser
 */
class CouchDBPlugin(app: Application) extends Plugin {
  val log = Logger(classOf[CouchDBPlugin])

  def validateDesignDoc(doc: JsValue, cfg: Configuration): Boolean = false

  def resolveResource(resource: String): String = {
    log.debug(s"Loading resource '$resource'")
    Source.fromInputStream(getClass.getResourceAsStream(resource)).buffered.mkString
  }

  def updateDesign(dba: DBAccess, cfg: Configuration, rev: Option[String]): Future[DBAccess] = {
    val docid = "_design/"+cfg.getString("name").get
    log.info(s"Updating design $docid")
    val obj: mutable.Buffer[(String, JsValue)] = mutable.Buffer("_id" -> JsString(docid))
    rev.foreach { r =>
      obj += ("_rev" -> JsString(r))
    }
    obj += ("language" -> JsString(cfg.getString("language").getOrElse("javascript")))

    val views = for {
      view  <- cfg.getConfigList("views").get
      vname <- view.getString("name").get
    } yield {
      val map = view.getString("map").get
      val reduce = view.getString("reduce")
      if(reduce.isDefined) {
        (vname, Json.obj(
          "map" -> resolveResource(map),
          "reduce" -> resolveResource(reduce.get)
        ))
      } else {
        (vname, Json.obj(
          "map" -> resolveResource(map)
        ))
      }
    }

    obj += ("views" -> JsObject(views.toSeq))
    dba.doc(docid, JsObject(obj)).map(design => dba)
  }

  private def updateDesigns(fdb: Future[DBAccess]): Future[DBAccess] = fdb flatMap { db =>
    val designs = db.cfg.getConfigList("designs").getOrElse(Collections.emptyList[Configuration]())
    designs.foldLeft(future(db)) { case (chain, cfg) =>
      chain.flatMap { cdb =>
        cdb.doc("_design/"+cfg.getString("name").get).flatMap {
          case Right(value) => if(!validateDesignDoc(value, cfg)) { updateDesign(cdb, cfg, Some((value \ "_rev").as[String])) } else { future(cdb) }
          case _ => updateDesign(cdb, cfg, None)
        }
      }
    }
  }

  override def onStart() {
    log.info("Starting couchdb plugin")
    for((_, dba) <- db) {
      log.info(s"Checking db ${dba.dbName} for existence")
      Await.result(updateDesigns(dba.createIfNotExists), 60.seconds)
    }
  }

  lazy val server = {
    val serverSettings = app.configuration.getConfig("couchdb.server").get
    val auth = serverSettings.getConfig("auth").map { c =>
      Authentication(c.getString("user").get, c.getString("password").get)
    }
    val serverUrl: String = serverSettings.getString("url").get
    log.info(s"Initializing server at $serverUrl")
    Server(serverUrl, auth)
  }

  lazy val db = {
    val dbs = for {
      dbcfg <- app.configuration.getConfigList("couchdb.db").get
      dbname = dbcfg.getString("name").get
    } yield {
      log.info(s"Opening database ${dbname}")
      (dbname, DBAccess(dbname, dbcfg, server))
    }
    dbs.toMap
  }
}

object CouchDBPlugin {
  def db(name: String) = {
    Play.maybeApplication.get.plugin(classOf[CouchDBPlugin]).get.db.get(name).get
  }

  private def urienc(str: String): String = {
    URLEncoder.encode(str, "UTF-8")
  }

  case class Authentication(user: String, password: String)

  case class ServerError(message: String, method: String, path: String, response: Response, cause: Option[Throwable] = None) extends Throwable(s"$message: $method => $path = ${response.status}: ${response.statusText}")

  case class Server(url: String, auth: Option[Authentication]) {
    private lazy val pUrl = new URL(url)

    /**
     * Encodes query string parameters
     * @param params List of the parameters
     * @return encoded parameters starting with "?" or empty string if no parameters were provided
     */
    private def encodeParams(params: Seq[(String, String)]): String = {

      val (_, encodedParams) = params.foldLeft(("?", "")) { case ((sep, queryStr), elm) =>
        ("&", queryStr+sep+urienc(elm._1)+"="+urienc(elm._2))
      }
      encodedParams
    }

    def request(path: String, params: (String, String)*) = {
      val baseUrl: URL = new URL(pUrl.getProtocol, pUrl.getHost, pUrl.getPort, s"${pUrl.getPath}/$path${encodeParams(params)}")
      val baseWs = WS.url(baseUrl.toString).withHeaders(("Accept", "application/json"))
      auth match {
        case None    => baseWs
        case Some(a) => baseWs.withAuth(a.user, a.password, AuthScheme.BASIC)
      }
    }
  }

  case class DBAccess(dbName: String, cfg: Configuration, conn: Server) {
    val log = Logger(classOf[DBAccess])

    def create: Future[Either[ServerError, JsValue]] = {
      conn.request(dbName).put("").map { r =>
        if(r.status != 201) {
          Left(ServerError("Error creating database", "PUT", dbName, r))
        } else {
          Right(r.json)
        }
      }
    }

    def exists: Future[Boolean] = {
      conn.request(dbName).head().map { r =>
        r.status == 200
      }
    }

    def createIfNotExists: Future[DBAccess] = {
      exists.flatMap { dbExists =>
        if(!dbExists) {
          log.info(s"Creating non-existing database ${dbName}")
          create.map((_) => this)
        } else {
          future(this)
        }
      }
    }

    def delete(id: String, rev: String): Future[Either[ServerError, JsValue]] = {
      val path = docPath(id)
      log.debug(s"deleting doc $path")
      conn.request(path, ("rev" -> rev)).delete() map {
        r =>
          if (r.status > 299){
            Left(ServerError("Error removing document ", "DELETE", path, r))
          } else {
            Right(r.json)
          }
      }
    }

    def forceDelete(id: String): Future[Either[ServerError, JsValue]] = {
      doc(id).flatMap { _ match {
        case l @ Left(error) => Future(l)
        case Right(jsVal) =>
          val rev = (jsVal \ "_rev").toString
          delete(id, rev)
       }
      }
    }

    private def docPath(id: String) = s"$dbName/$id"

    def doc(id: String): Future[Either[ServerError, JsValue]] = {
      val path = docPath(id)
      conn.request(path).get().map { r =>
        if(r.status != 200) {
          Left(ServerError("Document not found", "GET", path, r))
        } else {
          Right(r.json)
        }
      }
    }


    def doc(id: String, content: JsValue): Future[Either[ServerError, JsValue]] = {
      val path = docPath(id)
      log.debug(s"Storing doc $path with content: ${content}")
      conn.request(path).put(content).map { r =>
        if(r.status > 299) {
          Left(ServerError("Error saving document ", "PUT", path, r))
        } else {
          Right(r.json)
        }
      }
    }

    def forceDoc(id: String, content: JsValue): Future[Either[ServerError, JsValue]] = {
      doc(id).flatMap { res =>
        res.fold(
          error => doc(id, content),
          jsdoc => {
            val rev = jsdoc \ "_rev"
            val tr =
              (__ \ "_rev").json.prune andThen
                __.json.update((__ \ "_rev").json.put(rev))
            doc(id, content.transform(tr).get)
          }
        )
      }
    }

    def view(design: String, view: String, key: Option[JsValue] = None,
             startKey: Option[JsValue] = None, endKey: Option[JsValue] = None,
             includeDocs: Boolean = false, group: Boolean = false, reduce: Boolean = true,
             params: List[(String, String)] = Nil): Future[Either[ServerError, JsValue]] = {
      val path = docPath(s"_design/$design") + s"/_view/$view"
      val genericParams = List("group" -> group.toString, "reduce" -> reduce.toString, "include_docs" -> includeDocs.toString)
      val keyParams = List("key" -> key, "startKey" -> startKey, "endKey" -> endKey). flatMap { case(key,maybeValue) =>
        maybeValue.map(v=>(key, Json.stringify(v)))
      }
      conn.request(path, (keyParams ++ genericParams ++ params):_*).get().map { r =>
        if(r.status != 200) {
          Left(ServerError("Error querying view", "GET", path, r))
        } else {
          Right(r.json)
        }
      }
    }
  }
}
