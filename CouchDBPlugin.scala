package plugins

import play.api.{Logger, Plugin, Application}
import play.api.libs.ws.{Response, WS}
import java.net.{URLEncoder, URL}
import play.api.libs.json.JsValue
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import plugins.CouchDBPlugin.{DBAccess, Server, Authentication}
import scala.collection.JavaConversions._
import play.api.libs.concurrent.Execution.Implicits.defaultContext

/**
 * This plugin provides the couch db api and checks the couch db views and updates or creates them if necessary.
 *
 * @author Gerry Gunzenhauser
 */
class CouchDBPlugin(app: Application) extends Plugin {
  val log = Logger(classOf[CouchDBPlugin])

  override def onStart() {
    log.info("Starting couchdb plugin")
    for((_, dba) <- db) {
      log.info(s"Checking db ${dba.dbName} for existence")
      Await.result(dba.createIfNotExists, 60.seconds)
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
      (dbname, DBAccess(dbname, server))
    }
    dbs.toMap
  }
}

object CouchDBPlugin {

  case class Authentication(user: String, password: String)

  case class ServerError(message: String, method: String, path: String, response: Response) extends Throwable(s"$message: $method => $path = ${response.status}: ${response.statusText}")

  case class Server(url: String, auth: Option[Authentication]) {
    private lazy val pUrl = new URL(url)

    /**
     * Encodes query string parameters
     * @param params List of the parameters
     * @return encoded parameters starting with "?" or empty string if no parameters were provided
     */
    private def encodeParams(params: Seq[(String, String)]): String = {
      def urienc(str: String): String = {
        URLEncoder.encode(str, "UTF-8")
      }

      val (_, encodedParams) = params.foldLeft(("?", "")) { case ((sep, queryStr), elm) =>
        ("&", queryStr+sep+urienc(elm._1)+"="+urienc(elm._2))
      }
      encodedParams
    }

    def request(path: String, params: (String, String)*) = {
      WS.url(new URL(pUrl.getProtocol, pUrl.getHost, pUrl.getPort, s"${pUrl.getPath}/$path${encodeParams(params)}").toString)
        .withHeaders(("Accept", "application/json"))
    }
  }

  case class DBAccess(dbName: String, conn: Server) {
    val log = Logger(classOf[DBAccess])

    def create: Future[JsValue] = {
      conn.request(dbName).put("").map { r =>
        if(r.status != 201) {
          throw ServerError("Error creating database", "PUT", dbName, r)
        }
        r.json
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
          Future(this)
        }
      }
    }
  }
}
