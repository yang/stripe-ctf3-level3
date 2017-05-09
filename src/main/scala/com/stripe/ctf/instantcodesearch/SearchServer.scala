package com.stripe.ctf.instantcodesearch

import com.twitter.util.{Future, Promise, FuturePool}
import com.twitter.concurrent.Broker
import org.jboss.netty.handler.codec.http.{HttpResponse, HttpResponseStatus}

class SearchServer(port : Int, id : Int) extends AbstractSearchServer(port, id) {
  val IndexPath = "instantcodesearch-" + id + ".index"
  case class Query(q : String, broker : Broker[SearchResult])
  lazy val searcher = new Searcher(IndexPath, theIndex)
  @volatile var theIndex: Index = null
  @volatile var indexed = false

  override def healthcheck() = {
    Future.value(successResponse())
  }

  override def isIndexed() = {
    if (indexed) {
      Future.value(successResponse())
    }
    else {
      Future.value(errorResponse(HttpResponseStatus.OK, "Not indexed"))
    }
  }
  override def index(path: String) = {
    val indexer = new Indexer(path, id)

    FuturePool.unboundedPool {
      try {
        System.err.println("[node #" + id + "] Indexing path: " + path)
        indexer.index()
        System.err.println("[node #" + id + "] Writing index to: " + IndexPath)
        theIndex = indexer.idx
        indexed = true
      } catch {
        case e: Error => println(e); e.printStackTrace()
      } finally {
        println(indexer.count)
      }
      // indexer.write(IndexPath)
    }

    Future.value(successResponse())
  }

  override def query(q: String) = {
    System.err.println("[node #" + id + "] Searching for: " + q)
    handleSearch(q)
  }

  def handleSearch(q: String) = {
    val searches = new Broker[Query]()
    searches.recv foreach { q =>
      FuturePool.unboundedPool {searcher.search(q.q, q.broker)}
    }

    val matches = new Broker[SearchResult]()
    val err = new Broker[Throwable]
    searches ! new Query(q, matches)

    val promise = Promise[HttpResponse]
    var results = List[Match]()

    matches.recv foreach { m =>
      m match {
        case Matches(matches) => promise.setValue(querySuccessResponse(matches))
//        case m : Match => results = m :: results
//        case Done() => promise.setValue(querySuccessResponse(results))
      }
    }

    promise
  }
}
