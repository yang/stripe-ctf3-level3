package com.stripe.ctf.instantcodesearch

import java.io._
import java.nio.file._

import com.twitter.concurrent.Broker
import scala.collection.mutable.ArrayBuffer

abstract class SearchResult
case class Matches(matches: Iterable[Match]) extends SearchResult
case class Match(path: String, line: Int) extends SearchResult
case class Done() extends SearchResult

class Searcher(indexPath : String, val index: Index)  {
  println("constructing Searcher")
  // val index : Index = readIndex(indexPath)
  println("readed")
  val root = FileSystems.getDefault().getPath(index.path)

  final def toMatches(postings: Iterable[Posting]) =
    for (posting <- postings) yield Match(index.paths(posting.docid), posting.line + 1)

  def search(needle : String, b : Broker[SearchResult]) = {
    // note: there may be duplicate grams; that's OK, should barely affect perf
    val sets = for (gram <- index.grams(needle)) yield index.gram2postings(gram)
    // `eq` is for efficient duplicate handling
    val postings = if (!sets.isEmpty) sets.reduce((x,y) => if (x eq y) x else x & y) else Set()

    val matches = new ArrayBuffer[Posting]
    for (posting <- postings) {
      val lineOffsets = index.docid2lineOffsets(posting.docid)
      val lineStart = if (posting.line == 0) 0 else lineOffsets(posting.line - 1)
      val lineEnd = lineOffsets(posting.line)
      if (index.contents(posting.docid).substring(lineStart, lineEnd - 1).indexOf(needle) >= 0) {
        matches += posting
      }
    }

    b !! Matches(toMatches(matches))

/*
    val offsets = for (posting <- postings) yield index.grampostings2offsets(posting)

    // starting with smallest set of offsets,
    // filter the next smallest set of offsets to successor offsets

    val paths = docids map index.paths
    b !! Matches(paths)

    for (path <- index.files) {
      for (m <- tryPath(path, needle)) {
        b !! m
      }
    }

    b !! new Done()
    */
  }

  def tryPath(path: String, needle: String) : Iterable[SearchResult] = {
    try {
      val text : String = slurp(root.resolve(path))
      if (text.contains(needle)) {
        var line = 0
        return text.split("\n").zipWithIndex.
          filter { case (l,n) => l.contains(needle) }.
          map { case (l,n) => new Match(path, n+1) }
      }
    } catch {
      case e: IOException => {
        return Nil
      }
    }

    return Nil
  }

  def readIndex(path: String) : Index = {
    println("reading")
    new ObjectInputStream(new FileInputStream(new File(path))).readObject.asInstanceOf[Index]
  }
}
