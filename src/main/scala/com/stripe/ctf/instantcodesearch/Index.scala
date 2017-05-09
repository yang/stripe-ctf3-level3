package com.stripe.ctf.instantcodesearch

import java.io._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class Posting(docid: Int, line: Int)
class Index(repoPath: String) extends Serializable {
  var files = List[String]()
  val paths = new mutable.ArrayBuffer[String]()
  type Gram = String
  val gram2postings = new mutable.HashMap[Gram, mutable.HashSet[Posting]]()
  val grampostings2offsets = new mutable.HashMap[(Gram, Posting), mutable.ArrayBuffer[Int]]
  val gram2docids = new mutable.HashMap[String, mutable.HashSet[Int]]()
  val docid2gram2lines = new mutable.ArrayBuffer[mutable.HashMap[String, mutable.ArrayBuffer[Int]]]()
  val contents = new mutable.ArrayBuffer[String]
  val docid2lineOffsets = new mutable.ArrayBuffer[mutable.ArrayBuffer[Int]]

  def path() = repoPath

  final def grams(text: String) =
    for (i <- 0 until (text.length - 2) toIterator) yield text.substring(i, i + 3)

  def addFile(file: String, text: String) {
    files = file :: files
    val docid = paths.length
    contents += text
    paths += file
    val lineOffsets = new mutable.ArrayBuffer[Int]
    docid2lineOffsets += lineOffsets

    /*
    val gram2lines = new mutable.HashMap[String, mutable.ArrayBuffer[Int]]()
    docid2gram2lines += gram2lines
    */

    for ((line, lineno) <- text.split('\n').zipWithIndex) {
      lineOffsets += 1 + line.length + (if (lineOffsets.isEmpty) 0 else lineOffsets.last)
      for ((gram, offset) <- grams(line).zipWithIndex) {
        val posting = Posting(docid, lineno)
        gram2postings.getOrElseUpdate(gram, new mutable.HashSet[Posting]()) += posting
        grampostings2offsets.getOrElseUpdate((gram, posting), new ArrayBuffer[Int]()) += offset
      }
    }
    /*
    for (gram <- grams(text)) {
      if (!gram2docids.contains(gram)) {
        gram2docids(gram) = new mutable.HashSet[Int]()
      }
      gram2docids(gram) += docid
    }
    */
  }

  def write(out: File) {
    val stream = new FileOutputStream(out)
    write(stream)
    stream.close
  }

  def write(out: OutputStream) {
    println("writing")
    val w = new ObjectOutputStream(out)
    w.writeObject(this)
    w.close
    println("written")
  }
}

