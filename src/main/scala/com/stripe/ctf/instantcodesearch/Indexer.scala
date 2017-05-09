package com.stripe.ctf.instantcodesearch

import java.io._
import java.util.Arrays
import java.nio.file._
import java.nio.charset._
import java.nio.file.attribute.BasicFileAttributes
import scala.collection.mutable

class Indexer(indexPath: String, id: Int) {
  val root = FileSystems.getDefault().getPath(indexPath)
  val idx = new Index(root.toAbsolutePath.toString)
  var count = 0

  def index() : Indexer = {
    val paths = mutable.ArrayBuffer[Path]()
    Files.walkFileTree(root, new SimpleFileVisitor[Path] {
      override def preVisitDirectory(dir : Path, attrs : BasicFileAttributes) : FileVisitResult = {
        if (Files.isHidden(dir) && dir.toString != ".")
          return FileVisitResult.SKIP_SUBTREE
        return FileVisitResult.CONTINUE
      }
      override def visitFile(file : Path, attrs : BasicFileAttributes) : FileVisitResult = {
        if (Files.isHidden(file))
          return FileVisitResult.CONTINUE
        if (!Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS))
          return FileVisitResult.CONTINUE
        if (Files.size(file) > (1 << 20))
          return FileVisitResult.CONTINUE
        val bytes = Files.readAllBytes(file)
        if (Arrays.asList(bytes).indexOf(0) > 0)
          return FileVisitResult.CONTINUE
        paths += file
        return FileVisitResult.CONTINUE
      }
    })

    val actual = paths.slice(((id - 1) / 3.0 * paths.length).round.toInt, ((id) / 3.0 * paths.length).round.toInt)

    for (file <- actual) {
      val bytes = Files.readAllBytes(file)
      val decoder = Charset.forName("UTF-8").newDecoder()
      decoder onMalformedInput CodingErrorAction.REPORT
      decoder onUnmappableCharacter CodingErrorAction.REPORT
      count += 1
      try {
        val r = new InputStreamReader(new ByteArrayInputStream(bytes), decoder)
        val strContents = slurp(r)
        idx.addFile(root.relativize(file).toString, strContents)
      } catch {
        case e: IOException => println(e)
      }
    }

    return this
  }

  def write(path: String) = {
    idx.write(new File(path))
  }
}
