package marrony

import java.io.File
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.nio.ByteBuffer
import Implicits._

sealed trait IndexEntry {
  val rowKey: ByteBuffer
  val position: Long
}

case class RowIndexEntry(rowKey: ByteBuffer, position: Long) extends IndexEntry {
  override def toString: String =
    s"RowIndexEntry(${rowKey.toHexString}, $position)"
}

case class ColumnEntry(
  first: ByteBuffer,
  last: ByteBuffer,
  offset: Long,
  width: Long) {
  override def toString: String =
    s"ColumnEntry(${first.toHexString}, ${last.toHexString}, $offset, $width)"
}

case class ColumnIndexEntry(
  rowKey: ByteBuffer,
  position: Long,
  deletionTime: DeletionTime,
  columns: Seq[ColumnEntry])
    extends IndexEntry {
  override def toString: String =
    s"ColumnIndexEntry(${rowKey.toHexString}, $position, $deletionTime, $columns)"
}

case class Index(keys: Seq[ByteBuffer], entries: Map[ByteBuffer, IndexEntry])

object Index {

  def read(index: File): Index = {
    val c = FileChannel.open(index.toPath, StandardOpenOption.READ)
    val input = c.map(FileChannel.MapMode.READ_ONLY, 0, index.length())

    val b = Map.newBuilder[ByteBuffer, IndexEntry]
    val k = Seq.newBuilder[ByteBuffer]

    while (input.remaining() > 0) {
      val key = input.read16BitString()
      val position = input.getLong

      val size = input.getInt
      val entry = if (size > 0) {
        val deletionTime = input.readDeletionTime()

        val entriesCount = input.getInt
        val columns = (1 to entriesCount) map { _ =>
          val first = input.read16BitString()
          val last = input.read16BitString()
          val offset = input.getLong
          val width = input.getLong

          ColumnEntry(first, last, offset, width)
        }

        ColumnIndexEntry(key, position, deletionTime, columns)
      } else {
        RowIndexEntry(key, position)
      }

      k += key
      b += key -> entry
    }

    c.close()

    Index(k.result(), b.result())
  }
}
