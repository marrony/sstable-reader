package marrony

import java.io.File
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.nio.ByteBuffer
import Implicits._

object AtomIterator {
  val DELETION_MASK = 0x01
  val EXPIRATION_MASK = 0x02
  val COUNTER_MASK = 0x04
  val COUNTER_UPDATE_MASK = 0x08
  val RANGE_TOMBSTONE_MASK = 0x10
}

class AtomIterator(input: Input) extends Iterator[Atom] {
  import AtomIterator._

  private[this] var cellName = ByteBuffer.allocate(0)

  def hasNext: Boolean = {
    cellName = input.read16BitString()

    cellName.remaining() != 0
  }

  def next(): Atom = {
    val mask = input.readByte() & 0xff

    if ((mask & RANGE_TOMBSTONE_MASK) != 0) {
      val lastColumnKey = input.read16BitString()
      val deletionTime = input.readDeletionTime()

      Tombstone(cellName, lastColumnKey, deletionTime)
    } else {
      if ((mask & COUNTER_MASK) != 0) {
        val timestampOfLastDelete = input.readLong()
        val timestamp = input.readLong()
        val value = input.read32BitString()

        CounterCell(cellName, value, timestampOfLastDelete, timestamp)
      } else if ((mask & EXPIRATION_MASK) != 0) {
        val ttl = input.readInt()
        val expiration = input.readInt()
        val timestamp = input.readLong()
        val value = input.read32BitString()

        ExpiringCell(cellName, value, ttl, expiration, timestamp)
      } else {
        val timestamp = input.readLong()
        val value = input.read32BitString()

        if ((mask & COUNTER_UPDATE_MASK) != 0) {
          CounterUpdateCell(cellName, value, timestamp)
        } else if ((mask & DELETION_MASK) != 0) {
          DeletionCell(cellName, value, timestamp)
        } else {
          Cell(cellName, value, timestamp)
        }
      }
    }
  }
}

case class Row(input: Input, rowKey: ByteBuffer, deletionTime: DeletionTime) {
  override def toString: String = s"Row(${rowKey.toHexString}, $deletionTime)"

  def atomIterator(): Iterator[Atom] = new AtomIterator(input)
}

class SSTable(val input: Input, val index: Index) {

  def rowIterator(): Iterator[Row] = new Iterator[Row] {
    val keyIter = index.keys.iterator

    def hasNext: Boolean = keyIter.hasNext

    def next(): Row = {
      val key = keyIter.next()
      val entry = index.entries(key)
      input.readerIndex(entry.position.toInt)
      readRow()
    }
  }

  def getRow(rowKey: ByteBuffer): Option[Row] = {
    index.entries.get(rowKey) map { entry =>
      input.readerIndex(entry.position.toInt)

      readRow()
    }
  }

  private def readRow(): Row = {
    val rowKey = input.read16BitString()

    if (rowKey.remaining() == 0) {
      throw new IllegalStateException("Invalid row")
    }

    val deletionTime = input.readDeletionTime()

    Row(input, rowKey, deletionTime)
  }

  def close(): Unit = {
    input.channel.close()
  }
}

object SSTable {
  def open(file: String): SSTable = {
    val indexFile = new File(s"$file-Index.db")
    val dataFile = new File(s"$file-Data.db")
    val compressFile = new File(s"$file-CompressionInfo.db")

    val compressionInfo = CompressionInfo.read(compressFile)

    val index = Index.read(indexFile)

    val channel = FileChannel.open(dataFile.toPath, StandardOpenOption.READ)
    val mapped = channel.map(FileChannel.MapMode.READ_ONLY, 0, dataFile.length())

    val input = new Input(channel, mapped, compressionInfo)

    new SSTable(input, index)
  }
}
