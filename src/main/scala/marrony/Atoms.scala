package marrony

import java.nio.ByteBuffer
import Implicits._

case class DeletionTime(localDeletionTime: Int, markedForDeletedAt: Long) {
  override def toString: String =
    if (this == DeletionTime.Live) "DeletionTime.Live" else super.toString
}

object DeletionTime {
  val Live = DeletionTime(Int.MaxValue, Long.MinValue)
}

sealed trait Atom {
  val cellName: ByteBuffer
}

case class Tombstone(
  cellName: ByteBuffer,
  lastColumnKey: ByteBuffer,
  deletionTime: DeletionTime)
    extends Atom

case class CounterCell(
  cellName: ByteBuffer,
  value: ByteBuffer,
  timestampOfLastDelete: Long,
  timestamp: Long)
    extends Atom

case class ExpiringCell(
  cellName: ByteBuffer,
  value: ByteBuffer,
  ttl: Int,
  expiration: Int,
  timestamp: Long)
    extends Atom

case class Cell(cellName: ByteBuffer, value: ByteBuffer, timestamp: Long)
    extends Atom {
  override def toString: String =
    s"Cell(${cellName.toHexString}, ${value.toHexString}, $timestamp)"
}

case class CounterUpdateCell(
  cellName: ByteBuffer,
  value: ByteBuffer,
  timestamp: Long)
    extends Atom {
  override def toString: String =
    s"CounterUpdateCell(${cellName.toHexString}, ${value.toHexString}, $timestamp)"
}

case class DeletionCell(cellName: ByteBuffer, value: ByteBuffer, timestamp: Long)
    extends Atom {
  override def toString: String =
    s"DeletionCell(${cellName.toHexString}, ${value.toHexString}, $timestamp)"
}
