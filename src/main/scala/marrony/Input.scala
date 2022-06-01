package marrony

import java.nio.channels.FileChannel
import java.nio.ByteBuffer
import Implicits._

object Input {
  val MemoryChunks = 1024
}

class Input(
  val channel: FileChannel,
  val inputBuffer: ByteBuffer,
  val compressionInfo: CompressionInfo) {
  import Input._

  private[this] var position = -1
  private[this] var lastChunk = 0
  private[this] var current = emptyBuffer()

  private def checkBuffer(length: Int): Unit = {
    if (position < 0) {
      current = readChunks(emptyBuffer(), 0, MemoryChunks)
      lastChunk = MemoryChunks - 1
      position = 0
    }

    if (length == 0) return

    val bytesLeft = current.remaining()

    val chunksCount = (length / compressionInfo.chunkLength) max MemoryChunks

    if (length >= bytesLeft) {
      val outputBuffer = readChunks(current, lastChunk + 1, chunksCount)
      lastChunk = lastChunk + chunksCount
      current = outputBuffer
    }

    position += length
  }

  private def readChunks(prevBuf: ByteBuffer, start: Int, count: Int): ByteBuffer = {
    val bytesLeft = prevBuf.remaining()
    val outputSize = compressionInfo.chunkLength * count + bytesLeft

    val outputBuffer = ByteBuffer.allocate(outputSize)
    outputBuffer.put(prevBuf)

    var offset = bytesLeft
    val last = (start + count) min compressionInfo.chunks.size

    (start until last) foreach { idx =>
      val chunkPosition = compressionInfo.chunks(idx)
      LZ4.decompress(inputBuffer, chunkPosition.toInt, outputBuffer, offset)
      offset += compressionInfo.chunkLength
    }

    outputBuffer.position(0)
    outputBuffer
  }

  def read16BitString(): ByteBuffer = {
    checkBuffer(2)
    val keyLength = current.getShort & 0xffff
    checkBuffer(keyLength)
    current.getSlice(keyLength)
  }

  def read32BitString(): ByteBuffer = {
    val dataLength = readInt()
    checkBuffer(dataLength)
    current.getSlice(dataLength)
  }

  def readDeletionTime(): DeletionTime = {
    checkBuffer(8 + 4)
    current.readDeletionTime()
  }

  def readByte(): Byte = {
    checkBuffer(1)
    current.get
  }

  def readInt(): Int = {
    checkBuffer(4)
    current.getInt
  }

  def readLong(): Long = {
    checkBuffer(8)
    current.getLong
  }

  def readableBytes(): Long = {
    checkBuffer(0)
    current.remaining()
  }

  def readerIndex(): Int = {
    checkBuffer(0)
    position
  }

  def readerIndex(p: Int): Unit = {
    val chunk = p / compressionInfo.chunkLength
    val outputBuffer = readChunks(emptyBuffer(), chunk, MemoryChunks)
    outputBuffer.position(p % compressionInfo.chunkLength)
    current = outputBuffer
    lastChunk = chunk + MemoryChunks - 1
    position = p
  }

  private def emptyBuffer() = ByteBuffer.allocate(0)
}
