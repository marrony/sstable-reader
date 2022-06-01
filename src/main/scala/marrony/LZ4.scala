package marrony

import java.nio.{ ByteBuffer, ByteOrder }
import java.util.zip.Adler32
import net.jpountz.lz4.LZ4Factory

object LZ4 {

  private[this] val lz4Factory = LZ4Factory.fastestInstance
  private[this] val decompressor = lz4Factory.fastDecompressor()

  def decompress(
    inputBuffer: ByteBuffer,
    chunkPosition: Int,
    outputBuffer: ByteBuffer,
    dstOffset: Int): Int = {
    inputBuffer.position(chunkPosition)

    inputBuffer.order(ByteOrder.LITTLE_ENDIAN)
    val outputSize = inputBuffer.getInt
    inputBuffer.order(ByteOrder.BIG_ENDIAN)

    require(outputSize + dstOffset <= outputBuffer.remaining())

    val compressedLength = decompressor.decompress(
      inputBuffer,
      chunkPosition + 4, // skip 4 bytes of size
      outputBuffer,
      dstOffset,
      outputSize)

    val limit = inputBuffer.limit()

    // todo: check probability
    inputBuffer.position(chunkPosition)
    inputBuffer.limit(chunkPosition + compressedLength + 4)

    val adler32 = new Adler32
    adler32.update(inputBuffer)

    inputBuffer.position(chunkPosition + compressedLength + 4)
    inputBuffer.limit(limit)
    val checksum = inputBuffer.getInt().toLong & 0xffffffffL

    require(adler32.getValue == checksum, "Invalid Adler-32 checksum")

    compressedLength
  }
}
