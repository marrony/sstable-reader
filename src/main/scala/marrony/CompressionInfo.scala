package marrony

import java.io.{
  DataInputStream,
  DataOutputStream,
  File,
  FileInputStream,
  FileOutputStream
}

case class CompressionInfo(
  compressor: String,
  options: Map[String, String],
  chunkLength: Int,
  dataLength: Long,
  chunks: Seq[Long])

object CompressionInfo {

  def read(compressInfo: File): CompressionInfo = {
    val input = new DataInputStream(new FileInputStream(compressInfo))

    val compressor = input.readUTF()

    val optsCount = input.readInt()
    val options = (1 to optsCount) map { _ =>
      val name = input.readUTF()
      val value = input.readUTF()
      name -> value
    }

    val chunkLength = input.readInt()
    val dataLength = input.readLong()
    val chunkCount = input.readInt()

    val chunks = (1 to chunkCount) map { _ =>
      val chunk = input.readLong()
      chunk
    }

    CompressionInfo(compressor, options toMap, chunkLength, dataLength, chunks)
  }

  def write(compressionInfo: CompressionInfo, file: File): Unit = {
    val output = new DataOutputStream(new FileOutputStream(file))

    output.writeUTF(compressionInfo.compressor)

    output.writeInt(compressionInfo.options.size)
    compressionInfo.options foreach { case (k, v) =>
      output.writeUTF(k)
      output.writeUTF(v)
    }

    output.writeInt(compressionInfo.chunkLength)
    output.writeLong(compressionInfo.dataLength)

    output.writeInt(compressionInfo.chunks.size)
    compressionInfo.chunks foreach { chunk =>
      output.writeLong(chunk)
    }

    output.close()
  }
}
