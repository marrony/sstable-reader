package marrony

import java.nio.ByteBuffer

object Implicits {
  implicit class RichString(val str: String) extends AnyVal {
    def toByteBuffer: ByteBuffer = Hex.fromHexString(str)
  }

  implicit class RichByteBuf(val buf: ByteBuffer) extends AnyVal {
    def toHexString: String = Hex.toHexString(buf)

    def read16BitString(): ByteBuffer = {
      val keyLength = buf.getShort & 0xffff
      getSlice(keyLength)
    }

    def getSlice(length: Int): ByteBuffer = {
      val slice = ByteBuffer.allocate(length)

      val limit = buf.limit()
      try {
        buf.limit(buf.position() + length)
        slice.put(buf)
      } finally {
        buf.limit(limit)
      }

      slice.position(0)

//      val slice = buf.slice()
//      slice.limit(length)
//      buf.position(buf.position() + length)
//      slice
    }

    def readDeletionTime(): DeletionTime = {
      val localDeletionTime = buf.getInt
      val markedForDeletedAt = buf.getLong

      DeletionTime(localDeletionTime, markedForDeletedAt)
    }
  }
}
