package marrony

import java.nio.ByteBuffer

object Hex {
  private[this] val HexBytes = Array[Char]('0', '1', '2', '3', '4', '5', '6', '7',
    '8', '9', 'A', 'B', 'C', 'D', 'E', 'F')

  private[this] val Empty = "[]"

  def fromHexString(str: String): ByteBuffer = {
    if (str == Empty) {
      return ByteBuffer.allocate(0)
    }

    val offset = if (str.startsWith("0x")) 2 else 0
    val length = (str.length - offset) / 2

    val buf = ByteBuffer.allocate(length)

    (0 until length) foreach { i =>
      val c0 = str.charAt(i * 2 + 0 + offset).toUpper
      val c1 = str.charAt(i * 2 + 1 + offset).toUpper

      val b0 = if (c0 >= 'A') c0 - '7' else c0 - '0'
      val b1 = if (c1 >= 'A') c1 - '7' else c1 - '0'

      val b = (b0 << 4 | b1) & 0xff
      buf.put(length - i - 1, b.toByte)
    }

    buf
  }

  def toHexString(buf: ByteBuffer): String = {
    try {
      buf.mark()
      val len = buf.remaining()

      val builder = new StringBuilder

      if (len > 0) {
        builder.append("0x")

        (0 until len) foreach { i =>
          val byte = buf.get(len - i - 1)
          val b0 = (byte & 0xf0) >> 4
          val b1 = byte & 0x0f

          builder.append(HexBytes(b0))
          builder.append(HexBytes(b1))
        }
      } else {
        builder.append("[]")
      }

      builder.toString
    } finally {
      buf.reset()
    }
  }
}
