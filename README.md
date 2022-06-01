# Usage

```scala
import java.nio.ByteBuffer
import marrony._
import marrony.Implicits._

object Main {

  def main(args: Array[String]): Unit = {
    val sstable = SSTable.open("Namespace-ColumnFamily-ka-102935")

    //iterate all rows
    sstable.rowIterator() foreach { row =>
      println(row)
    }

    //get a specific key
    val key = "0x06040402200931B92F044ACD81B58000001A0C0220BEC73DAC021B83".toByteBuffer
    sstable.getRow(key) match {
      case Some(row) =>
        row.atomIterator() foreach println
      case None =>
        println("Key not found")
    }
  }
}
```
