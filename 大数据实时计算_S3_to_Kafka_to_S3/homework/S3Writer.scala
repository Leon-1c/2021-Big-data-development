import java.io.{BufferedReader, File, FileReader, FileWriter}
import java.util.{Timer, TimerTask,LinkedList,HashMap}

import scala.util.parsing.json.JSON
import com.bingocloud.auth.BasicAWSCredentials
import com.bingocloud.{ClientConfiguration, Protocol}
import com.bingocloud.services.s3.AmazonS3Client
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.Configuration


class S3Writer(accessKey: String, secretKey: String, endpoint: String, bucket: String, keyPrefix: String, period: Int, filename: String) extends OutputFormat[String] {
  var timer: Timer = _
  var file: File = _
  var fileWriter: FileWriter = _
  var length = 0L
  var amazonS3: AmazonS3Client = _
  var content = new StringBuilder("")

  /**
   * 将内容按照 dest 分类，返回一个有序的内容
   * @param lines
   * @return
   */
  def group(lines: String):String = {
    val content = new StringBuilder("")
    val fullContent = new LinkedList[Tuple2[String, String]]
    lines.split("\n")
      .foreach(line => {
        val result = JSON.parseFull(line)
        var new_line: Tuple2[String, String] = null
        result.foreach(m => {
          m match {
            case map: Map[String, Any] => {
              map.get("origin") match {
                case Some(x) => new_line = (line, x.toString)
                case _ => new_line = (line, "")
              }
            }
            case _ => None
          }
          fullContent.add(new_line)
        })
      })

    val map: HashMap[String, LinkedList[String]] = new HashMap[String, LinkedList[String]]()
    val iter = fullContent.iterator()
    while (iter.hasNext) {
      val next = iter.next()
      if (!map.containsKey(next._2)) {
        val list = new LinkedList[String]()
        list.add(next._1)
        map.put(next._2, list)
      }
      else {
        map.get(next._2).add(next._1)
      }
    }
    val keys = map.keySet()
    val keyIter = keys.iterator()
    while (keyIter.hasNext) {
      val dest = keyIter.next()
      val list = map.get(dest)
      for (i <- 0 to list.size() - 1) {
        content.append(list.get(i)+"\n")
      }
    }
    content.toString()
  }


  def upload: Unit = {
    this.synchronized {
      if (length > 0) {
        // 重新排序
        fileWriter.write(group(content.toString))
        fileWriter.flush()
        val targetKey = "sorted_data"
        println("开始上传文件：%s 至 %s 桶的 %s 目录下".format(file.getAbsoluteFile, bucket, targetKey))
        amazonS3.putObject(bucket, targetKey, file)

        length = 0L
        content = new StringBuilder("")
      }
    }
  }

  override def configure(configuration: Configuration): Unit = {
    timer = new Timer("S3Writer")
    timer.schedule(new TimerTask() {
      def run(): Unit = {
        upload
      }
    }, 1000, period)
    val credentials = new BasicAWSCredentials(accessKey, secretKey)
    val clientConfig = new ClientConfiguration()
    clientConfig.setProtocol(Protocol.HTTP)
    amazonS3 = new AmazonS3Client(credentials, clientConfig)
    amazonS3.setEndpoint(endpoint)
    file = new File(filename)
    if (file.exists) {
      val bufferReader = new BufferedReader(new FileReader(file))
      var contentStr: String = null
      while ((contentStr = bufferReader.readLine()) != None && contentStr != null) {
        content.append(contentStr)
      }
      bufferReader.close()
    }

    fileWriter = new FileWriter(file)
  }

  override def open(taskNumber: Int, numTasks: Int): Unit = {
  }

  override def writeRecord(it: String): Unit = {
    this.synchronized {
      if (StringUtils.isNoneBlank(it)) {
        content.append(it + "\n")
        length += it.length

      }
    }
  }


  override def close(): Unit = {
    fileWriter.flush()
    fileWriter.close()
    timer.cancel()
  }
}