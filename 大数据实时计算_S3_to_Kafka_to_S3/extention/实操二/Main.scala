import java.util.{Properties, UUID}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Main {
  /**
   * 输入的主题名称
   */
//  val inputTopic = "mn_buy_ticket_1"
  val inputTopic = "lcx_1"
  /**
   * kafka地址
   */
  val bootstrapServers = "bigdata35.depts.bingosoft.net:29035,bigdata36.depts.bingosoft.net:29036,bigdata37.depts.bingosoft.net:29037"

  def main(args: Array[String]): Unit = {
    def regJson(json:Option[Any]) = json match {
      case Some(map: Map[String, Any]) => map
    }
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val kafkaProperties = new Properties()
    kafkaProperties.put("bootstrap.servers", bootstrapServers)
    kafkaProperties.put("group.id", UUID.randomUUID().toString)
    kafkaProperties.put("auto.offset.reset", "earliest")
    kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val kafkaConsumer = new FlinkKafkaConsumer010[String](inputTopic,
      new SimpleStringSchema, kafkaProperties)
    kafkaConsumer.setCommitOffsetsOnCheckpoints(true)
    val inputKafkaStream = env.addSource(kafkaConsumer)
//    println(inputKafkaStream)
    inputKafkaStream.map(x => x.substring(x.indexOf("destination")+14,x.indexOf("username")-3))
      .map(x => (x, 1))
      .keyBy(0)
      .window(SlidingProcessingTimeWindows.of(Time.seconds(60L), Time.seconds(10L)))
      .sum(1)
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10L)))
      .process(new ProcessAllWindowFunction[(String, Int), String, TimeWindow] {
        override def process(context: Context, elements: Iterable[(String, Int)], out: Collector[String]): Unit = {
          val top3 = elements.toSeq
            .sortBy(-_._2)
            .take(5)
            .zipWithIndex
            .map { case ((item, price), idx) => s"   ${idx + 1}. $item: $price" }
            .mkString("\n")
          out.collect(("-" * 16) + "\n" + top3)
        }
      }
      )
      .print()
    //      .keyBy(0)
//    inputKafkaStream.map(x=>regJson(JSON.parseFull(x)).get("destination")).print()
    env.execute()
  }
}
