
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

object Main {
  val target = "b"

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //Linux or Mac:nc -l 9999
    //Windows:nc -l -p 9999
    val text: DataStream[String] = env.socketTextStream("localhost", 9999)
    //    val stream = text.timeWindowAll(Time.minutes(1))
    val stream: DataStream[(String, Int)] = text.flatMap(x => x.toLowerCase.split("").filter(x => x.contains(target)))
      .map(x => (x, 1))
      .keyBy(0)
      .timeWindowAll(Time.minutes(1), Time.seconds(1))
      .sum(1)
    stream.print()
    env.execute("Window Stream WordCount")
  }
}

