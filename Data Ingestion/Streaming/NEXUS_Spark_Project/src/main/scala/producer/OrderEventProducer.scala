package producer

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source
import scala.util.matching.Regex

object OrderEventProducer {

  private val OrderIdPattern: Regex =
    "\"order_id\"\\s*:\\s*(null|\"[^\"]+\")".r

  private val EventIdPattern: Regex =
    "\"event_id\"\\s*:\\s*\"([^\"]+)\"".r

  def main(args: Array[String]): Unit = {

    val inputDirectory =
      sys.env.getOrElse("INPUT_DIRECTORY", "/data/incoming")

    val stateDirectory =
      sys.env.getOrElse("STATE_DIRECTORY", "/data/state")

    val bootstrapServers =
      sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

    val topic =
      sys.env.getOrElse("KAFKA_TOPIC", "nexus-order-events")

    val pollIntervalMs =
      sys.env.getOrElse("POLL_INTERVAL_MS", "5000").toLong

    val inputPath = Paths.get(inputDirectory)
    val statePath = Paths.get(stateDirectory)
    val processedFile = statePath.resolve("processed-files.txt")

    Files.createDirectories(statePath)

    if (!Files.exists(inputPath)) {
      throw new IllegalStateException(
        s"Input directory does not exist: $inputDirectory"
      )
    }

    val processedFiles = mutable.Set[String]()

    if (Files.exists(processedFile)) {
      Files.readAllLines(processedFile, StandardCharsets.UTF_8)
        .asScala
        .filter(_.trim.nonEmpty)
        .foreach(processedFiles.add)
    }

    val properties = new Properties()
    properties.put("bootstrap.servers", bootstrapServers)
    properties.put("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("acks", "all")
    properties.put("enable.idempotence", "true")

    val producer = new KafkaProducer[String, String](properties)

    println("NEXUS Order Event Producer started")
    println(s"Kafka broker : $bootstrapServers")
    println(s"Kafka topic  : $topic")
    println(s"Input folder : $inputDirectory")
    println(s"Poll interval: $pollIntervalMs ms")

    try {
      while (true) {

        try {
          val stream = Files.list(inputPath)

          val files = try {
            stream.iterator().asScala
              .filter(path =>
                Files.isRegularFile(path) &&
                path.getFileName.toString.matches(
                  "order_events_[0-9]{3}\\.json"
                )
              )
              .toSeq
              .sortBy(_.getFileName.toString)
          } finally {
            stream.close()
          }

          files.foreach { file =>

            val fileName = file.getFileName.toString

            if (!processedFiles.contains(fileName)) {

              println(s"\nProcessing new batch: $fileName")

              val source = Source.fromFile(file.toFile, "UTF-8")
              var count = 0

              try {
                source.getLines()
                  .filter(_.trim.nonEmpty)
                  .foreach { json =>

                    val orderId = OrderIdPattern.findFirstMatchIn(json)
                      .map(_.group(1))
                      .filter(_ != "null")
                      .map(_.replaceAll("^\"|\"$", ""))

                    val eventId = EventIdPattern.findFirstMatchIn(json)
                      .map(_.group(1))

                    val key = orderId.orElse(eventId).getOrElse(
                      throw new IllegalArgumentException(
                        s"Missing order_id and event_id: $json"
                      )
                    )

                    producer.send(
                      new ProducerRecord[String, String](
                        topic, key, json
                      )
                    ).get()

                    count += 1
                  }
              } finally {
                source.close()
              }

              producer.flush()

              Files.write(
                processedFile,
                (fileName + System.lineSeparator())
                  .getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
              )

              processedFiles.add(fileName)

              println(s"Batch published successfully: $fileName")
              println(s"Events published: $count")
            }
          }

        } catch {
          case ex: Exception =>
            System.err.println(
              s"Polling/processing error: ${ex.getMessage}"
            )
            ex.printStackTrace()
        }

        Thread.sleep(pollIntervalMs)
      }

    } finally {
      producer.close()
    }
  }
}