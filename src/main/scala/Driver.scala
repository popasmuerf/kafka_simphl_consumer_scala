import java.util
import java.util.Properties

import com.fractal.mdtsdb.client.api.Parse

import scala.collection.JavaConversions._
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}



/**
  * Created by mikeyb on 7/31/17.
  */
object Driver {

  val mdtsdb1 = "mdtsdb-1.fractal:8080"
  val mdtsdb2 = "mdtsdb-2.fractal:8080"
  val mdtsdb3 = "mdtsdb-3.fractal:8080"
  val mdtsdb4= "mdtsdb-4.fractal:8080"
  val mdtsdb5 = "mdtsdb-5.fractal:8080"
  val topic = "pfsense"
  //val topic = "conmon-host-logs"
  val group = "pfsenseGroup"
  def main(args:Array[String]): Unit ={

    var props:Properties = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    //props.put("bootstrap.servers","broker-0.kafka.mesos:9092,broker-1.kafka.mesos:9092,broker-2.kafka.mesos:9092")
    //props.put("bootstrap.servers","broker-0.kafka.mesos:9092")
    props.put("group.id", "test")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    var consumer: KafkaConsumer[String, String] = new KafkaConsumer[String,String](props)
    consumer.subscribe(util.Arrays.asList(topic))
    val mdtsdbClient = PFsenseMTSDBCLient.getClientDefault("mdtsdb-5.fractal","masterKey","masterSecret",false)
    val mdtsdbUserResp = PFsenseMTSDBCLient.createUser("mdb",mdtsdbClient)
    val userRes = new Parse(mdtsdbUserResp)
    val admKey:String = userRes.getKey
    val secretKey:String = userRes.getSecretKey
    val mdtsdbAdmClient = PFsenseMTSDBCLient.createAdminClient(admKey,secretKey,mdtsdbClient)
    val swimlaneProps = mdtsdbAdmClient.newAppkey("mdb")
    var swimlanePropsRes = new Parse(swimlaneProps)

    while(true){
      val records: ConsumerRecords[String, String] = consumer.poll(1)
        for(record <- records){
          //println(record.value())
          //PFSenseParser.parseRecordToStr(record.value())
          //val recordValue: String = record.value()
          val recordMap: Option[util.Map[String, String]] = PFSenseParser.parseRecordToMap(record.value())

        }
      }
    }
  }

