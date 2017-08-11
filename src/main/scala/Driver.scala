import java.io.IOException
import java.net.{InetSocketAddress, Socket}
import java.util
import java.util.Properties

import com.fractal.mdtsdb.client.api.Parse
import com.fractal.mdtsdb.client.api.{MdtsdbClient, Measurement, Parse}
import examples.com.fractal.mdtsdb.client.{MdtsdbCredentials, SwimlaneCases}
import com.google.gson.JsonObject
import com.google.common.base.Preconditions.checkArgument
import com.mongodb.Mongo

import scala.collection.JavaConversions._
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}



/**
  * Created by mikeyb on 7/31/17.
  */
object Driver {
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

    //****************MTSDB Build Client******************//
    //****************MTSDB Build Client End***************//
    while(true){
      val records: ConsumerRecords[String, String] = consumer.poll(1)
        for(record <- records){
          //println(record.value())
          //PFSenseParser.parseRecordToStr(record.value())
          //val recordValue: String = record.value()
          val recordMap: Option[util.Map[String, String]] = PFSenseParser.parseRecordToMap(record.value())
          //println(recordMap.toString)
        }
      }
  }
  def chickIfSLaneLives():Boolean={
      false
  }

  def selectMTSDBHost():String = {
    val mdtsdb1: String = "mdtsdb-1.fractal:8080"
    val mdtsdb2: String = "mdtsdb-2.fractal:8080"
    val mdtsdb3: String = "mdtsdb-3.fractal:8080"
    val mdtsdb4: String = "mdtsdb-4.fractal:8080"
    val mdtsdb5: String = "mdtsdb-5.fractal:8080"
    val hostList: Array[String] = Array(mdtsdb1,mdtsdb2,mdtsdb3,mdtsdb4,mdtsdb5)
    for(host <- hostList) {
      if(checkURlIsUp(host,8080))
          return host
      }
    return "Error no mdtsdb host available"
  }
  def checkURlIsUp(host:String, port:Int):Boolean={
    val protocol = "http"
    var socket:Socket = new Socket()
    val inetSocketAddress = new InetSocketAddress(host,port)
    socket.connect(inetSocketAddress)
    socket.isBound()
  }
  def getMongoConnection():Mongo = {
    try {
      val mongoConn = new Mongo("mongodb.fractal", 27017)
      return mongoConn
    }catch {
      case ioe: IOException => {println("Error....cannot connect to conmon-db"); sys.exit(-1)}
      case _ : Throwable => println("Error....cannot connect to conmon-db"); sys.exit(-1)
    }
  }



}//end of object


