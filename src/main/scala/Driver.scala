import java.io._
import java.net.{InetSocketAddress, Socket}
import java.nio.file.Files
import java.util
import java.util.Date
import java.util.Properties

import com.fractal.mdtsdb.client.api.Parse
import com.fractal.mdtsdb.client.api.{MdtsdbClient, Measurement, Parse}
import examples.com.fractal.mdtsdb.client.{MdtsdbCredentials, SwimlaneCases}
import com.google.gson.JsonObject
import com.google.common.base.Preconditions.checkArgument
import com.mongodb.client.{FindIterable, MongoCollection, MongoDatabase}
import com.mongodb.{BasicDBObject, DB, DBCollection, DBCursor, DBObject, Mongo, MongoClient, MongoException}
import org.bson.{BSONObject, Document}

import scala.collection.JavaConversions._
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.bson.types.ObjectId
import org.ini4j.{Ini, Profile, Wini}

import scala.util.matching.Regex



/**
  * Created by mikeyb on 7/31/17.
  */
object Driver {
  val topic = "pfsense"
  //val topic = "conmon-host-logs"
  val group = "pfsenseGroup"
  val userInfo = "pfsense_nettraffic_test11"
  def main(args:Array[String]): Unit = {

    ///************* Set properties required to consume msgs from Kafka *********************//
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


    //****************Declaring Mongo Stuff******************//
    val bsonObjId = new ObjectId("598cc1ab60481f26d6b8a2f6") //this is the documentID.  We should put this into a config file
    var mongoClient: MongoClient = null
    var collection: util.Collection[Document] = null
    var mongoDatabase: MongoDatabase = null
    var network_syslog_coll: MongoCollection[Document] = null
    var iterDoc: FindIterable[Document] = null



    //***************Declaring MTSDB Stuff******************//
    //var mdtsdbAppKey:String  = null
    //var mdtsdbAdminKey:String = null
    //var mdtsdbSecretKey:String = null
    //var mdtsdbAppClient: MdtsdbClient = null

    //*************Get MTSDB stuff from File***************//
    //var pFSenseParser = new PFSenseParser()
    //var mDTSDBUtils = new MDTSDBUtils()
    val mdtsdbKeysTuple:(String, String, String) = MDTSDBUtils.getKeysFromFile()
    val mdtsdbAppKey = mdtsdbKeysTuple._1
    val mdtsdbAdminKey = mdtsdbKeysTuple._2
    val mdtsdbSecretKey = mdtsdbKeysTuple._3
    val mdtsdbAppClient: MdtsdbClient = MDTSDBUtils.createSwimLaneClient(mdtsdbAdminKey,mdtsdbAppKey, mdtsdbSecretKey)
    //****************Getting Stuff from Kafka***************//
   // val slclient = MDTSDBUtils.createSwimLane()



    while(true){
      val records: ConsumerRecords[String, String] = consumer.poll(1)
        for(record <- records){
          //println(record.value())
          //PFSenseParser.parseRecordToStr(record.value())
          //val recordValue: String = record.value()
          val recordMap: Option[util.Map[String, String]] = PFSenseParser.parseRecordToMap(record.value())
          if(recordMap != None) {
            println(recordMap.get.toString())
            MDTSDBUtils.insertIntoSwimLane(mdtsdbAppClient, recordMap.get)
          }
          else
            println("Got back a back record that did'nt conform to nettraffic regex....")
          //println(recordMap.toString)
        }
      }


  }//end of Driver.main()



}//end of object


