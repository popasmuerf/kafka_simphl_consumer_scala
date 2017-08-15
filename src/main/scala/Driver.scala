import java.io.IOException
import java.net.{InetSocketAddress, Socket}
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



/**
  * Created by mikeyb on 7/31/17.
  */
object Driver {
  val topic = "pfsense"
  //val topic = "conmon-host-logs"
  val group = "pfsenseGroup"
  def main(args:Array[String]): Unit = {



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
    val bsonObjId = new ObjectId("598cc1ab60481f26d6b8a2f6")
    var mongoClient: MongoClient = null
    var collection: util.Collection[Document] = null
    var mongoDatabase: MongoDatabase = null
    var network_syslog_coll: MongoCollection[Document] = null
    var iterDoc: FindIterable[Document] = null



    //***************Declaring MTSDB Stuff******************//
    var mtsdbAppKey = null
    var mtsdbAdminKey = null
    var mtsdbAppClient: MdtsdbClient = null


    try {
      mongoClient = new MongoClient("mongodb.fractal", 27017)
      mongoDatabase = mongoClient.getDatabase("conmon-db")
      network_syslog_coll = mongoDatabase.getCollection("network_syslog")
      val basicDBObject = new BasicDBObject()
      basicDBObject.append("_id", bsonObjId)
      var dbCursor = network_syslog_coll.find(basicDBObject)
      val document: Document = dbCursor.first()
      if (document == null) {
        println("*********document is null****************")
        println("Error...exiting program....")
        sys.exit(-1)
      } else {
        println("*********document exists******************")
        println("**********grabbing appKey for PFsense Net Traffic*****************")
        var appKey = document.get("mdtsdbAppKey").toString
        var appAdminKey = document.get("mdtsdbAdminKey").toString
        if (appKey == null || appAdminKey == null) {
          println("**********No appKey***********")
          println("**********SwimLane doesn't exist for PFSense Net Traffic**********")
          println("**********Creating appKey using MTSDBAdminClient*******************")
          var enableDebugOutput: Boolean = false
          val superAdmClient: MdtsdbClient = MdtsdbCredentials.createClientFromMasterProperties(enableDebugOutput)
          val admKeyResponse: JsonObject = superAdmClient.newAdminkey("pfsense_nettraffic") //new user and assoc. adminkey Resp
          val admKeyRes: Parse = new Parse(admKeyResponse)
          if (!admKeyRes.isOk()) {
            println("error...could not get admKeyResponse...something is wrong")
            sys.exit(-1)
          }
          val admKey: String = admKeyRes.getKey()
          val admSecretKey: String = admKeyRes.getSecretKey()
          val userDescription: String = admKeyRes.getUser()//user description
          val userAdminClient: MdtsdbClient = superAdmClient.newAdmClient(admKey,admSecretKey)
          println("user description: " + userDescription )
          println("Created a new userAdminClient for + : " + userDescription)
          val swimlanePropsResp: JsonObject = userAdminClient.newAppkey(userDescription)
          val swimlanePropsRes: Parse = new Parse(swimlanePropsResp)
          if (!swimlanePropsRes.isOk()) {
            println("error...could not create SwimLanes for PFsense net traffic...something is wrong")
            sys.exit(-1)
          }
          println("Creating swimlane client.... ")
          val swimlaneAppKey: String = swimlanePropsRes.getKey()
          val swimlaneSecretKey: String = swimlanePropsRes.getSecretKey()
          val swimlaneClient: MdtsdbClient = userAdminClient.newClient(swimlaneAppKey,swimlaneSecretKey)
          mtsdbAppClient = swimlaneClient
          //********don't forget to place the keys's (admin and app) into the document for later use********//
          var docIdObject: BasicDBObject = new BasicDBObject("_id",bsonObjId)
          var appKeyUpdateObj: BasicDBObject = new BasicDBObject("mdtsdbAppKey",swimlaneAppKey)
          var adminKeyUpdateObj: BasicDBObject = new BasicDBObject("mdtsdbAdminKey",admKey)
          var updateAppKeyObj = new BasicDBObject("$set",appKeyUpdateObj)
          var updateAdminKeyObj = new BasicDBObject("$set",adminKeyUpdateObj)
          mongoDatabase.getCollection("network_syslog").updateOne(docIdObject,appKeyUpdateObj)
          mongoDatabase.getCollection("network_syslog").updateOne(docIdObject,adminKeyUpdateObj)


        } else {
          println("*************Get appKey for PFsense Net Traffic appKey**************")
          println("appKey: " + appKey.toString)
          println("appAdminKey: " + appAdminKey.toString)
          println("*************Pulling up associated swimlane from MTSDB**************")
        }
      }
      mongoClient.close()
    } catch {
      case e: UnknownError => e.printStackTrace()
    }


    //***************Setting up Sensor************//
    var sensorObj: Measurement = new Measurement()
    var unixTime: Long = System.currentTimeMillis()
    var sensorDataJson = sensorObj.sensor(0).field("date","Nov 1st, 2098")
      .field("action","dropped")
      .field("port","53")
      .field("protocol","upd").time(unixTime).build()

    val statusJsonObj = mtsdbAppClient.sendStreamingData(sensorDataJson)


    //****************Getting Stuff from Kafka***************//
    /*
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
      */
  }//end of Driver.main()







  def chickIfSLaneLives(): Boolean = {
    false
  }

  def selectMTSDBHost(): String = {
    val mdtsdb1: String = "mdtsdb-1.fractal:8080"
    val mdtsdb2: String = "mdtsdb-2.fractal:8080"
    val mdtsdb3: String = "mdtsdb-3.fractal:8080"
    val mdtsdb4: String = "mdtsdb-4.fractal:8080"
    val mdtsdb5: String = "mdtsdb-5.fractal:8080"
    val hostList: Array[String] = Array(mdtsdb1, mdtsdb2, mdtsdb3, mdtsdb4, mdtsdb5)
    for (host <- hostList) {
      if (checkURlIsUp(host, 8080))
        return host
    }
    return "Error no mdtsdb host available"
  }

  def checkURlIsUp(host: String, port: Int): Boolean = {
    val protocol = "http"
    var socket: Socket = new Socket()
    val inetSocketAddress = new InetSocketAddress(host, port)
    try {
      socket.connect(inetSocketAddress)
      socket.isBound()
    } catch {
      case e: UnknownError => println("Error...could not verify MTSDB is up..."); false
    }
  }



}//end of object


