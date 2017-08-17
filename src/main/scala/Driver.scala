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
  val userInfo = "pfsense_nettraffic_test8"
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
    var mtsdbAppKey:String  = null
    var mtsdbAdminKey:String = null
    var mtsdbSecretKey:String = null
    var mtsdbAppClient: MdtsdbClient = null



    //*************Get MTSDB stuff from File***************//
    val mtsdbKeysTuple: (String, String, String) = getKeysFromFile()


    //test value

    //*************Check if MTSDB Keys actually exist*******//
    if(mtsdbKeysTuple._1.equals("null")){
      println("no swimlane exists...need to generate the appropiate mtsdb swimlane admin, app, secret keys....")
      println(".....working.....")
      val swimLaneStuffTuple: (Any, Any, Any,Any) = createSwimLaneClient()
      val swimlaneClient =  swimLaneStuffTuple._1.asInstanceOf[MdtsdbClient]
      val swimlaneAppKey = swimLaneStuffTuple._2.asInstanceOf[String]
      println("swimlaneAppKey : " + swimlaneAppKey )
      val swimlaneAdminKey = swimLaneStuffTuple._3.asInstanceOf[String]
      println("swimlaneAdminKey : " + swimlaneAdminKey)
      val swimlaneSecretKey = swimLaneStuffTuple._4.asInstanceOf[String]
      println("swimlaneSecretKey : " + swimlaneSecretKey)
      //Write keys to file
      //writeKeysToFile(swimlaneAppKey:String,swimlaneAdminKey:String,swimlaneSecretKey:String,docObjectIdStr:String, mongoClient: MongoClient ): Unit ={
      writeKeysToFile(swimlaneAppKey,swimlaneAdminKey,swimlaneSecretKey)
      }else{
        println("Just checked the config file...and we have a swimlane....")
        println("So let's just get our swimlane client.......")
        mtsdbAppKey = mtsdbKeysTuple._1
        mtsdbAdminKey = mtsdbKeysTuple._2
        mtsdbSecretKey = mtsdbKeysTuple._3
        mtsdbAppClient = createSwimLaneClient(mtsdbAdminKey,mtsdbAppKey, mtsdbSecretKey)
        println(mtsdbAppClient.toString)


    }

    //****************Getting Stuff from Kafka***************//


    while(true){
      val records: ConsumerRecords[String, String] = consumer.poll(1)
        for(record <- records){
          //println(record.value())
          //PFSenseParser.parseRecordToStr(record.value())
          //val recordValue: String = record.value()
          val recordMap: Option[util.Map[String, String]] = PFSenseParser.parseRecordToMap(record.value())
          if(recordMap != None) {
            println(recordMap.get.toString())
            insertIntoSwimLane(mtsdbAppClient, recordMap.get)
          }
          else
            println("Got back a back record that did'nt conform to nettraffic regex....")
          //println(recordMap.toString)
        }
      }

  }//end of Driver.main()


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

  def checkSwimLane(client:MdtsdbClient, appkey:String): Boolean = {
    var reponseJsonObj: JsonObject =  client.reportAppkey()
    var responseParsed = new Parse(reponseJsonObj)
    var key: String = responseParsed.getAppKey
    if(!key.equals(appkey)){
      return false
    }else{
      return true
    }
  }
  def createSwimLaneClient(): Tuple4[Any,Any,Any,Any] ={
    println("**********No appKey***********")
    println("**********SwimLane doesn't exist for PFSense Net Traffic**********")
    println("**********Creating appKey using MTSDBAdminClient*******************")
    var enableDebugOutput: Boolean = false
    val superAdmClient: MdtsdbClient = MdtsdbCredentials.createClientFromMasterProperties(enableDebugOutput)
    val admKeyResponse: JsonObject = superAdmClient.newAdminkey(userInfo) //new user and assoc. adminkey Resp
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
    return(swimlaneClient,swimlaneAppKey,admKey,swimlaneSecretKey)

  }

  def createSwimLaneClient(swimlaneAdminKey:String,swimlaneAppKey:String,swimLaneSecretKey:String): MdtsdbClient ={
    println("**********We have swimlane keys***********")
    println("**********We can by-pass creating them**********")
    println("**********we just need to create an MTSDBAdminClient*******************")
    var enableDebugOutput: Boolean = false
    val superAdminClient: MdtsdbClient = MdtsdbCredentials.createClientFromMasterProperties(enableDebugOutput)
    val swimlaneClient: MdtsdbClient = superAdminClient.newAdmClient(swimlaneAppKey,swimLaneSecretKey)
    return swimlaneClient

    //val swimlaneClient: MdtsdbClient = userAdminClient.newClient(swimlaneAppKey,swimlaneSecretKey)
  }

  def insertIntoSwimLane(client:MdtsdbClient,map:java.util.Map[String,String]): Unit = {
    val keyList: Array[AnyRef] = map.keySet().toArray()
    val valList: Array[AnyRef] = map.values().toArray()

    var unixTime: Long = System.currentTimeMillis() / 1000L
    var sensorData: Measurement = new Measurement()
    val mapLength = map.size()
    for (i <- 0 to (mapLength - 1)) {
       val sensorJsonObj: JsonObject = sensorData.sensor(i).field(keyList(i).toString, valList(i).toString).time(unixTime).build()
       val statusJsonObj = client.sendStreamingData(sensorJsonObj)
      // checkArgument(Parse.getStatus(statusJsonObj) == 1, "expect value status in response")
    }

  }
    def getKeysFromMongoConn(): Tuple3[String,String,String] ={
      val bsonObjId = new ObjectId("598cc1ab60481f26d6b8a2f6")
      val mongoClient = new MongoClient("mongodb.fractal", 27017)
      val mongoDatabase = mongoClient.getDatabase("conmon-db")
      val network_syslog_coll = mongoDatabase.getCollection("network_syslog")
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
        var appAdminKey: String = document.get("mdtsdbAdminKey").toString
        var appSecretKey: String = document.get("mdtsdbSecretKey").toString
        return (appKey,appAdminKey,appSecretKey)
      }

  }
  def createSwimLane(): Tuple3[Any,Any,Any] ={
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
    return (swimlaneClient, swimlaneAppKey,swimlaneSecretKey)
  }

  def writeKeysToMongo(swimlaneAppKey:String,swimlaneAdminKey:String,swimlaneSecretKey:String,docObjectIdStr:String, mongoClient: MongoClient ): Unit ={
    try {
      val bsonObjId = new ObjectId("598cc1ab60481f26d6b8a2f6")
      //val mongoClient = new MongoClient("mongodb.fractal", 27017)
      val mongoDatabase = mongoClient.getDatabase("conmon-db")
      val network_syslog_coll = mongoDatabase.getCollection("network_syslog")
      var docIdObject: BasicDBObject = new BasicDBObject("_id", bsonObjId)
      var appKeyUpdateObj: BasicDBObject = new BasicDBObject("mdtsdbAppKey", swimlaneAppKey)
      var secretKeyUpdateObj: BasicDBObject = new BasicDBObject("mdtsdbSecretKey", swimlaneSecretKey)
      var adminKeyUpdateObj: BasicDBObject = new BasicDBObject("mdtsdbAdminKey", swimlaneAdminKey)
      var updateAppKeyObj = new BasicDBObject("$set", appKeyUpdateObj)
      var updateAdminKeyObj = new BasicDBObject("$set", adminKeyUpdateObj)
      var updateSecretKeyObj = new BasicDBObject("$set", secretKeyUpdateObj)
      mongoDatabase.getCollection("network_syslog").updateOne(docIdObject, appKeyUpdateObj)
      mongoDatabase.getCollection("network_syslog").updateOne(docIdObject, adminKeyUpdateObj)
      mongoDatabase.getCollection("network_syslog").updateOne(docIdObject, secretKeyUpdateObj)
    }catch {
      case e: UnknownError => println("Error...could not write keys to MongoDB is up...");
    }
  }

  def writeKeysToFile(swimlaneAppKey:String,swimlaneAdminKey:String,swimlaneSecretKey:String): Unit ={
    import java.io.File
    val adminRegEx: Regex = "admin\\s+=\\s+null".r
    val appRegEx: Regex = "app\\s+=\\s+null".r
    val secretRegEx:Regex = "secret\\s+=\\s+null".r
    try {
      val fileName = "dat.txt"
      val tempFileName = "out.txt"
      var classLoader:ClassLoader  = getClass().getClassLoader()
      var file:File = new File(classLoader.getResource(fileName).getFile())
      var fileReader = new FileReader(classLoader.getResource(fileName).getFile())
      var fileWriter = new FileWriter(classLoader.getResource(tempFileName).getFile())
      var bufferedReader = new BufferedReader(fileReader)
      var bufferedWriter = new BufferedWriter(fileWriter)
      var line:String =""
      var strAdminKey:String = null
      var strAppKey:String = null
      var strSecretKey:String = null
      while({line = bufferedReader.readLine(); line != null}){
        if(line.contains("admin = null")){
          strAdminKey = line.replace("null",swimlaneAdminKey)
          println(strAdminKey)
          fileWriter.write(strAdminKey)
        }else
        if(line.contains("app = null")){
          strAppKey = line.replace("null",swimlaneAppKey)
          println(strAppKey)
          fileWriter.write(strAppKey)
        }else
        if(line.contains("secret = null")){
          strSecretKey = line.replace("null",swimlaneSecretKey)
          println(strSecretKey)
          fileWriter.write(strSecretKey)
        }else{
          println(line)
          fileWriter.write(line)
        }
      }

    }catch {
      case e: UnknownError => println("Error...could not write keys to resource file...");
    }
  }

  def getKeysFromFile(): Tuple3[String,String,String] ={
    var adminKey:String = null
    var appKey:String = null
    var secretKey:String = null
    try {
      val fileName = "dat.txt"
      var classLoader: ClassLoader = getClass().getClassLoader()
      var file: File = new File(classLoader.getResource(fileName).getFile())
      var ini: Wini = new Wini(file)
      var section: Profile.Section = ini.get("keys")
      adminKey = section("admin")
      appKey = section("app")
      secretKey = section("secret")
    }catch{
      case e: UnknownError => println("Error...could not write keys to resource file...");
    }
    return (appKey,adminKey,secretKey)
  }

  def getMongoDocIdFromFile(): String ={
    var docIdStr:String = null
    try {
      val fileName = "dat.txt"
      var classLoader: ClassLoader = getClass().getClassLoader()
      var file: File = new File(classLoader.getResource(fileName).getFile())
      var ini: Wini = new Wini(file)
      var section: Profile.Section = ini.get("mongo")
      docIdStr = section("object_id")
    }catch{
      case e: UnknownError => println("Error...could not write keys to resource file...");
    }
    return docIdStr
  }

}//end of object


