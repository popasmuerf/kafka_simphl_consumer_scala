import java.io._
import java.net.{InetSocketAddress, Socket}

import com.fractal.mdtsdb.client.api.{MdtsdbClient, Measurement, Parse}
import com.google.gson.JsonObject
import com.mongodb.{BasicDBObject, MongoClient}
import examples.com.fractal.mdtsdb.client.MdtsdbCredentials
import org.bson.types.ObjectId
import org.ini4j.{Profile, Wini}

import scala.util.matching.Regex

/**
  * Created by mdb on 8/21/17.
  */
object MDTSDBUtils {


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
      adminKey = section.get("admin")
      appKey = section.get("app")
      secretKey = section.get("secret")
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
      docIdStr = section.get("object_id")
    }catch{
      case e: UnknownError => println("Error...could not write keys to resource file...");
    }
    return docIdStr
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
    println("This is my adminKey: " + admKey)
    val admSecretKey: String = admKeyRes.getSecretKey()
    println("This is my adminSecretKey: " + admSecretKey)
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
    println("This is my swimlaneAppKey: " + swimlaneAppKey)
    val swimlaneSecretKey: String = swimlanePropsRes.getSecretKey()
    println("this is my swimlaneSecretKey :" + swimlaneSecretKey )
    val swimlaneClient: MdtsdbClient = userAdminClient.newClient(swimlaneAppKey,swimlaneSecretKey)
    return (swimlaneClient, swimlaneAppKey,swimlaneSecretKey)
  }
  def createIpSwimLane(): Tuple3[Any,Any,Any] ={
    var enableDebugOutput: Boolean = false
    val superAdmClient: MdtsdbClient = MdtsdbCredentials.createClientFromMasterProperties(enableDebugOutput)
    val admKeyResponse: JsonObject = superAdmClient.newAdminkey("pfsense_nettraffic") //new user and assoc. adminkey Resp
    val admKeyRes: Parse = new Parse(admKeyResponse)
    if (!admKeyRes.isOk()) {
      println("error...could not get admKeyResponse...something is wrong")
      sys.exit(-1)
    }
    val admKey: String = admKeyRes.getKey()
    println("This is my adminKey: " + admKey)
    val admSecretKey: String = admKeyRes.getSecretKey()
    println("This is my adminSecretKey: " + admSecretKey)
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
    println("This is my swimlaneAppKey: " + swimlaneAppKey)
    val swimlaneSecretKey: String = swimlanePropsRes.getSecretKey()
    println("this is my swimlaneSecretKey :" + swimlaneSecretKey )
    val swimlaneClient: MdtsdbClient = userAdminClient.newClient(swimlaneAppKey,swimlaneSecretKey)
    return (swimlaneClient, swimlaneAppKey,swimlaneSecretKey)
  }
  def createSwimLaneClient(swimlaneAdminKey:String,swimlaneAppKey:String,swimLaneSecretKey:String): MdtsdbClient ={
    println("**********We have swimlane keys***********")
    println("**********We can by-pass creating them**********")
    println("**********we just need to create an MTSDBAdminClient*******************")
    var enableDebugOutput: Boolean = false
    //val superAdminClient: MdtsdbClient = MdtsdbCredentials.createClientFromMasterProperties(enableDebugOutput)
    val superAdminClient: MdtsdbClient = new MdtsdbClient("mdtsdb-5.fractal",8080, "", "IkwCmcm3bbPcML", "Ruio4Np5kOa5CT", false)
    val swimlaneClient: MdtsdbClient = superAdminClient.newAdmClient(swimlaneAppKey,swimLaneSecretKey)
    return swimlaneClient
    //val swimlaneClient: MdtsdbClient = userAdminClient.newClient(swimlaneAppKey,swimlaneSecretKey)
  }
  def createIpSwimLaneClient(ipAdminKey:String,ipAppKey:String,ipSecretKey:String): MdtsdbClient ={
    println("**********We have swimlane keys***********")
    println("**********We can by-pass creating them**********")
    println("**********we just need to create an MTSDBAdminClient*******************")
    var enableDebugOutput: Boolean = false
    //val superAdminClient: MdtsdbClient = MdtsdbCredentials.createClientFromMasterProperties(enableDebugOutput)
    val superAdminClient: MdtsdbClient = new MdtsdbClient("mdtsdb-5.fractal",8080, "", "IkwCmcm3bbPcML", "Ruio4Np5kOa5CT", false)
    val ipSwimlaneClient: MdtsdbClient = superAdminClient.newAdmClient(ipAppKey,ipSecretKey)
    return ipSwimlaneClient
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
  def insertIpIntoSwimLane(client:MdtsdbClient,map:java.util.Map[String,String]): Unit = {
    val ipSend = map.get("ipSend")
    val ipRecv = map.get("ipRecv")
    val action = map.get("action")
    val clientPort = map.get("clientPort")
    val serverPort = map.get("serverPort")
    val externHost = ipSend + ":" + clientPort

    /*
      1. need swimlane name: pfsense_nettraffic_10.5.6.1
      2. insert into sensor $0
      3. ^^using admin and secret key

     */
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

  def checkPFSenseNettrafficSwimLane(): Boolean ={
    return false
  }
  def checkPFSenseInternalIpSwimLane(): Boolean ={
    return false
  }

}
