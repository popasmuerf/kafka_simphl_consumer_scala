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
  def getKeysFromFile(): Tuple6[String,String,String,String,String,String] ={
    var mongoDocId:String = null
    var name:String = null
    var clAdmKey:String  = null
    var clAdmSecretKey:String = null
    var swlAppKey:String  = null
    var swlSecretKey:String =null

    try {
      val fileName = "main/resources/dat.txt"
      val in = this.getClass().getClassLoader().getResourceAsStream(fileName)
      val ini: Wini = new Wini(in)
      val section: Profile.Section = ini.get("keys")
      mongoDocId =  section.get("_id")
      name = section.get("name")
      clAdmKey  =  section.get("clAdmKey")
      clAdmSecretKey = section.get("clAdmSecretKey")
      swlAppKey = section.get("swlAppKey")
      swlSecretKey = section.get("swlSecretKey")
    }catch{
      case e: UnknownError => println("Error...could not write keys to resource file...");
    }
    return (mongoDocId,name,clAdmKey,clAdmSecretKey,swlAppKey,swlSecretKey)
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
    val HTTP_HOST = "mdtsdb-4.fractal"
    val HTTP_PORT = 8080
    val SUPER_KEY = "Y2DQZRbhZ8DSoq5vu832UgxQfp6Iw/GLsnwBY/pVzSBRDBMBPFIV3MY6ZBbiAKjw"
    val SUPER_SECRET = "R4wBIZ6/kFb3kjOC8QgM1X7AyFb2QXmyKmC23TZMYAn9VF7RBwlV"
    val USE_SSL = false
    val superClient=   new MdtsdbClient(HTTP_HOST, HTTP_PORT, "", SUPER_KEY, SUPER_SECRET, USE_SSL)
    //create a new user from the super client
    val userDetails = "pfsense_net_filter_traffic0"
    val resp1 = superClient.newAdminkey(userDetails)
    val res1 = new Parse(resp1)
    println(s"Got response from MDTSDB: $resp1")
    if(!res1.isOk){
      println("Can't create new admin key: user already exists?") //very interesting...
      System.exit(-1);
    }
    //get the keys to make an admin client
    val clAdmKey = res1.getKey
    val clSecretKey = res1.getSecretKey
    println("admin key-secret " + clAdmKey + " " + clSecretKey)
    val admClient = superClient.newAdmClient(clAdmKey,clSecretKey)
    admClient.enableDebugOutput()
    println("admClient " + clAdmKey + " " + clSecretKey)
    //we have new adminstrator key ast this point
    //we can start making swimlanes
    val swimlaneUserDetails = "pfsense_net_filter_traffic0"
    val swimlaneProps = admClient.newAppkey(swimlaneUserDetails,true,false)
    val results = new Parse(swimlaneProps)

    if(!results.isOk){
      println("Cannot create a new swimlane (already exists?): " + results.getMessage)
    }

    //now we need to make a client for that swimlane
    val clAppKey = results.getKey
    val clAppSecretKey = results.getSecretKey()
    val appClient = admClient.newClient(clAppKey,clAppSecretKey)
    return (appClient, clAppKey,clAppSecretKey)  //all of the above seems to be in order
  }
  def createSwimLaneTest(): Tuple3[Any,Any,Any] ={
    var enableDebugOutput: Boolean = false
    val superAdmClient=   new MdtsdbClient("mdtsdb-4.fractal", 8080, "", "Y2DQZRbhZ8DSoq5vu832UgxQfp6Iw/GLsnwBY/pVzSBRDBMBPFIV3MY6ZBbiAKjw", "R4wBIZ6/kFb3kjOC8QgM1X7AyFb2QXmyKmC23TZMYAn9VF7RBwlV", false)
    val superAdmKeyResponse:JsonObject = superAdmClient.newAdminkey("pfsense_net_filter_traffic_53")
    val supAdmKeyResParsed: Parse = new Parse(superAdmKeyResponse)
    if (!supAdmKeyResParsed.isOk()) {
      println("error...could not get admKeyResponse...something is wrong")
      sys.exit(-1)
    }
    val clAdmKey: String = supAdmKeyResParsed.getKey()
    println("clAdmKey :" + clAdmKey)
    val clAdmSecretKey: String =supAdmKeyResParsed.getSecretKey()
    println("clAdmSecret :" + clAdmSecretKey)
    val userDescription: String = supAdmKeyResParsed.getUser()
    val userAdminClient: MdtsdbClient = superAdmClient.newAdmClient(clAdmKey,clAdmSecretKey)
    //val swimlanePropsResp: JsonObject = userAdminClient.newAppkey("pfsense_nettraffic",true,true,"pfsense_nettraffic") //6. create sl Json Resp Obj
    val swimlanePropsResp: JsonObject = userAdminClient.newAppkey("pfsense_net_filter_traffic_53")
    val swimlanePropsResParsed: Parse = new Parse(swimlanePropsResp)
    if (!swimlanePropsResParsed.isOk()) {
      println("error...could not create SwimLanes for PFsense net traffic...something is wrong")
      sys.exit(-1)
    }
    println("Creating swimlane client.... ")
    val swimlaneAppKey: String = swimlanePropsResParsed.getKey()  //7. Get swimlane app key
    val swimlaneSecretKey: String = swimlanePropsResParsed.getSecretKey() //8. Get swimlane secret Key
    val swimlaneClient: MdtsdbClient = userAdminClient.newClient(swimlaneAppKey,swimlaneSecretKey)  //9. creates swimlane client using slapp and secret key
    return (swimlaneClient, swimlaneAppKey,swimlaneSecretKey)  //all of the above seems to be in order
  }
  def createSwimLaneTest(swimlaneName:String): Tuple3[Any,Any,Any] ={
    var enableDebugOutput: Boolean = false
    val superAdmClient=   new MdtsdbClient("mdtsdb-4.fractal", 8080, "", "Y2DQZRbhZ8DSoq5vu832UgxQfp6Iw/GLsnwBY/pVzSBRDBMBPFIV3MY6ZBbiAKjw", "R4wBIZ6/kFb3kjOC8QgM1X7AyFb2QXmyKmC23TZMYAn9VF7RBwlV", false)
    val superAdmKeyResponse:JsonObject = superAdmClient.newAdminkey(swimlaneName)
    val supAdmKeyResParsed: Parse = new Parse(superAdmKeyResponse)
    if (!supAdmKeyResParsed.isOk()) {
      println("error...could not get admKeyResponse...something is wrong")
      sys.exit(-1)
    }
    val clAdmKey: String = supAdmKeyResParsed.getKey()
    println("clAdmKey :" + clAdmKey)
    val clAdmSecretKey: String =supAdmKeyResParsed.getSecretKey()
    println("clAdmSecret :" + clAdmSecretKey)
    val userDescription: String = supAdmKeyResParsed.getUser()
    val userAdminClient: MdtsdbClient = superAdmClient.newAdmClient(clAdmKey,clAdmSecretKey)
    //val swimlanePropsResp: JsonObject = userAdminClient.newAppkey("pfsense_nettraffic",true,true,"pfsense_nettraffic") //6. create sl Json Resp Obj
    val swimlanePropsResp: JsonObject = userAdminClient.newAppkey("pfsense_net_filter_traffic_53")
    val swimlanePropsResParsed: Parse = new Parse(swimlanePropsResp)
    if (!swimlanePropsResParsed.isOk()) {
      println("error...could not create SwimLanes for PFsense net traffic...something is wrong")
      sys.exit(-1)
    }
    println("Creating swimlane client.... ")
    val swimlaneAppKey: String = swimlanePropsResParsed.getKey()  //7. Get swimlane app key
    val swimlaneSecretKey: String = swimlanePropsResParsed.getSecretKey() //8. Get swimlane secret Key
    val swimlaneClient: MdtsdbClient = userAdminClient.newClient(swimlaneAppKey,swimlaneSecretKey)  //9. creates swimlane client using slapp and secret key
    return (swimlaneClient, swimlaneAppKey,swimlaneSecretKey)  //all of the above seems to be in order
  }
  def createIpSwimLane(): Tuple3[Any,Any,Any] ={
    var ipSwimLaneName:String = null
    var enableDebugOutput: Boolean = false
    val superAdmClient: MdtsdbClient = MdtsdbCredentials.createClientFromMasterProperties(enableDebugOutput)
    val admKeyResponse: JsonObject = superAdmClient.newAdminkey("pfsense_net_filter_traffic_1") //new user and assoc. adminkey Resp
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
    //val swimlanePropsResp: JsonObject = userAdminClient.newAppkey(userDescription)
    val swimlanePropsResp = userAdminClient.newAppkey(ipSwimLaneName,true,true,ipSwimLaneName)
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
  def createSwimLaneClientTest(clAdmKey:String,clSecretKey:String,slAppKey:String,slSecretKey:String): MdtsdbClient ={
    println("**********We have swimlane keys***********")
    println("**********We can by-pass creating them**********")
    println("**********we just need to create an MTSDBAdminClient*******************")
    var enableDebugOutput: Boolean = false
    val superAdmClient=   new MdtsdbClient("mdtsdb-4.fractal", 8080, "", "Y2DQZRbhZ8DSoq5vu832UgxQfp6Iw/GLsnwBY/pVzSBRDBMBPFIV3MY6ZBbiAKjw", "R4wBIZ6/kFb3kjOC8QgM1X7AyFb2QXmyKmC23TZMYAn9VF7RBwlV", false)
    val superAdmKeyResponse:JsonObject = superAdmClient.newAdminkey("pfsense_nettraffic")
    val supAdmKeyResParsed: Parse = new Parse(superAdmKeyResponse)
    if (!supAdmKeyResParsed.isOk()) {
      println("error...could not get admKeyResponse...something is wrong")
      sys.exit(-1)
    }
    val clAdmClient: MdtsdbClient = superAdmClient.newAdmClient(clAdmKey.trim(),clSecretKey.trim())
    println("Creating swimlane client.... ")

    val swimlaneClient = clAdmClient.newClient(slAppKey.trim(),slSecretKey.trim())
    return swimlaneClient
  }
  def createSwimLaneClient(clAdminKey:String,swlAdminSecretKey:String,swimlaneAppKey:String,swimLaneSecretKey:String): MdtsdbClient ={
    println("**********We have swimlane keys***********")
    println("**********We can by-pass creating them**********")
    println("**********we just need to create an MTSDBAdminClient*******************")
    var enableDebugOutput: Boolean = false
    val superAdmClient=   new MdtsdbClient("mdtsdb-4.fractal", 8080, "", "Y2DQZRbhZ8DSoq5vu832UgxQfp6Iw/GLsnwBY/pVzSBRDBMBPFIV3MY6ZBbiAKjw", "R4wBIZ6/kFb3kjOC8QgM1X7AyFb2QXmyKmC23TZMYAn9VF7RBwlV", false)
    val superAdmKeyResponse:JsonObject = superAdmClient.newAdminkey("pfsense_nettraffic")
    val supAdmKeyResParsed: Parse = new Parse(superAdmKeyResponse)
    if (!supAdmKeyResParsed.isOk()) {
      println("error...could not get admKeyResponse...something is wrong")
      sys.exit(-1)
    }
    val clAdmKey: String = supAdmKeyResParsed.getKey()
    val clAdmSecretKey: String =supAdmKeyResParsed.getSecretKey()
    val userDescription: String = supAdmKeyResParsed.getUser()
    val userAdminClient: MdtsdbClient = superAdmClient.newAdmClient(clAdmKey,clAdmSecretKey)
    //val swimlanePropsResp: JsonObject = userAdminClient.newAppkey("pfsense_nettraffic",true,true,"pfsense_nettraffic") //6. create sl Json Resp Obj
    val swimlanePropsResp: JsonObject = userAdminClient.newAppkey("pfsense_nettraffic")
    val swimlanePropsResParsed: Parse = new Parse(swimlanePropsResp)
    if (!swimlanePropsResParsed.isOk()) {
      println("error...could not create SwimLanes for PFsense net traffic...something is wrong")
      sys.exit(-1)
    }
    println("Creating swimlane client.... ")
    val swimlaneAppKey: String = swimlanePropsResParsed.getKey()  //7. Get swimlane app key
    val swimlaneSecretKey: String = swimlanePropsResParsed.getSecretKey() //8. Get swimlane secret Key
    val swimlaneClient = userAdminClient.newClient(swimlaneAppKey,swimlaneSecretKey)
    return swimlaneClient
  }

  def createSwimLaneClientTestII(swimlaneAppKey:String,swimLaneSecretKey:String): MdtsdbClient ={
    println("**********We have swimlane keys***********")
    println("**********We can by-pass creating them**********")
    println("**********we just need to create an MTSDBAdminClient*******************")
    var enableDebugOutput: Boolean = false
    val masterKey = "Y2DQZRbhZ8DSoq5vu832UgxQfp6Iw/GLsnwBY/pVzSBRDBMBPFIV3MY6ZBbiAKjw"
    val secretMasterKey = "R4wBIZ6/kFb3kjOC8QgM1X7AyFb2QXmyKmC23TZMYAn9VF7RBwlV"
    val superAdminClient: MdtsdbClient = new MdtsdbClient("mdtsdb-5.fractal",8080, "", masterKey,secretMasterKey, false)
    val adminClientRespJsonObj:JsonObject = superAdminClient.newAdminkey("pfsense_nettraffic")
    val adminClientRespJsonObjParsed:Parse = new Parse(adminClientRespJsonObj)
    val adminClientKey: String = adminClientRespJsonObjParsed.getKey()
    val adminClientSecretKey: String = adminClientRespJsonObjParsed.getSecretKey()
    val adminClientUser: String = adminClientRespJsonObjParsed.getUser()
    val adminClient:MdtsdbClient = superAdminClient.newAdmClient(adminClientKey,adminClientSecretKey)
    val swimlanePropsJsonObjResp: JsonObject = adminClient.newAppkey("pfsense_nettraffic")
    val swimlaneClient = adminClient.newClient(swimlaneAppKey.trim(),swimLaneSecretKey.trim())
    return swimlaneClient
  }

  def createIpSwimLaneClient(ipAdminKey:String,ipAppKey:String,ipSecretKey:String): MdtsdbClient ={
    println("**********We have swimlane keys***********")
    println("**********We can by-pass creating them**********")
    println("**********we just need to create an MTSDBAdminClient*******************")
    val superAdminClient: MdtsdbClient = new MdtsdbClient("mdtsdb-5.fractal",8080, "", "IkwCmcm3bbPcML", "Ruio4Np5kOa5CT", false)
    val ipSwimlaneClient: MdtsdbClient = superAdminClient.newAdmClient(ipAppKey,ipSecretKey)
    return ipSwimlaneClient
  }
  def insertIntoSwimLane(swimlaneClient:MdtsdbClient,map:java.util.Map[String,String]): Unit = {
    val keyList: Array[AnyRef] = map.keySet().toArray()
    val valList: Array[AnyRef] = map.values().toArray()
    var unixTime: Long = System.currentTimeMillis() / 1000L
    var sensorData: Measurement = new Measurement()
    val mapLength = map.size()
    for (i <- 0 to (mapLength - 1)) {
      val sensorJsonObj: JsonObject = sensorData.sensor(i).field(keyList(i).toString, valList(i).toString).time(unixTime).build()
      val statusJsonObj = swimlaneClient.sendEventsData(sensorJsonObj)
    }
  }
  def insertIntoSwimLaneTest(swimlaneClient:MdtsdbClient,map:java.util.Map[String,String]): Unit = {
    val dateTime = map.get("dateTime")
    val ipSend = map.get("ipSend")
    val ipRecv = map.get("ipRecv")
    val protocol = map.get("protocol")
    val action = map.get("action")
    val clientPort = map.get("clientPort")
    val serverPort = map.get("serverPort")
    var unixTime: Long = System.currentTimeMillis() / 1000L
    var sensorData: Measurement = new Measurement()
    val mapLength = map.size()
    val sesnsorData = new Measurement()
      .sensor(0)
      .field("dateTime",dateTime)
      .field("ipSend",ipSend)
      .field("ipRecv",ipRecv)
      .field("protocol",protocol)
      .field("action",action)
      .field("clientPort",clientPort)
      .field("serverPort",serverPort)
      .time(unixTime)
      .build()
    val status = swimlaneClient.sendEventsData(sesnsorData)
  }
  def insertIpIntoSwimLane(client:MdtsdbClient,map:java.util.Map[String,String]): Unit = {
    val ipSend = map.get("ipSend")
    val ipRecv = map.get("ipRecv")
    val action = map.get("action")
    val clientPort = map.get("clientPort")
    val serverPort = map.get("serverPort")
    val externHost = ipSend + ":" + clientPort
    val keyList: Array[AnyRef] = map.keySet().toArray()
    val valList: Array[AnyRef] = map.values().toArray()
    var unixTime: Long = System.currentTimeMillis() / 1000L
    var sensorData: Measurement = new Measurement()
    val mapLength = map.size()
    for (i <- 0 to (mapLength - 1)) {
      val sensorJsonObj: JsonObject = sensorData.sensor(i).field(keyList(i).toString, valList(i).toString).time(unixTime).build()
      val statusJsonObj = client.sendStreamingData(sensorJsonObj)
    }
  }

  def checkPFSenseNettrafficSwimLane(): Boolean ={
    return false
  }
  def checkPFSenseInternalIpSwimLane(): Boolean ={
    return false
  }
}
