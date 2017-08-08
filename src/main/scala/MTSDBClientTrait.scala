import com.fractal.mdtsdb.client.api.MdtsdbClient
import com.google.gson.JsonObject

/**
  * Created by mdb on 8/7/17.
  */
trait  MTSDBClientTrait  {
  val tsEndpoint:String = "127.0.0.1"
  val port:Int = 8080
  val tsAppKey:String =""
  val tsAdmkey:String = ""
  val tsSecretKey:String = "???"
  val useSSL:Boolean = false
  def getClientDefault(tsAppKey:String,tsAdmkey:String,tsSecretKey:String,useSSL:Boolean):MdtsdbClient
  def getClientCustom(tsEndpoint:String,port:Int,tsAppKey:String,tsAdmkey:String,tsSecretKey:String,useSSL:Boolean):MdtsdbClient
  def createUser(userDetails:String,client: MdtsdbClient):JsonObject
  def createAdminClient(key:String, secretKey:String,client: MdtsdbClient):MdtsdbClient
  def createSwimLaneClient(key:String, secretKey:String,adminClient: MdtsdbClient):MdtsdbClient
  def writeToSwimLane(sensorMap:java.util.Map[String,String],adminClient:MdtsdbClient):JsonObject
  def setUserDetails(userDetails:String):Unit
  def querySwimLane():Unit
}
