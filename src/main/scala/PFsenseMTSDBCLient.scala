import java.util

import com.fractal.mdtsdb.client.api.{MdtsdbClient, Measurement, Parse}
import com.google.common.base.Preconditions.checkArgument
import com.google.gson.JsonObject
import examples.com.fractal.mdtsdb.client.MdtsdbCredentials
/**
  * Created by mdb on 8/7/17.
  */
class PFsenseMTSDBCLient extends MTSDBClientTrait {
  private var userDetails:String = ""
  override def getClientDefault(tsAppKey: String, tsAdmkey: String, tsSecretKey: String, useSSL: Boolean): MdtsdbClient = {
    var mtsdbClient: MdtsdbClient = new MdtsdbClient(tsAppKey,tsAdmkey,tsSecretKey,useSSL)
    mtsdbClient
  }
  override def getClientCustom(tsEndpoint: String, port: Int, tsAppKey: String, tsAdmkey: String, tsSecretKey: String, useSSL: Boolean): MdtsdbClient = {
    var mtsdbClientCust = new MdtsdbClient(tsEndpoint,port,tsAppKey,tsAdmkey,tsSecretKey,useSSL)
    mtsdbClientCust
  }

  override def setUserDetails(userDetails: String): Unit = {
    this.userDetails = userDetails
  }
  override def createUser(userDetails:String,client:MdtsdbClient): JsonObject = {
    var user = client.newAdminkey(userDetails)
    user
  }
  override def createAdminClient(key:String, secretKey:String, client:MdtsdbClient):MdtsdbClient = {
    val adminClient: MdtsdbClient = client.newAdmClient(key,secretKey)
    adminClient
  }

  override def createSwimLaneClient(key:String, secretKey:String,adminClient: MdtsdbClient):MdtsdbClient = {
    val swimLaneClient: MdtsdbClient = adminClient.newAdmClient(key,secretKey)
    swimLaneClient
  }

  override def writeToSwimLane(sensorMap: util.Map[String, String], adminClient: MdtsdbClient): JsonObject = {
    val sensorData:JsonObject = new Measurement()
      .sensor(1).field(sensorMap).build()
    val status: JsonObject = adminClient.sendStreamingData(sensorData)
    status
  }

  override def querySwimLane(): Unit = {}
}
