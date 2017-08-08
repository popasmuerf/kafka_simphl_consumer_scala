/**
  * Created by mdb on 8/7/17.
  * https://bitbucket.org/fractalindustries/mdtsdb-client-java/src
  */
import com.fractal.mdtsdb.client.api.{MdtsdbClient, Measurement, Parse}
import com.google.common.base.Preconditions.checkArgument
import com.google.gson.JsonObject
import examples.com.fractal.mdtsdb.client.MdtsdbCredentials

class MTSDDBRefClass {
  def run(enableDebugOutput:Boolean){
    println("Swimlane operations:")
    println("create an admin client...")

    //Create client for adminstrative purposes
    val tsEndpoint:String = "127.0.0.1"  //time Series endpt
    val port:Int = 8080   //tsEndpt listening port
    val tsAppKey:String ="???"  //tsAppKey  --> we leave this blank in the examples....why ?
    val tsAdmkey:String = "" //tsAdmKey -->  AKA "masterKey" that grants super-user rights ?
    val tsSecretKey:String = "???" //tsSecretKey is used in tandem with tsAdmkey ???
    val useSSL:Boolean = false  //encrypt or not
    //We create a new admin client (standard endpt)
    var mtsdbClient: MdtsdbClient = new MdtsdbClient(tsAppKey,tsAdmkey,tsSecretKey,useSSL)
    //We create a new admin client to do admin stuff....(custom endpt)
    var mtsdbClientCust: MdtsdbClient = new MdtsdbClient(tsEndpoint,port,tsAppKey,tsAdmkey,tsSecretKey,useSSL)

    //create a new user
    //with MdtsdbClient::newAdminKey
    val userDetails = "mdb"
    val response:JsonObject = mtsdbClientCust.newAdminkey(userDetails)
    val res: Parse = new Parse(response)

    /*
      ^^^ userDetails is a string with a user description.
      A status of teh remote call of a MDTSDB
      server method can be checked with the help
      of the client Parse class:
          if(res.isOk))
          ....
      If the call successful, the variable 'res' provides
      access to reponse fields with helper member methods:
          String clAdmKey = res.getKey() ;
          String clSecretKey = res.getSecretKey() ;
          String userDesc = res.getUser();

     */

     //create an admin client for the new user
    val adminClient: MdtsdbClient = mtsdbClient.newAdmClient(res.getKey(),res.getSecretKey())


    //create a swimlane(application/tsAppKey)
    //make a swimlane with 'MdtsdbClient::newAppKey':
    val swimlaneProps:JsonObject = adminClient.newAppkey(userDetails)
    val results:Parse = new Parse(swimlaneProps)
    if(results.isOk()){
      //....proceed..else..mitiate problem
    }

    //create a client for the swimlane
    val clAppKey:String = results.getKey()
    val clSecretKey:String = results.getSecretKey()
    val client1:MdtsdbClient = adminClient.newClient(clAppKey,clSecretKey)


    //storing data
    //To store sensor data to MDTSDB, use
    //'MdtsdbClient:sendStreamingData()'
    //or
    //'MdtsdbClient:sendEventsData()'
    val unixTme:Long = System.currentTimeMillis()/1000L
    val sensorData1:JsonObject = new Measurement()
      .sensor(1)
      .field("p1","v1")
      .field("p2","v2")
      .sensor(2)
      .value("sensor_value")
        .tag("t1","v1")
        .tag("t1","v2")
        .time(unixTme)
        .build()
    val sensorData2:JsonObject = new Measurement()
      .sensor(1)
      .field("p1","v1")
      .field("p2","v2")
      .sensor(2)
      .value("sensor_value")
      .tag("t1","v1")
      .tag("t1","v2")
      .time(unixTme)
      .build()
    val status1: JsonObject = client1.sendStreamingData(sensorData1)
    val status2: JsonObject = client1.sendStreamingData(sensorData2)

    //To query data from MDTSDB, use
    //'MdtsdbClient::streamingQuery()'
    // -or-
    //'MdtsdbClient::eventsQuery()'





  }
}
