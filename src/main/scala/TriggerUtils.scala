/**
  * Created by mdb on 8/20/17.
  */
import com.fractal.mdtsdb.client.api.{MdtsdbClient, MdtsdbException, Measurement, Parse}
import com.google.gson.JsonObject

object TriggerUtils {
  var client:MdtsdbClient = null
  def setClient(client:MdtsdbClient):Unit={
    this.client = client
  }

  def runNewRemHostCase(): Unit ={
    //declare and initiate query string
    //declare and initate resp variable
    val script:String = ""
    /*
      set index for sensors 3,4,5 dedicated
      to date,send,recv respectively
    */
    var respJsonObj: JsonObject = client.streamingQuery("""SET INDEX $2.p1, $3.p1, $4.p1;""")
    val respCheck = new Parse(respJsonObj)
    if(!respCheck.isOk()){
      throw new MdtsdbException("Error: " + respCheck.getMessage());
    }
    val queryText:String = """ENV rmq_host: 'localhost', rmq_port: 5672, rmq_user: 'guest', rmq_pass: 'guest',
          rmq_vhost: '/', rmq_ex: 'mdtsdb_exchange', rmq_q: 'mdtsdb_notify', rmq_routing: '';
        CREATE TRIGGER 'pfs_new_host_alarm' ON INSERT $4
          HAVING $4, $5
            WHERE
              NOT EXIST $.p4 = $4.p4,
          DO AMQP [{$NOW} collision: sensor {$SENSOR} at {$T} with value {$VALUE} by trigger '{$TRIGGER}'];"""

    val respStr = client.streamingQuery(queryText).toString()

  }


}
