/**
  * Created by mdb on 8/4/17.
  */
class MTSDBClient {
  val swimlane = "pfsense_firewall_events"
  val mykey = " insert here swimlane for PFSense events logs"
  val other_key = "insert here for swimlane notifications"
  val queryStr = ""
  def getValueMap( _date:String, _host:String, _sendAddr:String, _recvAddr:String, _port:String,_protocol:String, _match:String, _pass:String, _bytes:String):Map[String,String]={
    val data = Map("date"->_date,"host"->_host,"sendAddr"->_sendAddr, "recvAddr"-> _recvAddr,"port"->_port,"protocol"->_protocol,"match"->_match, "pass"->_pass,"bytes"->_bytes)
    data
  }

}
