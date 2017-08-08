import java.util
import java.util.HashMap
import java.util.Map;


/**
  * Created by mikeyb on 7/31/17.
  */
object PFSenseParser {
  def parseRecord(record:String):Option[String]= {
    var lineNumber = 1
    val dayPttrn = "(^\\w{3}\\s{1}\\d{1,2})".r //day pattern
    val timePttrn = "\\d{2}:\\d{2}:\\d{2}".r //time pattern
    val dateTimePttrn = "\\w{3}\\s{1}\\d{1,2}\\s\\d{2}:\\d{2}:\\d{2}".r //nix -- DayTime
    val ipAddrPttrn = "\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}".r //ipAddr
    val portPttrn = ",(6553[0-5]|655[0-2][0-9]\\d|65[0-4](\\d){2}|6[0-4](\\d){3}|[1-5](\\d){4}|[1-9](\\d){0,3}){2}".r
    val actionPttrn = "pass|block|drop|dropped".r
    val protocolPttrn = "tcp|udp|igmp|icmp".r

    //    println(dayPttrn.findFirstIn(record).get)
    val day: Option[String] = dayPttrn.findFirstIn(record)
    // println(timePttrn.findFirstIn(record).get)
    val time: Option[String] = timePttrn.findFirstIn(record)
    // println(dateTimePttrn.findFirstIn(record).get)
    val dateTime: Option[String] = dateTimePttrn.findFirstIn(record)
    //ipAddrPttrn.findAllIn(record)).foreach(println _)
    var ipAddress = Option(ipAddrPttrn.findAllIn(record).toList)
    //portPttrn.findAllIn(record).foreach(println)
    val port: Option[List[String]] = Option(portPttrn.findAllIn(record).toList)
    //println(actionPttrn.findFirstIn(record).get)
    val action: Option[String] = actionPttrn.findFirstIn(record)
    //println(protocolPttrn.findFirstIn(record).get)
    val protocol: Option[String] = protocolPttrn.findFirstIn(record)
    val recordStr = buildRecordStr(day, time, dateTime, ipAddress, port, action, protocol)
    if (recordStr != None) {
      println(recordStr.get)
      return recordStr
    }
    else {
      println(":::bad log record:::")
      return None
    }
  }
  def buildRecordStr(day:Option[String],time:Option[String],dateTime:Option[String],ipAddress:Option[List[String]],port:Option[List[String]],action:Option[String],protocol:Option[String]): Option[String] = {
    var recordStr:String = ""
    day.map( _ => recordStr = day.get)
    dateTime.map(_  => {recordStr += " " ; recordStr += dateTime.get})
    ipAddress.foreach(_ => {
      val ipList = ipAddress.get
      if(ipList.length < 2 ){return None}
      recordStr += " "
      recordStr += ipList(0)
      recordStr += " "
      recordStr += ipList(1)
    })
    port.foreach(_ => {
      val portList = port.get
      recordStr += " "
      recordStr += portList(portList.length - 3 )
      recordStr += " "
      recordStr += portList(portList.length - 2 )
    })
    action.map( _ => {recordStr += " " ; recordStr += action.get})
    protocol.map( _ => {recordStr += " " ; recordStr += protocol.get})
    Option(recordStr)
  }
  def getParsedRecStr(day:Option[String],time:Option[String],dateTime:Option[String],ipAddress:Option[List[String]],port:Option[List[String]],action:Option[String],protocol:Option[String]):Option[String] ={
    var recordStr:String = ""
    day.map( _ => recordStr = day.get)
    dateTime.map(_  => {recordStr += " " ; recordStr += dateTime.get})
    ipAddress.foreach(_ => {
      val ipList = ipAddress.get
      if(ipList.length < 2 ){return None}
      recordStr += " "
      recordStr += ipList(0)
      recordStr += " "
      recordStr += ipList(1)
    })
    port.foreach(_ => {
      val portList = port.get
      recordStr += " "
      recordStr += portList(portList.length - 3 )
      recordStr += " "
      recordStr += portList(portList.length - 2 )
    })
    action.map( _ => {recordStr += " " ; recordStr += action.get})
    protocol.map( _ => {recordStr += " " ; recordStr += protocol.get})
    Option(recordStr)
  }
  def getParsedRecMap(day:Option[String],time:Option[String],dateTime:Option[String],ipAddress:Option[List[String]],port:Option[List[String]],action:Option[String],protocol:Option[String]):java.util.Map[String,String] ={
    var recordMap:java.util.Map[String,String] = new util.HashMap[String,String]()
    recordMap.put("day",day.get)
    recordMap.put("time",time.get)
    recordMap.put("dateTime",dateTime.get)
    recordMap.put("ipAddress",ipAddress.get.toString())
    val ipSeq: Seq[String] = ipAddress.get
    val ip0Str = ipSeq(0)
    val ip1Str = ipSeq(1)
    recordMap.put("ipSend","ip0Str")
    recordMap.put("ipRecv","ip1Str")
    val portSeq = port.get
    val portClient  = portSeq(0)
    val portServer = portSeq(1)
    recordMap.put("portClient",portClient)
    recordMap.put("portServer",portServer)
    recordMap.put("action",action.get)
    recordMap.put("protocol",protocol.get)
    recordMap
  }


}
