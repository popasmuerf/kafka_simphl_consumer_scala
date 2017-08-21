import java.util
import java.util.HashMap
import java.util.Map;


/**
  * Created by mikeyb on 7/31/17.
  *
  * mdtsdb-1.fractal:8080
    mdtsdb-2.fractal:8080
    mdtsdb-3.fractal:8080
    mdtsdb-4.fractal:8080
    mdtsdb-5.fractal:8080
  */
object PFSenseParser {
  def parseRecordToStr(record:String):Option[String]= {
    var lineNumber = 1
    val dayPttrn = "\\w{1,3}\\s+\\d".r //day pattern
    val timePttrn = "\\d{2}:\\d{2}:\\d{2}".r //time pattern
    val dateTimePttrn = "\\w{3}\\s+\\d{1,2}\\s+\\d{2}:\\d{2}:\\d{2}".r //nix -- DayTime
    val ipAddrPttrn = "\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}".r //ipAddr
    val portPttrn = "\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\,\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\,(6553[0-5]|655[0-2][0-9]\\d|65[0-4](\\d){2}|6[0-4](\\d){3}|[1-5](\\d){4}|[1-9](\\d){0,3}){2}\\,\\d{1,5}\\d{1,5}".r
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
  def parseRecordToMap(record:String):Option[java.util.Map[String,String]] = {
    var lineNumber = 1
    val dayPttrn = "\\w{1,3}\\s+\\d".r //day pattern
    val timePttrn = "\\d{2}:\\d{2}:\\d{2}".r //time pattern
    val dateTimePttrn = "\\w{3}\\s+\\d{1,2}\\s+\\d{2}:\\d{2}:\\d{2}".r //nix -- DayTime
    val ipAddrPttrn = "\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}".r //ipAddr
    val portPttrn = "\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\,\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\,(6553[0-5]|655[0-2][0-9]\\d|65[0-4](\\d){2}|6[0-4](\\d){3}|[1-5](\\d){4}|[1-9](\\d){0,3}){2}\\,\\d{1,5}\\d{1,5}".r
    val actionPttrn = "pass|block|drop|dropped".r
    val protocolPttrn = "tcp|udp|igmp|icmp".r



    val day: Option[String] = dayPttrn.findFirstIn(record)
    val time: Option[String] = timePttrn.findFirstIn(record)
    val dateTime: Option[String] = dateTimePttrn.findFirstIn(record)
    var ipAddress = Option(ipAddrPttrn.findAllIn(record).toList)
    //val port: Option[List[String]] = Option(portPttrn.findAllIn(record).toList)
    val port: Option[String] = portPttrn.findFirstIn(record)
    val action: Option[String] = actionPttrn.findFirstIn(record)
    val protocol: Option[String] = protocolPttrn.findFirstIn(record)
    val recordMap: Option[util.Map[String, String]] = getParsedRecMap(day, time, dateTime, ipAddress, port, action, protocol)
    if (recordMap != None) {
      println(recordMap.get.toString)
      return recordMap
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
  def getParsedRecMap(day:Option[String],time:Option[String],dateTime:Option[String],ipAddress:Option[List[String]],port:Option[String],action:Option[String],protocol:Option[String]):Option[java.util.Map[String,String]] ={
    var recordMap:java.util.Map[String,String] = new util.HashMap[String,String]()
    day.map( _ => recordMap.put("day", day.get ))
    time.map( _  => recordMap.put("time", time.get ))
    dateTime.map( _ =>  recordMap.put("dateTime", dateTime.get ))
    ipAddress.foreach( _ => {
      val ipList = ipAddress.get
      if(ipList.length < 2 ){return None}
      recordMap.put("ipSend",ipList(0))
      recordMap.put("ipRecv",ipList(1))
    })
    /*
    port.foreach(_ => {
      val portList = port.get
      recordMap.put("portSend",portList(portList.length - 3 ))
      recordMap.put("ipRecv",portList(portList.length - 2 ))
    })
    */
    port.map( _ => {
      val strLst: Array[String] = port.get.split(",")
      val clientPort: String = strLst(2).replaceAll("""(?m)\s+$""", "")
      val serverPort: String = strLst(3).replaceAll("""(?m)\s+$""", "")
      recordMap.put("clientPort",clientPort)
      recordMap.put("serverPort",serverPort)
    })
    action.map( _ =>   recordMap.put("action", action.get ))
    protocol.map( _  => recordMap.put("protocol", protocol.get))
    Option(recordMap)
  }
}
