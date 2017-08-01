/**
  * Created by mikeyb on 7/31/17.
  */
object PFSenseParser {
  def parseRecord(record:String):Option[String]= {
    /*
    val dayPttrn = "(^\\w{3}\\s{1}\\d{1,2})".r  //day pattern
    val timePttrn = "\\d{2}:\\d{2}:\\d{2}".r  //time pattern
    val dateTimePttrn = "\\w{3}\\s{1}\\d{1,2}\\s\\d{2}:\\d{2}:\\d{2}".r  //nix -- DayTime
    val ipAddrPttrn = "\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}".r  //ipAddr
    val socketPttrn = "\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.(6553[0-5]|655[0-2][0-9]\\d|65[0-4](\\d){2}|6[0-4](\\d){3}|[1-5](\\d){4}|[1-9](\\d){0,3})".r
    val actionPttrn = "pass|block".r
    val protocolPttrn = "TCP|UDP|IGMP|ICMP".r
    */
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
    day match{
      case None => {return None}
      case _ => recordStr = day.get
    }
    time match{
      case None => {return None}
      case _ => {recordStr += " " ; recordStr += time.get}
    }
    dateTime match{
      case None => {return None}
      case _ => {recordStr += " " ; recordStr += dateTime.get}
    }
    ipAddress match{
      case None => {return None}
      case _ => { val ipList = ipAddress.get
        if(ipList.length < 2 ){return None}
        recordStr += " "
        recordStr += ipList(0)
        recordStr += " "
        recordStr += ipList(1)
      }
    }
    port match{
      case None => {return None}
      case _ => {
        val portList = port.get
        recordStr += " "
        recordStr += portList(portList.length - 3 )
        recordStr += " "
        recordStr += portList(portList.length - 2 )
      }
    }
    action match{
      case None => {return None}
      case _ => {recordStr += " " ; recordStr += action.get}
    }
    protocol match{
      case None => {return None}
      case _ => {recordStr += " " ; recordStr += protocol.get}
    }

    return Option(recordStr)
  }


}
