import spray.json.DefaultJsonProtocol
/**
  * Created by mdb on 8/4/17.
  */
case class Record(_date:String,
                  _host:String,
                  _sendAddr:String,
                  _recvAddr:String,
                  _port:String,
                  _protocol:String,
                  _match:String,
                  _pass:String,
                  _bytes:String)


