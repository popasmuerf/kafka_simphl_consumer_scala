import spray.json.DefaultJsonProtocol
/**
  * Created by mdb on 8/4/17.
  */
class RecordJsonProtocol extends DefaultJsonProtocol  {
  implicit val recordFormat = jsonFormat9(Record)
}
