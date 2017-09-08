import com.mongodb._
import org.bson.types.ObjectId

/**
  * Created by mdb on 9/8/17.
  */
object MongoUtils {
  def keysFromMongo(): java.util.Map[String,String] ={
    var _id:ObjectId = null
    var name:String = null
    var clAdmKey:String = null
    var clAdmSecretKey:String = null
    var swlAppKey:String = null
    var swlSecretKey:String = null
    val bsonObjId = new ObjectId("598cc1ab60481f26d6b8a2f6") //this is the documentID.  We should put this into a config file
    val mongoClient = new MongoClient("mongodb.fractal", 27017)
    val database:DB = mongoClient.getDB("conmon-db")
    val collection: DBCollection =database.getCollection("network_syslog")
    var iterDoc: DBCursor = collection.find()
    var keyMap: java.util.Map[String, String] =  new java.util.HashMap[String,String]()
    while(iterDoc.hasNext()){
      val obj:BasicDBObject = iterDoc.next().asInstanceOf[BasicDBObject]
      _id =  obj.get("_id").asInstanceOf[ObjectId] ; println("_id from mongoDoc : " + _id.toString()) ; keyMap.put("_id",_id.toString)
      name = obj.get("name").asInstanceOf[String].trim() ; println("name from mongoDoc : " + name.toString()) ; keyMap.put("name",name)
      clAdmKey = obj.get("clAdmKey").asInstanceOf[String].trim() ; println("clAdmKey from mongoDoc : " + clAdmKey.toString()) ; keyMap.put("clAdmKey",clAdmKey)
      clAdmSecretKey = obj.get("clAdmSecretKey").asInstanceOf[String].trim()  ; println("clAdmSecretKey from mongoDoc : " + clAdmSecretKey.toString()) ; keyMap.put("clAdmSecretKey",clAdmSecretKey)
      swlAppKey = obj.get("swlAppKey").asInstanceOf[String].trim() ; println("swlAppKey from mongoDoc : " + swlAppKey.toString()) ; keyMap.put("swlAppKey",swlAppKey)
      swlSecretKey = obj.get("swlSecretKey").asInstanceOf[String].trim() ; println("swlSecretKey from mongoDoc : " + swlSecretKey.toString()) ; keyMap.put("swlSecretKey",swlSecretKey)
    }
    mongoClient.close()
    return keyMap
  }
}
