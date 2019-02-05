import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Dataset,DataFrame,Column}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
//import org.apache.log4j.Logger
//import org.apache.log4j.Level

object ToParquet {

	type Entity = String
	type Verb = String
	type Type = String

	case class Prop(subj: Entity, verb: Verb, obj: Entity, context: String)
	case class EntType(ent: Entity, typ: Type, context: String)


  def main(args: Array[String]) {

  //  Logger.getLogger("info").setLevel(Level.OFF)

    if (args.length != 2) {
                            Console.err.println("Need two arguments: <map.tql> <type.tql>")
                            sys.exit(1)
                          }

	/* File names
	 * ***********/
    val fMaps = args(0)
    val fTypes = args(1)

    val fExt = "pqt"
    
	val fPropsPqt = args(0) + "." + fExt

	val fTypesPqt = args(1) + "." + fExt

    val spark = SparkSession
            .builder()
            .appName("tqlTpParquet")
            .config("spark.some.config.option", "algun-valor")
                .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    import spark.implicits._

	/* Load Files
	 * ************/
	def loadProp(fPath : String) : Dataset[Prop] = {
		var re = """<([^>]+)>\s+<([^>]+)>\s+<([^>]+)>\s+<([^>]+)>\s*\.\s*""".r
//		var re = """<(\S+)>\s+<(\S+)>\s+<(\S+)>\s+<(\S+)>\s*\.\s*""".r
//		var re = """<(\S+)>\s+<(\S+)>\s+(.+)\.\s*""".r
		val ds = spark.read.text(fPath).as[String]
		ds.map(_.trim)
		  .filter(! _.isEmpty)
		  .filter(_.head == '<')
		  .map( _ match { case re(s,v,o) => Prop(s, v, o.trim) } )
		  .distinct() 
	}

	def loadType(fPath : String)  : Dataset[EntType] = {

	//    def stripAngles(s: String) = s.stripPrefix("<").stripSuffix(">")

		def getSuffix(s: String) =
			"""[^\/\>]+>$""".r
			.findFirstIn(s).get
			.stripSuffix(">")
		
		val ds = spark.read.text(fPath).as[String]
		ds.map(_.trim)
		  .filter(! _.isEmpty)
		  .filter(_.head == '<')
		  .map(_.split("""\s+"""))
		  .map(t => EntType(t(0), getSuffix(t(2))))
		  .distinct() // may be duplicates
	}

	val props : Dataset[Prop] = loadProp(fMaps)
	val types : Dataset[EntType] = loadType(fTypes)

    props.write.mode(SaveMode.Overwrite).save(fPropsPqt)
    types.write.mode(SaveMode.Overwrite).save(fTypesPqt)

  }
}
