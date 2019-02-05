import sys.process._
import java.io._
import java.nio.file.{Paths, Files}
import java.text.Normalizer



class UpdateChecker(val fileDir: String, val ed: Int, val sparkDir: String, val bdTargetPath: String){

  // TODO: pasar dir y nombre de scripts por argumento?
  val dir = new java.io.File(".").getCanonicalPath + "/status_update/" + ed.toString + "/"

  def checkIfNeedUpdate(): Boolean = {
    val outputDir = dir
    val sparkComm = Seq("./runChecker.sh", sparkDir, fileDir, bdTargetPath, outputDir)

    println("Chequeando si es necesario actualizar la BD...")
    val nameMatch = sparkComm.!!
    println("Listo!")


    val csv = dir + "update_status.csv"
    val bufferedSource = io.Source.fromFile(csv)
    val lines = bufferedSource.getLines.drop(1)
    var art_list : Array[Array[String]]= Array.empty

    val changes = lines.map(line => line.split(",").map(_.trim)).toSeq.map(l => (l(0),l(6).toBoolean))

    if (changes.map(_._2).contains(true)){

      println("==========================================================================")
      println("Se encontraron cambios en los siguientes artistas")
      println("\n")
      
      for ((name, updated) <- changes if updated) {

        //println(s"${cols(0)} \t |${cols(1)} | \t |${cols(2)} \t | \t |${cols(3)} \t |${cols(4)}")
        println(s"- ${name} \t")
        
      }
      println("\n")
    }
    else {
      println("No es necesario actualizar la BD Target!.")
    }

    return changes.map(_._2).contains(true)
    
  }


}

class DbUpdater(val fileDir: String, val ed: Int, val sparkDir: String, val bdTargetPath: String, val tmp2Dir: String, val updatedDBDir: String){
  
  // TODO: pasar dir y nombre de scripts por argumento?
  val dir = new java.io.File(".").getCanonicalPath + "/festival_ed_csvs/" + ed.toString + "/"
 
  /** runs the spark script to update bd-target and write csv with results */
  def updateBD(){
    
    val outputDir = dir
    val sparkComm = Seq("./updateDB.sh", sparkDir, fileDir, outputDir, bdTargetPath, tmp2Dir, updatedDBDir)

    println("Actualizando db...")
    val nameMatch = sparkComm.!!
    println("Listo!")
  }
}

class MusicbrainzFinder(val artist: String, val ed: Int, val sparkDir: String, val tmp2Dir: String){
  
  // TODO: pasar dir y nombre de scripts por argumento?
  val dir = new java.io.File(".").getCanonicalPath + "/csvs/musicbrainz/" + ed.toString + "/"
  
  
  /** runs the spark script to search an artist on parquet */
  def searchArtists(){
    
    val outputDir = dir
    val sparkComm = Seq("./searchMusicbrainz.sh", sparkDir, outputDir, tmp2Dir) ++ Seq(artist)

    println("Buscando " + artist.replaceAll("_", " ") + " en musicbrainz...")
    val nameMatch = sparkComm.!!
    println("Listo!")
  }
  
  /** reads artist selection */
  def readOption(options: Seq[Int]): Int = {
    
    println("Ingrese el numero del artista deseado o -1 si no se encuentra en la lista): ")
    try {
      var option = scala.io.StdIn.readInt()
      if (options.contains(option) || option == -1){
        println("confirmar selección [" + option + "] (y/n)")
        scala.io.StdIn.readLine() match {
          case "y" => return option
          case _ => readOption(options)
        }
      }
      else{
        println("la opción ingresada no es valida!")
        readOption(options)
      }
    }
    catch {
       case e : NumberFormatException => println("la opción ingresada no es valida!.")
       readOption(options)
    }
  }
   
  /** selects the correct artist, returns ID */
  def getID(): String = {
    
    def getListOfFiles(dir: String):List[File] = {
      val d = new File(dir)
      if (d.exists && d.isDirectory) {
          d.listFiles.filter(_.isFile).toList
      } else {
          List[File]()
      }
    }
    
    val list = getListOfFiles(dir).filter(f => f.getName.endsWith(".csv")) 
    
    var artistID = "no info"
    
    if(list.nonEmpty){
      val bufferedSource = io.Source.fromFile(list(0))
      val lines = bufferedSource.getLines.drop(1)
      var art_list : Array[Array[String]]= Array.empty
  
      println("==========================================================================")
      println("Resultados para: " + list(0).getName.dropRight(4).replaceAll("_", " "))
      for ((line, i) <- lines.zipWithIndex) {
        val cols = line.split(",").map(_.trim)
        
        println("["+ i.toString + "]\t" + s"${cols(0)} \t |${cols(1)}")
        art_list = art_list :+ cols
        
      }
      if (art_list.length > 0){
        val opt = readOption(0 to art_list.length)  
        artistID = if (opt != -1) art_list(opt)(2) else artistID
      }
      else{
        println("sin resultados!")
      }
      bufferedSource.close
    }
    else{
      println("no se encontró csv!!")
    }
    artistID
  }
}

class WikidataFinder(val artist: String, val ed: Int, val sparkDir: String, val tmp2Dir: String){
  
  // TODO: pasar dir y nombre de scripts por argumento?
  val dir = new java.io.File(".").getCanonicalPath + "/csvs/wikidata/" + ed.toString + "/"
  
  
  /** runs the spark script to search an artist on parquet */
  def searchArtists(){
    
    val outputDir = dir
    val sparkComm = Seq("./searchWikidata.sh", sparkDir, outputDir, tmp2Dir) ++ Seq(artist)
    //println(sparkComm)

    println("Buscando " + artist.replaceAll("_", " ") + " en wikidata...")
    val nameMatch = sparkComm.!!
    println("Listo!")
  }
  
  /** reads artist selection */
  def readOption(options: Seq[Int]): Int = {
    
    println("Ingrese el numero del artista deseado o -1 si no se encuentra en la lista): ")
    try {
      var option = scala.io.StdIn.readInt()
      if (options.contains(option) || option == -1){
        println("confirmar selección [" + option + "] (y/n)")
        scala.io.StdIn.readLine() match {
          case "y" => return option
          case _ => readOption(options)
        }
      }
      else{
        println("la opción ingresada no es valida!")
        readOption(options)
      }
    }
    catch {
       case e : NumberFormatException => println("la opción ingresada no es valida!.")
       readOption(options)
    }
  }


  /** selects the correct artist, returns ID */
  def getID(): String = {
    
    def getListOfFiles(dir: String):List[File] = {
      val d = new File(dir)
      if (d.exists && d.isDirectory) {
          d.listFiles.filter(_.isFile).toList
      } else {
          List[File]()
      }
    }
    
    val list = getListOfFiles(dir).filter(f => f.getName.endsWith(".csv"))
    
    var artistID = "no info"
    
    if(list.nonEmpty){
      val bufferedSource = io.Source.fromFile(list(0))
      val lines = bufferedSource.getLines.drop(1)
      var art_list : Array[Array[String]]= Array.empty
  
      println("==========================================================================")
      println("Resultados para: " + list(0).getName.dropRight(4).replaceAll("_", " "))
      for ((line, i) <- lines.zipWithIndex) {
        val cols = line.split(",").map(_.trim)
        
        println("["+ i.toString + "]\t" + s"${cols(0)} \t |${cols(1)}")
        art_list = art_list :+ cols
        
      }
      if (art_list.length > 0){
        val opt = readOption(0 to art_list.length)
        artistID = if (opt != -1) art_list(opt)(2) else artistID
      }
      else{
        println("sin resultados!")
      }
      bufferedSource.close
    }
    else{
      println("no se encontró csv!!")
    }
    artistID
  }
}

/** Main source artist finder (Discogs) */
class DiscogsFinder(val artists: Seq[String], val ed: Int, val sparkDir: String, val tmp2Dir: String,
  val targetDir: String, val updatedDBDir: String, val update: Boolean) {
   
  // TODO: pasar dir y nombre de scripts por argumento?
  val dir = new java.io.File(".").getCanonicalPath + "/csvs/discogs/" + ed.toString + "/"
  
  
  /** runs the spark script to search an artist on parquet */
  def searchArtists(){
    
    val outputDir = dir
    val sparkComm = Seq("./searchDiscogs.sh", sparkDir, outputDir, tmp2Dir) ++ artists
    //println(sparkComm)

    println("Buscando artistas en discogs...")
    val nameMatch = sparkComm.!!
    println("Listo!")
  }
  
  
  def writeToFile(lines: Seq[(String, String, String)]){
   
    val input_dir = new java.io.File(".").getCanonicalPath
    val file = input_dir + "/tmp_tsv/artists.tsv"
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
    for (line <- lines) {
      writer.write(line._1 + "\t" + line._2 + "\t" + line._3  + "\n")  // however you want to format it
    }
    writer.close()
  }

  /** writes csv of ids*/
  def writeCSV(lines: Seq[(String, String)]){
   
    val input_dir = new java.io.File(".").getCanonicalPath
    val file = input_dir + "/guests.csv"
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))

    writer.write("id,name\n")
    for (line <- lines) {
      writer.write(line._2 + "," + line._1 + "\n")  // however you want to format it
    }
    writer.close()
  }

  /** reads artist selection */
  def readOption(options: Seq[Int]): Int = {
    
    println("Ingrese el numero del artista deseado o -1 si no se encuentra en la lista): ")
    try {
      var option = scala.io.StdIn.readInt()
      if (options.contains(option) || option == -1){
        println("confirmar selección [" + option + "] (y/n)")
        scala.io.StdIn.readLine() match {
          case "y" => return option
          case _ => readOption(options)
        }
      }
      else{
        println("la opción ingresada no es valida!")
        readOption(options)
      }
    }
    catch {
       case e : NumberFormatException => println("la opción ingresada no es valida!.")
       readOption(options)
    }
  }
  
  /** selects the correct artist, and then updates DB with other sources if incomplete */
  def selectAndUpdate(){
    
    def getListOfFiles(dir: String):List[File] = {
      val d = new File(dir)
      if (d.exists && d.isDirectory) {
          d.listFiles.filter(_.isFile).toList
      } else {
          List[File]()
      }
    }

    def normalize(n: String): String = {
       
      val normalized = Normalizer.normalize(n, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "")
      normalized.trim().replaceAll("\\s+", "_")

    }

    val list = getListOfFiles(dir).filter(f => f.getName.endsWith(".csv")) 
    
    var arrayOfTup : Array[(String, String, String)] = Array.empty

    var nameIDTup: Array[(String, String)] = Array.empty

    var update_flag = false
    
    for (csv <- list){

      val bufferedSource = io.Source.fromFile(csv)
      val lines = bufferedSource.getLines.drop(1)
      var art_list : Array[Array[String]]= Array.empty
     
      val entry_name = csv.getName.dropRight(4).replaceAll("_", " ")

      println("==========================================================================")
      println("Resultados para: " + entry_name)
      for ((line, i) <- lines.zipWithIndex) {
        val cols = line.split(",").map(_.trim)
        
        println("["+ i.toString + "]\t" + s"${cols(0)} \t |${cols(1)}")
        art_list = art_list :+ cols
        
      }
      
      var ids : Array[Array[String]]= Array.empty
      
      if (art_list.length > 0){
        val opt = readOption(0 to art_list.length)
        if (opt != -1){
          val artistName = normalize(art_list(opt)(0))
          val artistID = art_list(opt)(2)
          
          val mbID = if (opt != -1){
                        val mbFinder = new MusicbrainzFinder(entry_name.replaceAll(" ", "_"), ed, sparkDir, tmp2Dir)
                        mbFinder.searchArtists()
                        mbFinder.getID()
                       } else "no info"
                        
          val wikiID = if (opt != -1){
                        val wikiFinder = new WikidataFinder(entry_name.replaceAll(" ", "_"), ed, sparkDir, tmp2Dir)
                        wikiFinder.searchArtists()
                        wikiFinder.getID()
                       } else "no info"
                         
          arrayOfTup = arrayOfTup :+ (artistID, mbID, wikiID)

          nameIDTup = nameIDTup :+ (artistName.replaceAll(" ", "_"), artistID)

          update_flag = true
        }
        else println("sin resultados!")
      }
      else{
        println("sin resultados!")
      }
      bufferedSource.close

    }
    writeToFile(arrayOfTup)

    println("Escribiendo CSV de artistas invitados...")
    writeCSV(nameIDTup)

    val needUpdate = if(update_flag && update){
      val input_dir = new java.io.File(".").getCanonicalPath
      val checker = new UpdateChecker(input_dir + "/tmp_tsv/artists.tsv", ed, sparkDir, targetDir)
      checker.checkIfNeedUpdate()
    }else{
      false
    }

    //Only updates if artists were found and is needed!
    if (update_flag && update && needUpdate){
      val input_dir = new java.io.File(".").getCanonicalPath
      val updater = new DbUpdater(input_dir + "/tmp_tsv/artists.tsv", ed, sparkDir, targetDir, tmp2Dir, updatedDBDir)
      updater.updateBD()
    }
  }
}

class YearReader(){

  def read(): Int = {

    println("Ingrese el año de la edición del festival:")
    try {
      var year = scala.io.StdIn.readInt()
      println("Año ingresado:")
      println(year)
      println("es correcto (y/n)?")
      
      val yearDir = new java.io.File(".").getCanonicalPath + "/festival_ed_csvs/" + year.toString + "/"
      
      scala.io.StdIn.readLine() match {
        case "y" => {
          if (Files.exists(Paths.get(yearDir))){
            println("Ya existe una búsqueda asociada a ese año, desea sobreescribirla? (y/n)")
            scala.io.StdIn.readLine() match {
              case "y" => return year
              case _ => {
                println("terminado")
                System.exit(1)
              }
            }
          }
          return year
        }
        case _ => read()
      
      }
    }
    catch {
       case e : NumberFormatException => println("el año ingresado no tiene el formato esperado (YYYY)!.")
       read()
    }
  }  
}

class ArtistReader(){  
  
  def read() : Seq[String] = {

    def normalize(n: String): String = {
       
      val normalized = Normalizer.normalize(n, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "")
      normalized.trim().replaceAll("\\s+", "_")

    }
    
    println("Ingrese nombres de artistas separados por coma:")

    var artists = scala.io.StdIn.readLine().split(",").map(_.trim)
    println("usted introdujo los siguientes artistas:")
    artists.map("- " ++ _).foreach(println)
    println("es correcto (y/n)?")
    scala.io.StdIn.readLine() match {
      case "y" => {
        return artists.filter(_.nonEmpty).map(n => normalize(n))
      }
      case _ => read()
    }
  }
}

object Finder extends App {
  
  if (args.length < 3){
    println("la cantidad de argumentos no es la correcta.")
    println("modo de uso: scala Finder <spark bin dir> <parquets tmp2 dir> <bd_target dir>")
    System.exit(0)
  }
  
  // spark binaries dir
  val sparkPath = args(0)
  // parquets tmp2 dir
  val parquetsPath = args(1)
  // bd target dir
  val bdTargetPath = args(2)
  // updated bd_target dir
  val updatedBDTargetDir = args(3)
  // update bd_target
  val updateBDTarget = args(4).toInt


  // setear a true para habilitar actualización de BD Target
  val activate_BD_update = if (updateBDTarget == 1) { true } else { false }
  
  val yReader = new YearReader()
  val year = yReader.read()
  
  val aReader = new ArtistReader()
  val artists = aReader.read()
  
  val discogs = new DiscogsFinder(artists, year, sparkPath, parquetsPath, bdTargetPath, updatedBDTargetDir, activate_BD_update)
  discogs.searchArtists()
  discogs.selectAndUpdate()

}