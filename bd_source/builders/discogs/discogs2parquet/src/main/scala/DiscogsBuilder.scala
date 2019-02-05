import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{ Dataset, DataFrame, Column }
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ Dataset, DataFrame, Row, Column }

//import org.apache.log4j.Logger
//import org.apache.log4j.Level

object DiscogsBuilder {

  case class TArtist(
    anv: String,
    id: Long,
    join: String,
    name: String,
    role: String,
    tracks: String)

  case class TArtRoles(
    id: Long,
    names: Seq[String],
    roles: Seq[String])

  case class TArtists(artist: Seq[TArtRoles])

  def main(args: Array[String]) {

    //  Logger.getLogger("info").setLevel(Level.OFF)

    if (args.length != 3) {
      Console.err.println("Need 3 arguments: <file releases xml in> <file artists xml in> <output dir>")
      sys.exit(1)
    }

    /* File names
     * ***********/
    val fXmlRelIn = args(0)
    val fXmlArtIn = args(1)

    val fPqtOut = if (args(2).last != '/') args(2).concat("/") else args(2)
    //val fPqtRelOut = args(2)
    //val fPqtArtOut = args(3)
    val fPqtRelOut = fPqtOut + "releases_artists.pqt"
    val fPqtArtOut = fPqtOut + "artists.pqt"

    val spark = SparkSession
      .builder()
      .appName("Discogs Builder")
      .config("spark.some.config.option", "algun-valor")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    /* Load Files
     * ************/
    def loadXml(f: String, tag: String): DataFrame = {

      println("Loadding " ++ f)

      val df = spark.read
        .format("com.databricks.spark.xml")
        .option("rowTag", tag)
        .load(f)

      return df
    }

    val dfRel = loadXml(fXmlRelIn, "release")

    println("Creating BD Releases")

    def concatArtists (arts: Seq[TArtist], extraArts: Seq[TArtist], tracksArts: Seq[Seq[TArtist]], tracksEArts: Seq[Seq[TArtist]]) : Seq[TArtist] = {
            arts ++ extraArts ++ tracksArts.flatten ++ tracksEArts.flatten
    }

    def setRoleArt(ar: Seq[Row]): Seq[TArtist] = {
      ar.map { a =>
        TArtist(a.getString(0),
          a.getLong(1),
          a.getString(2),
          a.getString(3),
          "Release Performer",
          a.getString(5))
      }
    }

    def rows2TArtists(arts: Seq[Row]): Seq[TArtist] = {
      arts.map { a =>
        TArtist(a.getString(0),
          a.getLong(1),
          a.getString(2),
          a.getString(3),
          a.getString(4),
          a.getString(5))
      }
    }

    def splitRoles(arts: Seq[TArtist]): Seq[TArtRoles] = {
      arts.map {
        case TArtist(anv, id, join, name, role, tracks) => TArtRoles(id, Seq(name),
          Option(role).getOrElse("")
            .split(",")
            .map(_.trim)
            .filter(!_.isEmpty))
      }
    }

    def filterArtists(arts: Seq[TArtRoles]) = {
      arts
        .map {
          case TArtRoles(id, name, roles) => 
            TArtRoles(id, name,
            roles.filter(r => !rolesNot.exists(rn => r.toLowerCase.contains(rn))))
        } // filter not in rolesNot
        .map {
          case TArtRoles(id, name, roles) => 
            TArtRoles(id, name,
            roles.filter(r => instruments.exists(instr => r.toLowerCase.contains(instr))))
        } // filter in instruments
        .filter(!_.roles.isEmpty)
        .filter(_.id != null)
    }

    def mergeArts(arts: Seq[TArtRoles]): Seq[TArtRoles] = {
      arts.groupBy(_.id)
        .map { case (id, tarts) => 
                  TArtRoles(id, tarts.flatMap(_.names).distinct, tarts.flatMap(_.roles).distinct) }
        .toSeq

    }

    def mergeArtists(arts: Seq[Row], extraArts: Seq[Row], tracksArts: Seq[Seq[Row]], tracksEArts: Seq[Seq[Row]]): TArtists = {
      // Remove null
      val ar = Option(arts).getOrElse(Seq())
      val ear = Option(extraArts).getOrElse(Seq())
      val tar = Option(tracksArts).getOrElse(Seq()).filter(_ != null)
      val tear = Option(tracksEArts).getOrElse(Seq()).filter(_ != null)

      val allArts = concatArtists(setRoleArt(ar), rows2TArtists(ear), tar.map(rows2TArtists _), tear.map(rows2TArtists _))
      TArtists(mergeArts(filterArtists(splitRoles(allArts))))
    }


    val mergeArtistsUDF = udf(mergeArtists _)

    def emptyArrStrIfNull(nameCol: String): Column = {

      val emptyArr = udf(() => Array.empty[String])

      when(col(nameCol).isNull, emptyArr()).otherwise(col(nameCol)).as(nameCol)
    }

    def emptyArrLongIfNull(nameCol: String): Column = {

      val emptyArr = udf(() => Array.empty[Long])

      when(col(nameCol).isNull, emptyArr()).otherwise(col(nameCol)).as(nameCol)
    }

    def flatten(xss : Seq[Seq[String]]) : Seq[String] = {
      val xss1 = Option(xss).getOrElse(Seq()).filter(_ != null)
      xss1.flatten
    }

    val flattenUDF = udf(flatten _)

    // falta parsear date in released
    val dfRelOut =
      dfRel
        .select($"_id".as("id_release"), $"master_id", $"title",
                $"released", $"country",$"genres.genre".as("genres"),
                flattenUDF($"formats.format.descriptions.description").as("formats"),
                $"artists.artist.id".as("primary_artists"),
                $"artists.artist.name".as("primary_artist_names"),
                $"labels.label._name".as("labels"), $"notes",
                mergeArtistsUDF($"artists.artist",
                          $"extraartists.artist",
                          $"tracklist.track.artists.artist",
                          $"tracklist.track.extraartists.artist"
                          ).as("artists"))
        .select($"id_release", $"master_id", $"title",
                $"released", $"country", $"genres",
                $"formats", $"primary_artists",
                $"primary_artist_names",
                $"labels", $"notes",
                explode($"artists.artist").as("artist"))
        .select($"id_release", $"master_id", $"title",
                $"released", $"country", $"genres",
                $"formats", $"primary_artists",
                $"primary_artist_names",
                $"labels", $"notes",
                $"artist.id".as("id_artist"),
                $"artist.names".as("names_artist"),
                $"artist.roles".as("roles_artist"))
        .select($"id_release", $"master_id", $"title",
                $"released", $"country", emptyArrStrIfNull("genres"),
                $"formats", emptyArrLongIfNull("primary_artists"),
                emptyArrStrIfNull("primary_artist_names"),
                emptyArrStrIfNull("labels"), $"notes",
                $"id_artist", emptyArrStrIfNull("names_artist"),
                emptyArrStrIfNull("roles_artist"))

    println("Writting " ++ fPqtRelOut)

    dfRelOut.write.mode(SaveMode.Overwrite).save(fPqtRelOut)

    val dfArt = loadXml(fXmlArtIn, "artist")

    println("Creating BD Artists")

    val dfArtOut = dfArt
      .select(
        $"id".as("id_artist"),
        $"name",
        $"realname",
        $"aliases.name._VALUE".as("aliases"),
        $"namevariations.name".as("namevariations"),
        $"groups.name._VALUE".as("groups"),
        $"members.id".as("members_id"),
        $"members.name._VALUE".as("members_name"),
        $"urls.url".as("urls"),
        $"profile")
      .select(
        $"id_artist",
        $"name",
        $"realname",
        emptyArrStrIfNull("aliases"),
        emptyArrStrIfNull("namevariations"),
        emptyArrStrIfNull("groups"),
        emptyArrLongIfNull("members_id"),
        emptyArrStrIfNull("members_name"),
        emptyArrStrIfNull("urls"),
        $"profile")

    println("Writting " ++ fPqtArtOut)

    dfArtOut.write.mode(SaveMode.Overwrite).save(fPqtArtOut)

  }

  val rolesNot =
    Seq("Produc", "Written", "Engineer", "Mixed", "Mastered", "Arranged", "Photo"
        , "Recorded", "Design", "Composed", "Lyrics", "Liner Notes", "Artwork"
        , "Photography", "Management", "Cover", "Layout", "Artwork", "Remix"
        , "Programmed", "Featuring", "Compiled", "DJ Mix", "Edit", "Illustra"
        , "Remastered", "Design", "Songwriter", "A&R", "Orchestrated", "Ensemble"
        , "Graphics", "Cut By", "Sleeve Notes", "Supervis"
        , "Coordinator", "Technician", "Recording", "Presenter"
        , "Painting", "Concept", "Band", "Text", "Recorder", "Technology"
        , "Microphone", "Translate", "Research", "Material", "Book", "Video"
        , "Image", "Stylist", "Hair", "Makeup", "Creat", "Adapted", "Film"
        , "Libretto", "Interview", "Cameraman", "Typograph", "Assistant", "Crew", "Words", "Narrator")
      .map(_.toLowerCase)

   val instruments = Seq(
        "mittlealtersackpfeife"
      , "octocontrabass"
      , "subcontrabass"
      , "metallophones"
      , "contrabassoon"
      , "sarrusophone"
      , "northumbrian"
      , "marinbaphone"
      , "gottuvadhyam"
      , "glockenspiel"
      , "fiscarmonica"
      , "clavicembelo"
      , "vladimirsky"
      , "tsuri-daiko"
      , "synthesizer"
      , "quinticlave"
      , "nyckelharpa"
      , "moodswinger"
      , "huemmelchen"
      , "heckelphone"
      , "harpsichord"
      , "folgerphone"
      , "fingerboard"
      , "electronic:"
      , "contra-alto"
      , "appalachian"
      , "vibraphone"
      , "turntables"
      , "transverse"
      , "tjelempung"
      , "teponaztli"
      , "tamburitza"
      , "tambourine"
      , "synclavier"
      , "stylophone"
      , "sousaphone"
      , "smallpipes"
      , "shakuhachi"
      , "saenghwang"
      , "sackpfeife"
      , "ophicleide"
      , "octocontra"
      , "nadaswaram"
      , "mellophone"
      , "mandocello"
      , "mando-bass"
      , "lithophone"
      , "lancashire"
      , "kwintangen"
      , "great-pipe"
      , "glasschord"
      , "flugelhorn"
      , "flageolets"
      , "doulophone"
      , "d’oliphant"
      , "didgeridoo"
      , "daraboukka"
      , "cuprophone"
      , "contrabass"
      , "concertina"
      , "clavichord"
      , "clarinette"
      , "chitaronne"
      , "cavaquinho"
      , "bullroarer"
      , "bombardino"
      , "aulochrome"
      , "arpeggione"
      , "anottolini"
      , "zhongdihu"
      , "xylorimba"
      , "xylophone"
      , "washboard"
      , "vibraharp"
      , "tsampouna"
      , "trekspill"
      , "sopranino"
      , "schweizer"
      , "saxophone"
      , "saxonette"
      , "saraswati"
      , "rainstick"
      , "palendang"
      , "organista"
      , "mridangam"
      , "mellotron"
      , "launeddas"
      , "langeleik"
      , "kulintang"
      , "kolintang"
      , "hichiriki"
      , "harmonium"
      , "harmonica"
      , "hardanger"
      , "handbells"
      , "guitarrón"
      , "gandingan"
      , "fangxiang"
      , "euphonium"
      , "duxianqin"
      , "dudelsack"
      , "doedelzak"
      , "diyingehu"
      , "darabukka"
      , "classical"
      , "chromatic"
      , "chabrette"
      , "celempung"
      , "castanets"
      , "bianzhong"
      , "bandurria"
      , "bandoneón"
      , "balalaika"
      , "accordion"
      , "accordian"
      , "zampogna"
      , "xiaodihu"
      , "warpipes"
      , "violotta"
      , "vichitra"
      , "vertical"
      , "uilleann"
      , "trompeta"
      , "tromboon"
      , "trombone"
      , "triangle"
      , "torupill"
      , "tom-toms"
      , "theremin"
      , "tenoroon"
      , "tárogató"
      , "soprillo"
      , "shinobue"
      , "shamisen"
      , "scottish"
      , "sang-auk"
      , "sallaneh"
      , "säckpipa"
      , "sackbutt"
      , "recorder"
      , "psaltery"
      , "pastoral"
      , "overtone"
      , "oliphant"
      , "muchosac"
      , "melodica"
      , "melodika"
      , "melodeon"
      , "martenot"
      , "marimbao"
      , "mangtong"
      , "mandolin"
      , "kutiyapi"
      , "kangling"
      , "japanese"
      , "istarski"
      , "hocchiku"
      , "highland"
      , "hegelong"
      , "hammered"
      , "geomungo"
      , "gayageum"
      , "electric"
      , "dumbelek"
      , "dulzaina"
      , "dulcimer"
      , "cymbalum"
      , "crumhorn"
      , "crotales"
      , "cromorne"
      , "croatian"
      , "cornetto"
      , "clarinet"
      , "cimbasso"
      , "cimbalom"
      , "chitarra"
      , "charango"
      , "carillon"
      , "calliope"
      , "cabrette"
      , "bressane"
      , "bouzouki"
      , "bordonua"
      , "bombarde"
      , "bianqing"
      , "berimbao"
      , "battebte"
      , "baritone"
      , "bagpipes"
      , "babendil"
      , "aru-ding"
      , "archlute"
      , "angklung"
      , "acoustic"
      , "yazheng"
      , "whistle"
      , "washtub"
      , "washint"
      , "volinka"
      , "vihuela"
      , "ukulele"
      , "tumpong"
      , "tubular"
      , "trumpet"
      , "tonette"
      , "tom-tom"
      , "timpani"
      , "theorbo"
      , "tan-tan"
      , "tamlang"
      , "tambour"
      , "talking"
      , "tagutok"
      , "string"
      , "soprano"
      , "singing"
      , "shekere"
      , "serunai"
      , "serpent"
      , "saxhorn"
      , "sarunay"
      , "sarunai"
      , "saronay"
      , "saronai"
      , "sanxian"
      , "sampler"
      , "ryuteki"
      , "ratchet"
      , "pistons"
      , "piccolo"
      , "paixiao"
      , "octavin"
      , "octapad"
      , "octaban"
      , "ocarina"
      , "natural"
      , "musical"
      , "musette"
      , "marimba"
      , "mandola"
      , "malimba"
      , "machine"
      , "lusheng"
      , "luntang"
      , "lowland"
      , "kwa-yen"
      , "komungo"
      , "knatele"
      , "kadlong"
      , "hunting"
      , "horagai"
      , "hélicon"
      , "guzheng"
      , "gambang"
      , "english"
      , "dulcian"
      , "darvyra"
      , "darbuka"
      , "dankiyo"
      , "d’amour"
      , "d’amore"
      , "dabakan"
      , "cymbals"
      , "cowbell"
      , "cornish"
      , "cornett"
      , "clapped"
      , "cittern"
      , "chapman"
      , "celesta"
      , "bodhrán"
      , "bazooka"
      , "bassoon"
      , "baryton"
      , "baroque"
      , "bandola"
      , "balafon"
      , "bagpipe"
      , "baglama"
      , "anglais"
      , "alphorn"
      , "aeolian"
      , "zufalo"
      , "zonghu"
      , "zither"
      , "zhuihu"
      , "wooden"
      , "willow"
      , "violin"
      , "vielle"
      , "valiha"
      , "tromba"
      , "tiniok"
      , "timple"
      , "thavil"
      , "taphon"
      , "tanbur"
      , "suling"
      , "sralai"
      , "spoons"
      , "splash"
      , "sleigh"
      , "shofar"
      , "santur"
      , "sampho"
      , "rozhok"
      , "rattle"
      , "racket"
      , "pulalu"
      , "octave"
      , "naqara"
      , "mizwad"
      , "mijwiz"
      , "marina"
      , "maraca"
      , "magyar"
      , "maguhu"
      , "lirone"
      , "leiqin"
      , "laruan"
      , "lambeg"
      , "kubing"
      , "koziol"
      , "kissar"
      , "khalam"
      , "kettle"
      , "jinghu"
      , "jiaohu"
      , "janggu"
      , "huluhu"
      , "hooked"
      , "hi-hat"
      , "guitar"
      , "ghatam"
      , "gender"
      , "gemecs"
      , "french"
      , "finger"
      , "fiddle"
      , "erxian"
      , "ektara"
      , "dubreq"
      , "double"
      , "dotara"
      , "dombak"
      , "djembe"
      , "dholak"
      , "da’uli"
      , "dadihu"
      , "cuatro"
      , "cornet"
      , "cimpoi"
      , "chimes"
      , "chi’in"
      , "chenda"
      , "centre"
      , "caxixi"
      , "cavaco"
      , "caccia"
      , "button"
      , "border"
      , "bodega"
      , "biniou"
      , "bifora"
      , "basset"
      , "barbat"
      , "bamboo"
      , "babone"
      , "arghul"
      , "alpine"
      , "alboka"
      , "ajaeng"
      , "zurna"
      , "yayli"
      , "xalam"
      , "welsh"
      , "viola"
      , "veuze"
      , "veena"
      , "tulum"
      , "thumb"
      , "tenor"
      , "taiko"
      , "tabla"
      , "suona"
      , "stick"
      , "steel"
      , "sorna"
      , "snare"
      , "slide"
      , "sitar"
      , "siren"
      , "sheng"
      , "shawm"
      , "setar"
      , "saung"
      , "sanza"
      , "sabar"
      , "rudra"
      , "rebec"
      , "rebab"
      , "ranat"
      , "rajao"
      , "raita"
      , "quena"
      , "qanun"
      , "putao"
      , "pipes"
      , "piano"
      , "organ"
      , "ondes"
      , "mouth"
      , "morin"
      , "mohan"
      , "mbira"
      , "leier"
      , "lasso"
      , "koudi"
      , "kokyu"
      , "khuur"
      , "khloy"
      , "khene"
      , "keyed"
      , "kaval"
      , "kagul"
      , "jew’s"
      , "irish"
      , "hurdy"
      , "huqin"
      , "hsaio"
      , "hosho"
      , "hands"
      , "gusli"
      , "gusle"
      , "gusla"
      , "gurdy"
      , "guqin"
      , "guiro"
      , "gugin"
      , "great"
      , "glass"
      , "gaohu"
      , "ganza"
      , "gamba"
      , "gajdy"
      , "gaita"
      , "gaida"
      , "flute"
      , "dutar"
      , "domra"
      , "denis"
      , "daiko"
      , "cuíca"
      , "crwth"
      , "crash"
      , "conga"
      , "conch"
      , "cigar"
      , "ching"
      , "china"
      , "ch’in"
      , "chest"
      , "cello"
      , "bugle"
      , "brian"
      , "bongo"
      , "bells"
      , "bayan"
      , "basse"
      , "banjo"
      , "banhu"
      , "array"
      , "anche"
      , "ahoko"
      , "agung"
      , "agong"
      , "zink"
      , "zaqq"
      , "yueh"
      , "yehu"
      , "yang"
      , "xiao"
      , "wind"
      , "whip"
      , "tuhu"
      , "tuba"
      , "tres"
      , "thum"
      , "slit"
      , "sihu"
      , "ruan"
      , "riqq"
      , "ride"
      , "reed"
      , "piva"
      , "pipe"
      , "pipa"
      , "oboe"
      , "nose"
      , "lyre"
      , "lute"
      , "koto"
      , "kora"
      , "khol"
      , "khim"
      , "kayo"
      , "inci"
      , "igil"
      , "huur"
      , "horn"
      , "harp"
      , "hang"
      , "guan"
      , "gong"
      , "gehu"
      , "erhu"
      , "duff"
      , "dudy"
      , "duda"
      , "duct"
      , "drum"
      , "d’or"
      , "dihu"
      , "dhol"
      , "dahu"
      , "cour"
      , "chin"
      , "ceng"
      , "bowl"
      , "boru"
      , "boha"
      , "bock"
      , "biwa"
      , "bell"
      , "bawu"
      , "bass"
      , "arpa"
      , "voice"
      , "choir"
      , "vocal"
      , "sing"
      , "performer"
      , "synth"
      , "kempul"
      , "keyboard"
      , "percussion"
      , "chorus"
      , "soloist"
      , "musician"
      , "scratch"
      , "instrument"
      , "sleeve"
      , "accompanied"
      , "guest"
      , "handclaps"
      , "clavinet"
      , "brass"
      , "timbales"
      , "shaker"
      , "cymbal"
      , "loops"
      , "dobro"
      , "vocoder"
      , "bodhr"
      , "kalimba"
      , "tambura"
      , "kazoo"
      , "claves"
      , "ebow"
      , "e-bow"
      , "omnichord"
      , "lyricon"
      )
}
