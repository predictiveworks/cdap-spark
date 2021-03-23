package de.kp.works.dl.video

import com.intel.analytics.bigdl.opencv.OpenCV
import com.intel.analytics.zoo.common.NNContext

import org.opencv.core.{Mat, Size}
import org.opencv.imgproc.Imgproc

import org.opencv.videoio.VideoCapture

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

object VideoUtils {
  
  OpenCV.isOpenCVLoaded
  
  private val schema = StructType(Array(
      /*
       * The origin of the video frame is defined as a 
       * concatenation of the file name and the milli 
       * seconds with respect to the start of the video
       */
      StructField("origin", StringType, true),
      /*
       * This is the image height of the frame
       */
      StructField("height", IntegerType, false),
      /*
       * This is the image width of the frame
       */
      StructField("width", IntegerType, false),
      /*
       * The number of channels of the frame
       */
      StructField("nChannels", IntegerType, false),
      /*
       * The OpenCV compatible type of the image
       * encoded as Integer: CV_8UC3, CV_8UC1
       */
      StructField("mode", IntegerType, false),
      /*
       * The bytes of the image (frame) in row-wise BGR
       */
      StructField("data", BinaryType, false)      
  ))
  /**
   * @param resizeH 	Height after resize, by default is -1 which will not resize the image
   * @param resizeW 	Width after resize, by default is -1 which will not resize the image
   * @param start 		Time in seconds after the video started to start clip
   * @param end   		Time in seconds after the videdo start to stop clip
   * 
   */
  def video2DF(
      sc:SparkContext, path:String, partitions:Int = 4, 
      resizeH:Int = -1, resizeW:Int = -1, begin:Int = -1, end:Int = -1):DataFrame = {

    /* Load video */
    val video = new VideoCapture(path)
    
    /* Prepare `origin` and result */
    val origin = path.split("\\/").last
    val rows = ArrayBuffer.empty[Row]

    /* Build control flags */

    val clip = begin != -1 && end != -1
    val resize = resizeH != -1 && resizeW != -1

    var running = true
    val frame = new Mat()

    val now = System.currentTimeMillis
    var currTime = now
    
    if (clip) {
      /*
       * Start frame retrieve at startTime` and end
       * at `endTime`
       */
      val startTime = now + (begin * 1000)
      val endTime   = now + (end * 1000)

      while (video.isOpened && running) {
  
        currTime = System.currentTimeMillis
        running = currTime < endTime
  
        if (currTime >= startTime) {
          
          /* Read the next frame */
          if (video.read(frame)) {
            
            /* Apply resizing */
            val mat = if (resize) resizeMat(frame, resizeH, resizeW) else frame
           
            /* Append frame to rows */
            rows += toRow(s"${origin}:${currTime}", mat)
            
          }
          
        }
  
      }
    
    } else {
      /*
       * Process entire frames of the provided
       * video
       */
      while (video.isOpened) {
        
        currTime = System.currentTimeMillis
        
        /* Read the next frame */
        if (video.read(frame)) {
          
          /* Apply resizing */
          val mat = if (resize) resizeMat(frame, resizeH, resizeW) else frame

          /* Append frame to rows */
          rows += toRow(s"${origin}:${currTime}", mat)
          
        }
  
      }
      
    }

    video.release

    val sqlc = new SQLContext(sc)
    val session = sqlc.sparkSession
    
    val rdd = sc.parallelize(rows, partitions)
    val dataframes = session.createDataFrame(rdd, schema)

    dataframes

  }
  
  private def toRow(origin:String, mat:Mat):Row = {
    
    val length = (mat.total * mat.channels).toInt
    
    val data = new Array[Byte](length)
    mat.get(0, 0, data)

    val width = mat.width
    val height = mat.height
    
    val nChannels = mat.channels
    val mode = mat.`type`()
    
    /*
     * The schema order: origin, height, width, nChannels, mode, data)
     */
    Row.fromSeq(Seq(origin, height, width, nChannels, mode, data))
    
  }
  
  private def resizeMat(src:Mat, resizeH:Int, resizeW:Int):Mat = {
            
    val size = new Size(resizeW, resizeH)
    val dest = new Mat()

    Imgproc.resize(src, dest, size)
    dest
  
  }
  /*
   * Sample implementation how to use the conversion of
   * video sequences into Apache Spark data frames
   */
  def main(args:Array[String]):Unit = {
   
    val conf = new SparkConf().setAppName("WorksDL").setMaster("local[*]");
    val sc = NNContext.initNNContext(conf)

    val path = "/Work/analytics-zoo/videos/IMG_8503.mp4"
    val dataframe = 
      video2DF(sc, path:String, partitions = 8, resizeH = 320, resizeW = 240, begin = 2, end = 3)
    
    dataframe.show
    sc.stop

  }
}