package pt

import com.typesafe.scalalogging.slf4j.LazyLogging

// This needs to be accessible to org.apache.spark.graphx.lib.backport
trait Logging extends LazyLogging {
  protected def logDebug(s: String) = logger.debug(s)
  protected def logInfo(s: String) = logger.info(s)
  protected def logTrace(s: String) = logger.trace(s)
}
