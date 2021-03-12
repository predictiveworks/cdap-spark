package com.redislabs.provider.redis.util
/*
 * This class is extracted from the spark-redis
 * project to downgrade for use with Spark 2.1.3
 */

import org.slf4j.{ Logger, LoggerFactory }

/**
 * @author The Viet Nguyen
 */
trait Logging {

  /**
   * This logger will likely to be used in serializable environment
   * like Spark contexts. So, we make it transient to avoid unnecessary
   * serialization errors.
   */
  @transient private var _logger: Logger = _

  protected def loggerName: String =
    this.getClass.getName.stripSuffix("$")

  protected def logger: Logger = {
    if (_logger == null) {
      _logger = LoggerFactory.getLogger(loggerName)
    }
    _logger
  }

  def logInfo(msg: => String): Unit = {
    if (logger.isInfoEnabled) {
      _logger.info(msg)
    }
  }

  def logDebug(msg: => String): Unit = {
    if (logger.isDebugEnabled) {
      _logger.debug(msg)
    }
  }

  def logTrace(msg: => String): Unit = {
    if (logger.isTraceEnabled) {
      _logger.trace(msg)
    }
  }
}
