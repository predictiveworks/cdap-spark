package com.redislabs.provider.redis.util
/*
 * This class is extracted from the spark-redis
 * project to downgrade for use with Spark 2.1.3
 */

import org.apache.commons.lang3.StringUtils
import redis.clients.jedis.{ StreamEntryID, Jedis }

/**
 * @author The Viet Nguyen
 */
object StreamUtils extends Logging {

  val EntryIdEarliest = new StreamEntryID(0, 0)

  def createConsumerGroupIfNotExist(conn: Jedis, streamKey: String, groupName: String,
    offset: StreamEntryID): Unit = {
    try {
      conn.xgroupCreate(streamKey, groupName, offset, true)
    } catch {
      case e: Exception if StringUtils.contains(e.getMessage, "already exists") =>
        logInfo(s"Consumer group already exists: $groupName")
    }
  }

  def resetConsumerGroup(conn: Jedis, streamKey: String, groupName: String,
    offset: StreamEntryID): Unit = {
    logInfo(s"Setting consumer group $groupName id to $offset")
    conn.xgroupSetID(streamKey, groupName, offset)
  }
}
