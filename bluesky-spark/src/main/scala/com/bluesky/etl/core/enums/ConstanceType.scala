package com.bluesky.etl.core.enums

/**
  * Created by root on 18-8-17.
  */
object ConstanceType extends Enumeration{
  type CommonFieldType = Value
  val HIVE_DATABASE_NAME = Value(0,"db")
  val HIVE_REPORT_DATABASE_NAME = Value(1,"SF_QUALITY_REPORT")
  val HIVE_STREAMING_DATABASE_NAME = Value(2,"SF_STREAMING")
  val HIVE_STREAMING_TABLE_NAME = Value(3,"statistics")
  val HIVE_STREAMING_ERROR_TABLE_TRY_TIME = Value(4,"try_time")
  val HIVE_STREAMING_ERROR_TABLE_ADD_TIME = Value(5,"add_time")
  val HIVE_STREAMING_ERROR_TABLE_ERROR_MSG = Value(6,"error_msg")
  val HIVE_STREAMING_ERROR_TABLE_SEQUENCE = Value(7,"sefon_sequence")
  val OGG_OPTS_TIMESTAMP = Value(8,"ogg_opts_time")
  val HIVE_STREAMING_BLUESKY_TIMESTAMP = Value(9,"bluesky_timestamp")

}
