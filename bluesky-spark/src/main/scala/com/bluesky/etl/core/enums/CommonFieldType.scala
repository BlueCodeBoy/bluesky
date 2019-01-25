package com.bluesky.etl.core.enums

/**
  * Created by root on 18-8-17.
  */
object CommonFieldType extends Enumeration{
  type CommonFieldType = Value
  val DATAFRAME_POSTFIX = Value(0,"SF_COL")
  val SF_RES = Value(1,"SF_RES")
  val SF_BATCH = Value(2,"SF_BATCH")

}
