package com.bluesky.etl.utils

/**
  * Created by root on 18-8-24.
  */
object HashUtils {
    def Md5hash(s:String)={
      val m = java.security.MessageDigest.getInstance("MD5")
      val b = s.getBytes("UTF-8")
      m.update(b,0,b.length)
      new java.math.BigInteger(1,m.digest()).toString(16)
    }
}
