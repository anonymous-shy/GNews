package com.donews.utils

import java.security.{MessageDigest, NoSuchAlgorithmException}

/**
  * Donews Company
  * Created by Liu dh 
  * 2017/11/16.15:28
  */
object MD5Utils {

  /**
    *
    * @param plainText
    *            明文
    * @return 32位密文
    */
  def md5(plainText:String):String = {
    var md5_str = ""
    try {
      val md = MessageDigest.getInstance("MD5")
      md.update(plainText.getBytes())
      val b = md.digest()
      var i = -1
      val buf = new StringBuffer("");
      for (offset <- 0 until(b.length)) {
        i = b(offset);
        if (i < 0)
          i += 256;
        if (i < 16)
          buf.append("0")
        buf.append(Integer.toHexString(i))
      }
      md5_str = buf.toString();

    } catch  {
      case e:NoSuchAlgorithmException => e.printStackTrace()
    }
    md5_str
  }

}
