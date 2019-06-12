package com.vova.utils

object Util {

  def filterUtf8m4(s: String): String = {
    if (s == null) s else s.replaceAll("[^\\u0000-\\uD7FF\\uE000-\\uFFFF]", "?")
  }

}
