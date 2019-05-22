package com.vova.snowplow.schema

import org.apache.spark.sql.Row

abstract class BaseWrappedRow {
    val row:Row

    def getRow(name: String): Row = {
        if (row == null) {
            null
        } else {
            try {
                row.getAs[Row](name)
            } catch {
                case _:IllegalArgumentException => null
                case _:ClassCastException => null
            }
        }
    }

    def getRowSeq(name: String): Seq[Row] = {
        if (row == null) {
            null
        } else {
            try {
                row.getAs[Seq[Row]](name)
            } catch {
                case _:IllegalArgumentException => null
                case _:ClassCastException => null
            }
        }
    }

    def getString(name: String): String = {
        if (row == null) {
            null
        } else {
            try {
                row.getAs[Any](name) match {
                    case v:String => v
                    case null => null
                    case v => v.toString
                }
            } catch {
                case _:IllegalArgumentException => null
            }
        }
    }

    def getLong(name: String, default: Long = 0): Long = {
        if (row == null) {
            default
        } else {
            try {
                row.getAs[Any](name) match {
                    case v:Long => v
                    case v:Int => v
                    case v:Number => v.longValue()
                    case v:String => v.toLong
                    case _ => default
                }
            } catch {
                case _:IllegalArgumentException => default
            }
        }
    }
}
