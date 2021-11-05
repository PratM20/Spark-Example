package com.crif.highmark.CrifSemantic.Impl
import org.apache.log4j.Logger

trait LoggerTrait {
    val loggerName: String = this.getClass.getName
    lazy val logger: Logger = Logger.getLogger(loggerName)
}