package hbase

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object JsonMapper {
  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  def get: ObjectMapper = mapper

}
