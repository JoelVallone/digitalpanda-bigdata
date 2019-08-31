package org.digitalpanda.bigdata

package object sensor {
  type Location = String
  case class Measure (timestamp: Long, value: Double)
}
