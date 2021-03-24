package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  // YOU NEED TO CHANGE THIS PART

  def IsCellInBounds(x: Double, y: Double, z: Int, x1: Double, x2: Double, y1: Double, y2: Double, z1: Int, z2: Int): Boolean = {
    if ((x >= x1) && (x <= x2) && (y >= y1) && (y <= y2) && (z >= z1) && (z <= z2)) {
      return true
    }
    return false
  }

  def CheckBoundary(point: Int, min: Int, max: Int) : Int = {
      if (point == min || point == max) {
        return 1
      }
      return 0
    }

    def GetNeighbourCount(x1: Int, y1: Int, z1: Int, x2: Int, y2:Int, z2:Int, x: Int, y: Int, z: Int): Int = {
        val pointLocationInCube: Map[Int, String] = Map(0->"inside", 1 -> "face", 2-> "edge", 3-> "corner")
        val mapping: Map[String, Int] = Map("inside" -> 26, "face" -> 17, "edge" -> 11, "corner" -> 7)

        var intialState = 0;

        intialState += CheckBoundary(x, x1, x2)
        intialState += CheckBoundary(y, y1, y2)
        intialState += CheckBoundary(z, z1, z2)

        var location = pointLocationInCube.get(intialState).get.toString()
        return mapping.get(location).get.toInt
    }

    def GetGScore(x: Int, y: Int, z: Int, num: Int, avg: Double, dev: Double, neighb: Int, sum: Int): Double ={
        val numerator = (sum.toDouble - (avg * neighb.toDouble))
        val denominator = dev * math.sqrt((((num.toDouble * neighb.toDouble) - (neighb.toDouble * neighb.toDouble)) / (num.toDouble-1.0).toDouble).toDouble).toDouble
        return (numerator/denominator).toDouble
    }
}
