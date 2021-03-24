package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    
    // YOU NEED TO CHANGE THIS PART
    val Array(x1, y1, x2, y2) = queryRectangle.split(",").map(x => x.toDouble)
    val xmin = math.min(x1, x2)
    val xmax = math.max(x1, x2)

    val ymin = math.min(y1, y2)
    val ymax = math.max(y1, y2)

    val Array(x, y) = pointString.split(',').map(x => x.toDouble)

    if ( (x >= xmin) && (x <= xmax) && (y >= ymin) && (y <= ymax) ){
        return true
    }

    return false // YOU NEED TO CHANGE THIS PART
  }

  // YOU NEED TO CHANGE THIS PART IF YOU WANT TO ADD ADDITIONAL METHODS
}
