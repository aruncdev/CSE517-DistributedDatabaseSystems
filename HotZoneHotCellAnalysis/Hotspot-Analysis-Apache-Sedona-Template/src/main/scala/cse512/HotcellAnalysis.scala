package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART

    pickupInfo.createOrReplaceTempView("pickupInfoView")

    spark.udf.register("IsCellInBounds", (x: Double, y:Double, z:Int) =>  ( (x >= minX) && (x <= maxX) && (y >= minY) && (y <= maxY) && (z >= minZ) && (z <= maxZ) ))

    val filteredDf = spark.sql("select x,y,z from pickupInfoView where IsCellInBounds(x, y, z) order by z,y,x").persist()
    filteredDf.createOrReplaceTempView("filteredPointsView")

    val filteredPointDf = spark.sql("select x,y,z,count(*) as numPoints from filteredPointsView group by z,y,x order by z,y,x").persist()
    filteredPointDf.createOrReplaceTempView("filteredPointCountDfView")

    spark.udf.register("square", (inputX: Int) => (inputX*inputX).toDouble)
    val pointsSum = spark.sql("select count(*) as numCellsWithAtleastOnePoint, sum(numPoints) as totalPointsInsideTheGivenArea, sum(square(numPoints)) as squaredSumOfAllPointsInGivenArea from filteredPointCountDfView")
    pointsSum.createOrReplaceTempView("pointsSum")

    val numCellsWithAtleastOnePoint = pointsSum.first().getLong(0)
    val total = pointsSum.first().getLong(1)
    val sumCells = pointsSum.first().getDouble(2)

    val bar = total / numCells
    val SD = math.sqrt((sumCells / numCells) - (bar * bar) )

    spark.udf.register("GetNeighbourCount", (minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int, Xin: Int, Yin: Int, Zin: Int)
    => ((HotcellUtils.GetNeighbourCount(minX, minY, minZ, maxX, maxY, maxZ, Xin, Yin, Zin))))
    val neigh = spark.sql("select " +
      "view1.x as x, " +
      "view1.y as y, " +
      "view1.z as z, " +
      "GetNeighbourCount("+minX + "," + minY + "," + minZ + "," + maxX + "," + maxY + "," + maxZ + "," + "view1.x,view1.y,view1.z) as totalNeighbours, " +
      "count(*) as neighboursWithValidPoints, " +
      "sum(view2.numPoints) as sumAllNeighboursPoints " +
      "from filteredPointCountDfView as view1, filteredPointCountDfView as view2 " +
      "where (view2.x = view1.x+1 or view2.x = view1.x or view2.x = view1.x-1) and (view2.y = view1.y+1 or view2.y = view1.y or view2.y = view1.y-1) and (view2.z = view1.z+1 or view2.z = view1.z or view2.z = view1.z-1) " +
      "group by view1.z, view1.y, view1.x order by view1.z, view1.y, view1.x").persist()

    neigh.createOrReplaceTempView("NeighboursView")

    spark.udf.register("GetGScore", (x: Int, y: Int, z: Int, numcells: Int, mean:Double, sd: Double, totalNeighbours: Int, sumAllNeighboursPoints: Int) => ((
      HotcellUtils.GetGScore(x, y, z, numcells, mean, sd, totalNeighbours, sumAllNeighboursPoints))))
    val neighbours = spark.sql("select x, y, z, " +
      "GetGScore(x, y, z," +numCells+ ", " + bar + ", " + SD + ", totalNeighbours, sumAllNeighboursPoints) as gi_statistic " +
      "from NeighboursView " +
      "order by gi_statistic desc")
    neighbours.createOrReplaceTempView("NeighboursDescView")
    neighbours.show()

    val res = spark.sql("select x,y,z from NeighboursDescView")

   return res // YOU NEED TO CHANGE THIS PART
}
}
