package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
  	var rectangle = new Array[String](4)
    rectangle = queryRectangle.split(",")
    var rect_a1 = rectangle(0).trim.toDouble
    var rect_b1 = rectangle(1).trim.toDouble
    var rect_a2 = rectangle(2).trim.toDouble
    var rect_b2 = rectangle(3).trim.toDouble
            
    var point = new Array[String](2)
    point= pointString.split(",")          
    var point_a=point(0).trim.toDouble
    var point_b=point(1).trim.toDouble
          

    var lower_a =0.0
    var higher_a =0.0
          
    if (rect_a1 < rect_a2)
    {
    	lower_a = rect_a1
        higher_a = rect_a2
    }
    else
    {
        lower_a = rect_a2
        higher_a = rect_a1
    }
          
    var lower_b = math.min(rect_b1, rect_b2)
    var higher_b = math.max(rect_b1, rect_b2)
          
    if(point_b > higher_b || point_a < lower_a || point_a > higher_b || point_b < lower_b)
    	return false
    else
        return true
        
    return true 
  }

  // YOU NEED TO CHANGE THIS PART IF YOU WANT TO ADD ADDITIONAL METHODS

}
