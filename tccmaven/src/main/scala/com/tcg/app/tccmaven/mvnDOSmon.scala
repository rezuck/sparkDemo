package com.tcg.app.tccmaven
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object mvnDOSmon {
  def main(args: Array[String]) = {
    /*
     * The function of this app is to determine if a distributed denial of service (DDOS) attack
     *  has occurred. 
     */
    
    //Prepare the work environment.  
    
    
    val conf = new SparkConf()
      .setAppName("mvnDOSmon")
      .setMaster("local")
      
      
    //Open a new context.
      
      
    val sc = new SparkContext(conf)
    
    
    //Get the work file.
    
    
    val iplog = sc.textFile("/home/ron/workspace/maven/tccmaven/apache-access-log.txt")
    
    
    /* We note that a typical record has the form:
     * 
     * 209.112.9.34 - - [25/May/2015:23:11:15 +0000] "GET / HTTP/1.0" 200 3557 "-" "Mozilla/3.01 (compatible;)"
     * 
     * For our purposes, two pieces of information are important, the IP address and the time stamp.
     * 
     * The time stamp can be used to isolate, from all of the data, only those records that are pertinent
     * to our investigation. For example, we may be interested in the possible attacks over a month, or a second.
     * 
     * From our data we see that our minimal increment is one second. To filter for this second we might use
     */
    
    
      val ipTime = iplog.filter(line => line.contains("25/May/2015:23:11:15"))
     
      
     /* 
     * This gives us a reduced set containing only those records that occurred at the 25/May/2015:23:11:15.
     * 
     * The next item, the IP address, tells us the source of the potential attack. Here we recall 
     * some facts about IP addresses.
     * 
     * IP addresses have the following format:
     * 
     * 		0.0.0.0 -> 126.255.255.255 => Large Network
		 *		128.0.0.0 -> 191.255.255.255 => Medium
		 *		192.000.000.000 -> 223.255.255.255 => Small
     * 
     * where the first number refers to the network and the remainder, the host. Thus, for
     * our purposes, the first number is the only one that matters.
     * 
     * In order to obtain the network, we need to perform some string manipulation. Here we will
     * break the record up by ".". This gives us an array list. The first element will be
     * the network value that we seek. The parameter is a RegEx expression.
    */
    
    
    val iplines = ipTime.map(line => line.split("\\."))
    
    
    /*
     * Having split our records, we now seek to build our key, value pair. Since the first
     * element of our array list contains the network, we use that as our key.
     * 
     * Next, for each record matching our key we indicate the hit using the map command. In our case
     * we would get (209,1) where "209" is the key and "1" (integer) is the value.
     * 
     * We then sum the number of hits up with the reduce command. In our case we add up all
     * of the values.
     * 
     * Finally, we write out the results to a file.
     */
    
    
    val ipadrrs = iplines.map{line => (makeKey(line(0)) , 1)}
     .reduceByKey(_+_)
     .saveAsTextFile("/home/ron/workspace/maven/tccmaven/Outputx")
    
     
   /*
    * The use of a function is kind of silly as we don't need to use a function to map
    * the line. It is included here to demonstrate that it can be done. If we were to 
    * want, say, the full IP Address we could use the function makeKey2 listed below.
    * We also note that a function could be used in the creation of a value.
    *
    * The output has the form
    * 
    * (139,356)
		* (63,6966)
		* (8,176)
		* (234,794)
		* (155,53969)
		* (57,350)
		* (209,6814)
		* (21,442)
    *     * 	
    * This has the form of a key, value pair. The key is the IP address network. 
    * The value shows the number of hits. As can be seen, "155" generated a significantly 
    * larger number of hits than the others. This suggests an attack. We also note 
    * that "209" (a small network) also has also generated a larger number of hits. 
    * Also, note that "63" has a large number of hits. But, since it is a large network,
    * this might be normal. Depending upon the rule chosen, one or both could be viewed
    * as threats.
    */  
     
     /*
      * We note that the above process can be combined into one operation with the following command:
      */
     
   //iplog.flatMap{line => line.split("\\.")}
   //  .map { ipaddr => (ipaddr, 1)}
   //  .reduceByKey(_+_)
   //  .saveAsTextFile("/home/ron/Documents/Output")
     
     /* In this version we look at the top ten networks by number of hits.
      * 
      * Here we build our RDD of network (K), hit (V) pairs and count the hits.
      * 
      * We then collect our results remembering that the RDD(s) could be scattered.
      *  
      * The resultant data is manageable locally so we should be okay. We then convert it 
      * to an array and sort it on the second element. Notice that, even though most
      * arrays are zero indexed. This is not. I think that this is due to the "_2" tuple.
      * The "_2" doesn't represent the element but the total count of the elements in the array.
      * So "_4" means ARRAY[1,2,3,4], etc., and "_4" is last element? in any event, it works.
      * If ".sortWith(_.1 > _._1)" is used the array is sorted on the first element.
      * 
      * Finally, we take the top ten and print them out to the console. 
      */
     
     
     val res = iplines.map{line => (makeKey(line(0)) , 1)}
               .reduceByKey(_+_)
               .collect()
               .toArray
               .sortWith(_._2 > _._2)
               .take(5).foreach(println)
 
   /*
    * We get:
      * 
      * (155,53969)
			* (129,7176)
			* (63,6966)
			* (209,6814)
			* (200,1965)
			* (82,969)
			* (148,952)
			* (94,879)
			* (194,878)
			* (234,794)
      * 
      *
      * 
    * Finally, we stop the process.  
    */
     
     
     sc.stop
        
  }
  /*
   * makeKey is a Scala function to construct the key. Here it is trivial. The input value is the
   * first element of our array list and is just returns it. This is done to show that a fully
   * formed function can be used.
   */
  
  
  def makeKey(l:String) = l
  
  
  /*
   * makeKey2 is a more elaborate function. It constructs the full IP address.
   * It's usage is:
   * 
   * val ipadrrs = iplines.map{line => (makeKey(line(0),line(1),line(2),line(3)) , 1)}
   * 
   * The input values are the first four elements of our array list.
   * 
   * We take the substring of our fourth element because the input value has the form:
   * 
   * 34 - - [25/May/2015:23:11:15 +0000] "GET / HTTP/1
   * 
   * because of the way we split the record.
   * 
   * Also, we can create a function for the value. This is probably done most often.
   */
  
  
  def makeKey2(l:String,l1:String, l2:String, l3:String) = l + "." + l1 + "." + l2 + "." + l3.substring(0,l3.indexOf(" "))

  /*
   * By the way, these can be combined into one class (object):
   * 
   * def class keyFunctions {
   * 
   * 	 def makeKey(l:String) = l
   * 
   *   def makeKey2(l:String,l1:String, l2:String, l3:String) = l + "." + l1 + "." + l2 + "." + l3.substring(0,l3.indexOf(" "))
   * 
   * }
   * 
   * Of course, when calling a method, we have to include the class name:
   * 
   * iplines.map{line => (keyFunctions.makeKey(line(0)) , 1)}
   * 
   * This might be useful for a larger project. Note that the entire class has to be sent to the 
   * cluster so they must be kept small.
   */
  
/*
 * Discussion:
 * 
 * 1. This application is largely a "proof of concept" and is designed to demonstrate a
 * particular approach.
 * 
 * 2. It seems possible to turn something like this into a streaming version where 
 * an input stream (the http header) is received and checked against previous results.
 * If it matches a network with activity that suggests it is an attack, it could be deflected
 * away from the servers.
 * 
 * 3. The importance of string manipulation cannot be stressed enough. Parsing the input stream
 * and the creation of keys is critical.
 */
}