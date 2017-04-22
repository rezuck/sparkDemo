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
    
    //Get the workfile.
    val iplog = sc.textFile("/home/ron/Documents/apache-access-log.txt")
    
    /* We note that a typical record has the form:
     * 
     * 209.112.9.34 - - [25/May/2015:23:11:15 +0000] "GET / HTTP/1.0" 200 3557 "-" "Mozilla/3.01 (compatible;)"
     * for our purposes, two pieces of information are important, the IP address and the time stamp.
     * 
     * The time stamp can be used to isolate, from all of the data, only those records that are pertinent to the
     * investigation. For example, we may be interested in the possible attacks over a minute.
     * 
     * From our data we see that our minimal increment is one minute. To filter for this minute we might use
     * 
     * val ipTime iplog.filter(line => line.contains("25/May/2015:23:11:15"))
     * 
     * This would give us a reduced set containing only those records that occurred at the 25/May/2015:23:11:15.
     * (We don't do this here because the data only covers the minimal period.
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
     * the network value that we seek. The parameter is a ReGex expression.
    */
    val iplines = iplog.map(line => line.split("\\."))
    /*
     * Having spilt our records, we now seek to build our key, value pair. Since the first
     * element of our array list contains the network, we use that as our key.
     * 
     * Next, for each record matching our key we indicate the hit using the map command.
     * 
     * We then sum the number of hits up with the reduce command.
     * 
     * Finally, we write out the results to a file.
     */
    val ipadrrs = iplines.map{line => (makeKey(line(0)) , 1)}
     .reduceByKey(_+_)
     .saveAsTextFile("/home/ron/Documents/Output.txt")
   /*
    * The use of a function is kind of silly as we don't need to use a function to map
    * the line. It is included here to demonstrate that it can be done. If we we to 
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
     
     
   //iplog.flatMap{line => line.split(".")}
   // .map { ipaddr => (ipaddr, 1)}
   //  .reduceByKey(_+_)
   //  .saveAsTextFile("/home/ron/Documents/Output.txt")

   /*
    * Finally, we stop the process.  
    */
     sc.stop
        
  }
  /*
   * makeKey is a Scala function to construct the key. Here it is trivial. It is done
   * to show that a fully formed function can be used.
   */
  def makeKey(l:String) = l
  /*
   * makeKey2 is a more elaborate function. It constructs the full IP address.
   * It's usage is:
   * 
   * val ipadrrs = iplines.map{line => (makeKey(line(0),line(1),line(2),line(3)) , 1)}
   */
  def makeKey2(l:String,l1:String, l2:String, l3:String) = l + "." + l1 + "." + l2 + "." + l3
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