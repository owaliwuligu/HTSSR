package demo

import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable.ArrayBuilder
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Queue}
import scala.util.control.Breaks

object HTSSR extends Serializable{
  //防止SSR覆盖重复
  var overlapping_check_array:Array[Boolean] = new Array[Boolean](10)
//  var overlapping_check_array:Broadcast[Array[Boolean]] = null
//  val conf = new SparkConf()
//    .setAppName("map")
//    .setMaster("local")
//  val sc=new SparkContext(conf)
  var sc:SparkContext = null
  var num_partitions = 51200 //5.2:51200;4.21:1009, 1280,12800
  var acid_is_open = true //true
  val MAX_INSERT_TIMES = 10000000
  var seq_id:Int = 0
  var seq_num:Int = 0
  var is_ssr = true //true
  var sum:Int = 0
  var miss:Int = 0 //0
  var MAX_GAP = 0 //0
  var is_split = false// The flag describes whether sequences would be splited
  var loop_type = false
  var sName_arr:Array[String] = null
//  var broadcast_ssr_split:Broadcast[Array[(Int, Int, Int)]] = null
//  var broadcast_len:Broadcast[Int] = null
  def init():Unit={
//    sc = sparkContext
//    overlapping_check_array.foreach(f=>false)
    if(overlapping_check_array != null) {
      for (i <- 0 to overlapping_check_array.length - 1) {
        overlapping_check_array(i) = false
      }
    }
  }

  def init2(over_lapped_list:List[(Int, Int)]):Unit={
    for(i <- over_lapped_list){
      for(j <- i._1 to i._2){
        overlapping_check_array(j) = true
      }
    }
  }

  val min_repeat = 3
  var split_thresold=500000//500000
  val max_motif_len=6

  //  判断motif的原子性，具有原子性返回true，否则返回false | passed
  def ifAcid_old(motif_str:String, is_open:Boolean=true):Boolean={
    if(is_open==false){
      return true
    }
    val loop=new Breaks
    var temp_str=""
    for(ch <- motif_str){
      temp_str+=ch
      //      println(temp_str)
      var flag=true
      val temp_len = temp_str.length()
      loop.breakable {
        if(motif_str.length % temp_str.length==0) {
          if (temp_len > motif_str.length / 2) {
            flag = false
          }
          for (i <- temp_len to motif_str.length() - temp_len by temp_len) {
            //          println(i)
            if (temp_str != motif_str.substring(i, i + temp_len)) {
              flag = false
              loop.break()
            }
          }
          if (flag == true) {
            //          println("yes")
            return false
          }
        }
      }
    }
    true
  }

  def ifAcid(motif_str:String, is_open:Boolean=true):Boolean={
    if(is_open==false){
      return true
    }

    var temp_str = ""
    var acid_motif_len = 1
    var motif_index = 0

    for(i <- 0 to motif_str.length-1){
      if(acid_motif_len > motif_str.length()/2){
        return true
      }
      if(i==0){
        temp_str += motif_str(i)
      }else{
        temp_str += motif_str(i)
        if(motif_str(i)==temp_str(motif_index)){
          if(motif_index+1==acid_motif_len){
            motif_index = 0
          }else{
            motif_index += 1
          }
        }else{
          if(motif_str(i)==temp_str(0)){
            acid_motif_len = i
            motif_index = 1
          }else{
            acid_motif_len = i+1
            motif_index = 0
          }
        }
      }
    }
    if(acid_motif_len<=motif_str.length/2 && motif_str.length%acid_motif_len==0){
      return false
    }else{
      return true
    }
  }

  def motif_cmp(str_1:String, str_2:String) :Int={
    if(str_1.length!=str_2.length){
      0
    }
    val len = str_1.length
    var res = 0
    for(i <- 0 to len-1){
      if(str_1(i)==str_2(i)){
        res += 1
      }
    }
    res
  }

  //  判断从motif_pos开始长为motif_len的motif，是否是微卫星的一部分。第一个返回值是微卫星的起始位置，第二个返回值是motif重复次数，第三个返回值是ssr结束为止；如果返回值为(0, 0)，则表明不是微卫星的一部分 | passed  x
  def ifContain(seq:String, motif_pos:Int, motif_len:Int):(Int, Int, Int)={
    var count=1
    val min_cmp_num = motif_len - miss
    var gap = 0 //记录motif之间的gap距离
    var gap_flag = false //标记是否出现过gap
    var gap_times = 0 //记录gap出现此处
    if(motif_pos+motif_len-1>=seq.length()){
      return (0, 0, 0)
    }
//    println(seq+" "+motif_pos+" "+motif_len)
    val motif_str=seq.substring(motif_pos, motif_pos+motif_len)
    //    println(motif_str)
    val loop=new Breaks
    if(!ifAcid(motif_str)){
      return (0, 0, 0)
    }
    //    println("!!")
    //往左遍历
    var temp_pos_left=motif_pos
    temp_pos_left -= motif_len
    loop.breakable {
      while (temp_pos_left >= 0) {
        val temp_str = seq.substring(temp_pos_left, temp_pos_left+ motif_len)
//        println(temp_str)
        if (motif_cmp(temp_str, motif_str)>=min_cmp_num) {
          count += 1
          temp_pos_left -= motif_len
        } else {
          var break_flag = true
          if(gap_times < MAX_INSERT_TIMES) {
            if (gap_flag == false) {
              var temp_gap = 1
              var temp_pos_left_2 = temp_pos_left
              val loop2 = Breaks
              loop2.breakable {
                while (temp_gap <= MAX_GAP) {
                  temp_pos_left_2 = temp_pos_left - temp_gap
                  if (temp_pos_left_2 < 0) {
                    loop2.break()
                  }
                  val temp_str_2 = seq.substring(temp_pos_left_2, temp_pos_left_2 + motif_len)
                  if (motif_cmp(temp_str_2, motif_str) >= min_cmp_num) {
                    count += 1
                    break_flag = false
                    gap = temp_gap
                    gap_times += 1
                    gap_flag = true
                    temp_pos_left -= (motif_len + gap)
                    temp_gap = MAX_GAP + 1
                  }
                  temp_gap += 1
                }
              }
            } else {
              if (temp_pos_left - gap >= 0) {
                val temp_str_2 = seq.substring(temp_pos_left - gap, temp_pos_left - gap + motif_len)
                if (motif_cmp(temp_str_2, motif_str) >= min_cmp_num) {
                  break_flag = false
                  gap_times += 1
                  temp_pos_left -= (motif_len + gap)
                  count += 1
                }
              }
            }
          }
          if(break_flag == true) {
            loop.break()
          }
        }
      }
    }

    //往右遍历
    var temp_pos_right=motif_pos
    temp_pos_right += motif_len
    loop.breakable {
      while (temp_pos_right+motif_len-1 < seq.length()) {
        val temp_str = seq.substring(temp_pos_right, temp_pos_right+ motif_len)
        if (motif_cmp(temp_str, motif_str)>=min_cmp_num) {
          count += 1
          temp_pos_right += motif_len
        } else {
          var break_flag = true
          if(gap_times < MAX_INSERT_TIMES) {
            if (gap_flag == false) {
              var temp_gap = 1
              var temp_pos_right_2 = temp_pos_right
              val loop2 = Breaks
              loop2.breakable {
                while (temp_gap <= MAX_GAP) {
                  temp_pos_right_2 = temp_pos_right + temp_gap
                  if (temp_pos_right_2 + motif_len - 1 >= seq.length) {
                    loop2.break()
                  }
                  val temp_str_2 = seq.substring(temp_pos_right_2, temp_pos_right_2 + motif_len)
                  if (motif_cmp(temp_str_2, motif_str) >= min_cmp_num) {
                    count += 1
                    break_flag = false
                    gap = temp_gap
                    gap_times += 1
                    temp_pos_right += (motif_len + gap)
                    gap_flag = true
                    temp_gap = MAX_GAP + 1
                  }
                  temp_gap += 1
                }
              }
            } else {
              if (temp_pos_right + gap + motif_len < seq.length) {
                val temp_str_2 = seq.substring(temp_pos_right + gap, temp_pos_right + gap + motif_len)
                if (motif_cmp(temp_str_2, motif_str) >= min_cmp_num) {
                  temp_pos_right += (motif_len + gap)
                  break_flag = false
                  gap_times += 1
                  count += 1
                }
              }
            }
          }
          if(break_flag == true) {
            loop.break()
          }
        }
      }
    }
    if(count>=min_repeat){
      (temp_pos_left+motif_len, count, temp_pos_right-1)
    }else{
      (0, 0, 0)
    }
  }

  //非递归分割序列2 返回值(seq_id, seq_name, motif/序列, 重复次数/0, 起始位置, 结束位置/0)
  def split_non_recursive3(seq_id:Int, seq_name:String, seq:(String, Int), kmer_list:List[Int]): ArrayBuffer[(Int, String, String, Int, Int, Int)]={
    //    var ssr_rdd:RDD[(String, String, Int, Int)] = sc.parallelize(List())  //(序列名称，motif，重复次数，起始位置)
    //    var seq_rdd:RDD[(String, String, Int)] = sc.parallelize(List()) //(序列名称， 序列， 起始位置)
    //    init()
    //    var res_list:List[(Int, Int, String, String, Int, Int, Int)] = List() //结果rdd，既保存seq由保存ssr
    val seg_num = Math.log(seq._1.length.toDouble) - Math.log(split_thresold.toDouble)
    val res_arr:ArrayBuffer[(Int, String, String, Int, Int, Int)] = new ArrayBuffer[(Int, String, String, Int, Int, Int)]
    var index = 0
    val que: Queue[(Int, Int)] = Queue() //(起始位置， 结束为止)

    //    var over_lapped_rdd:RDD[(Int, Int)]=sc.parallelize(List()) //(覆盖起始位置, 覆盖结束为止)
    que.enqueue((seq._2, seq._1.length - 1))

    while (!que.isEmpty) {
      val cur_node = que.dequeue()
      //      println(cur_node._1 + " " + cur_node._2)
      //序列长度小于等于阈值，直接加入seq_list
      if (cur_node._2 - cur_node._1 + 1 <= split_thresold) {
        //        seq_list = seq_list ++: List((seq_name, seq._1.substring(cur_node._1, cur_node._2+1), cur_node._1))
        //        seq_rdd = seq_rdd.union(sc.parallelize(List((seq_name, seq._1.substring(cur_node._1, cur_node._2+1), cur_node._1))))
        //          res_list = res_list ++: List((seq_id, kmer_k, seq_name, seq._1.substring(cur_node._1, cur_node._2 + 1), 0, cur_node._1, 0))
        res_arr += ((seq_id, seq_name, seq._1.substring(cur_node._1, cur_node._2 + 1), 0, cur_node._1, 0))
        index += 1
      } else {
        val mid_pos = (cur_node._1 + cur_node._2) / 2

        var left_most0 = 999999999//维护SSR的最左端
        var right_most0 = -1//维护SSR的最右端
        for(kmer_k <- kmer_list) {
          var left_most = 999999999 // 维护SSR的最左端+motif的位置
          var right_most = -1 // 维护SSR最右端-motif的位置
          for (j <- 1 to kmer_k) {
            if (mid_pos - kmer_k + j >= cur_node._1 && mid_pos + j - 1 <= cur_node._2) {
              val temp_res = ifContain(seq._1.substring(cur_node._1, cur_node._2 + 1), mid_pos - kmer_k + j - cur_node._1, kmer_k)

              //判断是否可能是微卫星的一部分
              if (temp_res._1 != 0 || temp_res._2 != 0) {
                val re_ssr = seq._1.substring(temp_res._1 + cur_node._1, temp_res._3 + cur_node._1 + 1)
                val motif_str = seq._1.substring(mid_pos - kmer_k + j, mid_pos + j)
                //判断是否满足原子性
                if (ifAcid(motif_str, acid_is_open)) {
                  //判断是否为cyclic SSR
                  if (temp_res._1 + cur_node._1 > right_most) {
                    if (temp_res._1 + kmer_k - 1 + cur_node._1 < left_most) {
                      left_most = temp_res._1 + kmer_k - 1 + cur_node._1
                    }
                    if (temp_res._3 - kmer_k + cur_node._1 > right_most) {
                      right_most = temp_res._3 - kmer_k + cur_node._1+1
                    }

                    if(temp_res._1<left_most0||temp_res._3>right_most0) {
                      //加入该合法SSR
                      println("ssr:" + seq._1.substring(temp_res._1 + cur_node._1, temp_res._3 + cur_node._1+1)+ " " +(temp_res._1+cur_node._1)+" "+(cur_node._1+temp_res._3))
                      //                    ssr_rdd = ssr_rdd.union(sc.parallelize(List((seq_name, motif_str, temp_res._2, temp_res._1+cur_node._1))))
                      //                      res_list = res_list ++: List((seq_id, kmer_k, seq_name, motif_str, temp_res._2, temp_res._1 + cur_node._1, temp_res._3 + cur_node._1))
                      res_arr += ((seq_id, seq_name, motif_str, temp_res._2, temp_res._1 + cur_node._1, temp_res._3 + cur_node._1))
                      index += 1
                    }

                    //                ssr_rdd2  = ssr_rdd2.union(sc.parallelize(List(seq._1.substring(temp_res._1, temp_res._1+temp_res._2*kmer_k))))
                    //                    over_lapped_rdd = over_lapped_rdd.union(sc.parallelize(List((0, (temp_res._1+cur_node._1, temp_res._3+cur_node._1)))))

                  }
                  if(temp_res._1<left_most0){
                    left_most0 = temp_res._1
                  }
                  if(temp_res._3>right_most0){
                    right_most0 = temp_res._3
                  }
                }
              }
            }
          }
        }

        que.enqueue((cur_node._1, mid_pos))
        que.enqueue((mid_pos, cur_node._2))
      }
    }
    //    res_list
    res_arr
  }

  //非递归分割序列2 返回值(seq_id, kmer_k, seq_name, motif/序列, 重复次数/0, 起始位置, 结束位置/0)
  def split_non_recursive_ao(seq_id:Int, seq_name:String, seq:(String, Int), kmer_k:Int): ArrayBuffer[(Int, Int, String, String, Int, Int, Int)]={
    //    var ssr_rdd:RDD[(String, String, Int, Int)] = sc.parallelize(List())  //(序列名称，motif，重复次数，起始位置)
    //    var seq_rdd:RDD[(String, String, Int)] = sc.parallelize(List()) //(序列名称， 序列， 起始位置)
    //    init()
    //    var res_list:List[(Int, Int, String, String, Int, Int, Int)] = List() //结果rdd，既保存seq由保存ssr
    val seg_num = Math.log(seq._1.length.toDouble) - Math.log(split_thresold.toDouble)
    val res_arr:ArrayBuffer[(Int, Int, String, String, Int, Int, Int)] = new ArrayBuffer[(Int, Int, String, String, Int, Int, Int)]
    var index = 0
    val que: Queue[(Int, Int)] = Queue() //(起始位置， 结束为止)

    //    var over_lapped_rdd:RDD[(Int, Int)]=sc.parallelize(List()) //(覆盖起始位置, 覆盖结束为止)
    que.enqueue((seq._2, seq._1.length - 1))

    while (!que.isEmpty) {
      val cur_node = que.dequeue()
      //      println(cur_node._1 + " " + cur_node._2)
      //序列长度小于等于阈值，直接加入seq_list
      if (cur_node._2 - cur_node._1 + 1 <= split_thresold) {
        //        seq_list = seq_list ++: List((seq_name, seq._1.substring(cur_node._1, cur_node._2+1), cur_node._1))
        //        seq_rdd = seq_rdd.union(sc.parallelize(List((seq_name, seq._1.substring(cur_node._1, cur_node._2+1), cur_node._1))))
        //          res_list = res_list ++: List((seq_id, kmer_k, seq_name, seq._1.substring(cur_node._1, cur_node._2 + 1), 0, cur_node._1, 0))
        res_arr += ((seq_id, kmer_k, seq_name, seq._1.substring(cur_node._1, cur_node._2 + 1), 0, cur_node._1, 0))
        index += 1
      } else {
        val mid_pos = (cur_node._1 + cur_node._2) / 2
        var left_most = 999999999 // 维护SSR的最左端+motif的位置
        var right_most = -1 // 维护SSR最右端-motif的位置

        for (j <- 1 to kmer_k) {
          if (mid_pos - kmer_k + j >= cur_node._1 && mid_pos + j - 1 <= cur_node._2) {
            val temp_res = ifContain(seq._1.substring(cur_node._1, cur_node._2 + 1), mid_pos - kmer_k + j - cur_node._1, kmer_k)

            //判断是否可能是微卫星的一部分
            if (temp_res._1 != 0 || temp_res._2 != 0) {
              val re_ssr = seq._1.substring(temp_res._1 + cur_node._1, temp_res._3 + cur_node._1)
              val motif_str = seq._1.substring(mid_pos - kmer_k + j, mid_pos + j)
              //判断是否满足原子性
              if (ifAcid(motif_str, acid_is_open)) {
                //判断是否为cyclic SSR
                if (temp_res._1 + cur_node._1 > right_most) {
                  if (temp_res._1 + kmer_k - 1 + cur_node._1 < left_most) {
                    left_most = temp_res._1 + kmer_k - 1 + cur_node._1
                  }
                  if (temp_res._3 - kmer_k + cur_node._1 > right_most) {
                    right_most = temp_res._3 - kmer_k + cur_node._1
                  }


                  //加入该合法SSR
                  println("ssr:" + seq._1.substring(temp_res._1 + cur_node._1, temp_res._3 + cur_node._1))
                  //                    ssr_rdd = ssr_rdd.union(sc.parallelize(List((seq_name, motif_str, temp_res._2, temp_res._1+cur_node._1))))
                  //                      res_list = res_list ++: List((seq_id, kmer_k, seq_name, motif_str, temp_res._2, temp_res._1 + cur_node._1, temp_res._3 + cur_node._1))
                  res_arr += ((seq_id, kmer_k, seq_name, motif_str, temp_res._2, temp_res._1 + cur_node._1, temp_res._3 + cur_node._1))
                  index += 1
                  //                ssr_rdd2  = ssr_rdd2.union(sc.parallelize(List(seq._1.substring(temp_res._1, temp_res._1+temp_res._2*kmer_k))))
                  //                    over_lapped_rdd = over_lapped_rdd.union(sc.parallelize(List((0, (temp_res._1+cur_node._1, temp_res._3+cur_node._1)))))

                }
              }
            }
          }
        }
        //左右seq入队列
        if (left_most > mid_pos) {
          left_most = mid_pos - 1
        }
        if (right_most < mid_pos) {
          right_most = mid_pos + 1
        }

        que.enqueue((cur_node._1, left_most))
        que.enqueue((right_most, cur_node._2))
      }
    }
    //    res_list
    res_arr
  }
  //非递归分割序列2 返回值(seq_id, kmer_k, seq_name, motif/序列, 重复次数/0, 起始位置, 结束位置/0)
  def split_non_recursive2(seq_id:Int, seq_name:String, seq:(String, Int), kmer_k:Int): ArrayBuffer[(Int, Int, String, String, Int, Int, Int)]={
//    var ssr_rdd:RDD[(String, String, Int, Int)] = sc.parallelize(List())  //(序列名称，motif，重复次数，起始位置)
//    var seq_rdd:RDD[(String, String, Int)] = sc.parallelize(List()) //(序列名称， 序列， 起始位置)
//    init()
//    var res_list:List[(Int, Int, String, String, Int, Int, Int)] = List() //结果rdd，既保存seq由保存ssr
    val seg_num = Math.log(seq._1.length.toDouble) - Math.log(split_thresold.toDouble)
    val res_arr:ArrayBuffer[(Int, Int, String, String, Int, Int, Int)] = new ArrayBuffer[(Int, Int, String, String, Int, Int, Int)]
    var index = 0
    val que: Queue[(Int, Int)] = Queue() //(起始位置， 结束为止)

      //    var over_lapped_rdd:RDD[(Int, Int)]=sc.parallelize(List()) //(覆盖起始位置, 覆盖结束为止)
    que.enqueue((seq._2, seq._1.length - 1))

    while (!que.isEmpty) {
        val cur_node = que.dequeue()
        //      println(cur_node._1 + " " + cur_node._2)
        //序列长度小于等于阈值，直接加入seq_list
        if (cur_node._2 - cur_node._1 + 1 <= split_thresold) {
          //        seq_list = seq_list ++: List((seq_name, seq._1.substring(cur_node._1, cur_node._2+1), cur_node._1))
          //        seq_rdd = seq_rdd.union(sc.parallelize(List((seq_name, seq._1.substring(cur_node._1, cur_node._2+1), cur_node._1))))
//          res_list = res_list ++: List((seq_id, kmer_k, seq_name, seq._1.substring(cur_node._1, cur_node._2 + 1), 0, cur_node._1, 0))
          res_arr += ((seq_id, kmer_k, seq_name, seq._1.substring(cur_node._1, cur_node._2 + 1), 0, cur_node._1, 0))
          index += 1
        } else {
          val mid_pos = (cur_node._1 + cur_node._2) / 2
          var left_most = 999999999 // 维护SSR的最左端+motif的位置
          var right_most = -1 // 维护SSR最右端-motif的位置

          for (j <- 1 to kmer_k) {
            if (mid_pos - kmer_k + j >= cur_node._1 && mid_pos + j - 1 <= cur_node._2) {
              val temp_res = ifContain(seq._1.substring(cur_node._1, cur_node._2 + 1), mid_pos - kmer_k + j - cur_node._1, kmer_k)

              //判断是否可能是微卫星的一部分
              if (temp_res._1 != 0 || temp_res._2 != 0) {
                val re_ssr = seq._1.substring(temp_res._1 + cur_node._1, temp_res._3 + cur_node._1)
                val motif_str = seq._1.substring(mid_pos - kmer_k + j, mid_pos + j)
                //判断是否满足原子性
                if (ifAcid(motif_str, acid_is_open)) {
                  //判断是否为cyclic SSR
                  if (temp_res._1 + cur_node._1 > right_most) {
                    if (temp_res._1 + kmer_k - 1 + cur_node._1 < left_most) {
                      left_most = temp_res._1 + kmer_k - 1 + cur_node._1
                    }
                    if (temp_res._3 - kmer_k + cur_node._1 > right_most) {
                      right_most = temp_res._3 - kmer_k + cur_node._1
                    }

                    //判断是否为重复覆盖SSR
                    var flag = false
                    for (i <- temp_res._1 + cur_node._1 to temp_res._3 - 1 + cur_node._1) {
                      if (overlapping_check_array(i) == false) {
                        flag = true
                      }
                    }
                    if (flag == true) {
                      //加入该合法SSR
                      println("ssr:" + seq._1.substring(temp_res._1 + cur_node._1, temp_res._3 + cur_node._1))
                      //                    ssr_rdd = ssr_rdd.union(sc.parallelize(List((seq_name, motif_str, temp_res._2, temp_res._1+cur_node._1))))
//                      res_list = res_list ++: List((seq_id, kmer_k, seq_name, motif_str, temp_res._2, temp_res._1 + cur_node._1, temp_res._3 + cur_node._1))
                      res_arr += ((seq_id, kmer_k, seq_name, motif_str, temp_res._2, temp_res._1 + cur_node._1, temp_res._3 + cur_node._1))
                      index += 1
                      //                ssr_rdd2  = ssr_rdd2.union(sc.parallelize(List(seq._1.substring(temp_res._1, temp_res._1+temp_res._2*kmer_k))))
                      //                    //更新overlapping_check_array
                      for (i <- temp_res._1 + cur_node._1 to temp_res._3 - 1 + cur_node._1) {
                        overlapping_check_array(i) = true
                      }
                      //                    over_lapped_rdd = over_lapped_rdd.union(sc.parallelize(List((0, (temp_res._1+cur_node._1, temp_res._3+cur_node._1)))))
                    }
                  }
                }
              }
            }
          }
          //左右seq入队列
          if (left_most > mid_pos) {
            left_most = mid_pos - 1
          }
          if (right_most < mid_pos) {
            right_most = mid_pos + 1
          }

          que.enqueue((cur_node._1, left_most))
          que.enqueue((right_most, cur_node._2))
        }
    }
//    res_list
    res_arr
  }
  //非递归分割序列
  def split_non_recursive(seq_name:String, seq:(String, Int), kmer_k:Int, miss:Int = 0, MAX_GAP:Int = 0): (List[(String, String, Int)], List[(String, String, Int, Int)])={
    var ssr_list:List[(String, String, Int, Int)] = List() //(序列名称，motif，重复次数，起始位置)
    var seq_list:List[(String, String, Int)] = List() //(序列名称， 序列， 起始位置)
    val que:Queue[(Int, Int)] = Queue() //(起始位置， 结束为止)
    var over_lapped_rdd:RDD[(Int, (Int, Int))]=sc.parallelize(List()) //(序列编号, (覆盖起始位置, 覆盖结束为止))
    que.enqueue((seq._2, seq._1.length-1))

    while(!que.isEmpty){
      val cur_node = que.dequeue()
//      println(cur_node._1 + " " + cur_node._2)
      //序列长度小于等于阈值，直接加入seq_list
      if(cur_node._2 - cur_node._1 + 1 <= split_thresold){
//        seq_list = seq_list ++: List((seq_name, seq._1.substring(cur_node._1, cur_node._2+1), cur_node._1))
        seq_list = seq_list ++: List((seq_name, seq._1.substring(cur_node._1, cur_node._2+1), cur_node._1))
      } else{
        val mid_pos = (cur_node._1 + cur_node._2)/2
        var left_most = 999999999  // 维护SSR的最左端+motif的位置
        var right_most = -1 // 维护SSR最右端-motif的位置

        for(j <- 1 to kmer_k){
          if(mid_pos-kmer_k+j >= cur_node._1 && mid_pos+j-1 <= cur_node._2){
            val temp_res = ifContain(seq._1.substring(cur_node._1, cur_node._2+1), mid_pos - kmer_k +j - cur_node._1, kmer_k)

            //判断是否可能是微卫星的一部分
            if(temp_res._1 != 0 || temp_res._2 != 0){
              val re_ssr = seq._1.substring(temp_res._1+cur_node._1, temp_res._3+cur_node._1)
              val motif_str = seq._1.substring(mid_pos - kmer_k + j, mid_pos + j)
              //判断是否满足原子性
              if(ifAcid(motif_str)){
                //判断是否为cyclic SSR
                if(temp_res._1+cur_node._1 > right_most) {
                  if (temp_res._1 + kmer_k - 1 + cur_node._1 < left_most) {
                    left_most = temp_res._1 + kmer_k - 1 + cur_node._1
                  }
                  if (temp_res._3 - kmer_k + cur_node._1 > right_most) {
                    right_most = temp_res._3 - kmer_k + cur_node._1
                  }

                  //判断是否为重复覆盖SSR
                  var flag = false
                  for(i <- temp_res._1 + cur_node._1 to temp_res._3 - 1 + cur_node._1){
                    if(overlapping_check_array(i)==false){
                      flag = true
                    }
                  }
                  if(flag==true){
                    //加入该合法SSR
                    println("ssr:"+seq._1.substring(temp_res._1 + cur_node._1, temp_res._3+cur_node._1))
                    ssr_list = ssr_list ++: List((seq_name, motif_str, temp_res._2, temp_res._1+cur_node._1))
                    //                ssr_rdd2  = ssr_rdd2.union(sc.parallelize(List(seq._1.substring(temp_res._1, temp_res._1+temp_res._2*kmer_k))))
                    //更新overlapping_check_array
                    for(i <- temp_res._1 + cur_node._1 to temp_res._3 - 1 + cur_node._1){
                      overlapping_check_array(i) = true
                    }
                    over_lapped_rdd = over_lapped_rdd.union(sc.parallelize(List((0, (temp_res._1+cur_node._1, temp_res._3+cur_node._1)))))
                  }
                }
              }
            }
          }
        }
        //左右seq入队列
        if(left_most>mid_pos){
          left_most=mid_pos-1
        }
        if(right_most<mid_pos ){
          right_most=mid_pos+1
        }

        que.enqueue((cur_node._1, left_most))
        que.enqueue((right_most, cur_node._2))
      }
    }
    (seq_list, ssr_list)
  }

  // [添加gap后pos会计算错误] 递归分割序列，返回值第一个List是分割后剩下的序列(序列, 序列起始位置)，第二个List是分割过程中得到的SSR
  def split(seq:(String, Int), kmer_k:Int):(List[(String, Int)], List[(String, Int)])={
    if(seq._1.length()<=split_thresold){
//            val list_seq:List[(String, Int)] = List(seq)
//      val seq_rdd:RDD[(String, Int)] = sc.parallelize(List(seq)).repartition(1)
      val seq_list:List[(String, Int)] = List(seq)
      //      val list_ssr:List[String] = List()
//      val ssr_rdd:RDD[String] = sc.parallelize(List()).repartition(1)
      val ssr_list:List[(String, Int)] = List()
      //
      return (seq_list, ssr_list)
    }

    val mid_pos = seq._1.length()/2

//    var ssr_rdd2:RDD[String] = sc.parallelize(List())
    var ssr_list2:List[(String, Int)] = List()
//    var seq_rdd2:RDD[(String, Int)] = sc.parallelize(List())
    var seq_list2:List[(String, Int)] = List()
    var left_most = 999999999  // 维护SSR的最左端+motif的位置
    var right_most = -1 // 维护SSR最右端-motif的位置

    for(j <- 1 to kmer_k){
      if(mid_pos-kmer_k+j >= 0 && mid_pos+j-1 < seq._1.length()){
        val temp_res = ifContain(seq._1, mid_pos - kmer_k + j, kmer_k)
        //判断是否可能是微卫星的一部分
        if(temp_res._1 != 0 || temp_res._2 != 0){
          val re_ssr = seq._1.substring(temp_res._1, temp_res._1 + kmer_k*temp_res._2)
          val motif_str = seq._1.substring(mid_pos - kmer_k + j, mid_pos + j)
          //判断是否满足原子性
          if(ifAcid(motif_str)){
            //判断是否为cyclic SSR
            if(temp_res._1 > right_most) {
              if (temp_res._1 + kmer_k - 1 < left_most) {
                left_most = temp_res._1 + kmer_k - 1
              }
              if (temp_res._1 + kmer_k * temp_res._2 - kmer_k > right_most) {
                right_most = temp_res._1 + kmer_k * temp_res._2 - kmer_k
              }

              //判断是否为重复覆盖SSR
              var flag = false
              for(i <- temp_res._1 + seq._2 to temp_res._1 + temp_res._2*kmer_k - 1 + seq._2){
                if(overlapping_check_array(i)==false){
                  flag = true
                }
              }
              if(flag==true){
                //加入该合法SSR
                println("ssr:"+seq._1.substring(temp_res._1, temp_res._1+temp_res._2*kmer_k))
                ssr_list2 = ssr_list2 ++: List((seq._1.substring(temp_res._1, temp_res._1+temp_res._2*kmer_k), temp_res._1+seq._2))
//                ssr_rdd2  = ssr_rdd2.union(sc.parallelize(List(seq._1.substring(temp_res._1, temp_res._1+temp_res._2*kmer_k))))
                //更新overlapping_check_array
                for(i <- temp_res._1 + seq._2 to temp_res._1 + temp_res._2*kmer_k - 1 + seq._2){
                  overlapping_check_array(i) = true
                }
              }
            }
          }
        }
      }
    }

    if(left_most>mid_pos){
      left_most=mid_pos-1
    }
    if(right_most<mid_pos ){
      right_most=mid_pos+1
    }

//    seq_rdd = seq_rdd.union(sc.parallelize(List((seq._1.substring(0, left_most+1), seq._2))))
//    seq_rdd = seq_rdd.union(sc.parallelize(List((seq._1.substring(right_most, seq._1.length()), right_most + seq._2))))

    val recurrence_res_left = split((seq._1.substring(0, left_most+1), seq._2), kmer_k)
    val recurrence_res_right = split((seq._1.substring(right_most, seq._1.length()), right_most + seq._2), kmer_k)

//    seq_rdd2 = seq_rdd2.union(recurrence_res_left._1)
    seq_list2 = seq_list2 ++: recurrence_res_left._1
//    seq_rdd2 = seq_rdd2.union(recurrence_res_right._1)
    seq_list2 = seq_list2 ++: recurrence_res_right._1
//    ssr_rdd2 = ssr_rdd2.union(recurrence_res_left._2)
    ssr_list2 = ssr_list2 ++: recurrence_res_left._2
//    ssr_rdd2 = ssr_rdd2.union(recurrence_res_right._2)
    ssr_list2 = ssr_list2 ++: recurrence_res_right._2
    (seq_list2, ssr_list2)
  }

  //从current_pos开始往右查找SSR， 第一个返回值是该motif重复次数，第二个返回值是ssr结束为止，返回(0, 0)代表未找到
  def searchSSR_ao(seq: (String, Int), motif_pos: Int, motif_len: Int): (Int, Int)={
    var count = 1
    val min_cmp_num = motif_len - miss
    var gap = 0 //记录motif之间的gap距离
    var gap_flag = false //标记是否出现过gap
    var gap_time = 0 //标记gap出现此处
    if(motif_pos + motif_len - 1 >= seq._1.length()){
      return (0, 0)
    }

    val motif_str = seq._1.substring(motif_pos, motif_pos+motif_len)
    val loop = Breaks

    //motif原子性判断
    if(!ifAcid(motif_str, acid_is_open)){
      return (0, 0)
    }

    //往右查找
    var current_pos = motif_pos + motif_len
    loop.breakable{
      while(current_pos + motif_len - 1 < seq._1.length()){
        val temp_str = seq._1.substring(current_pos, current_pos + motif_len)
        //判断当前片段是否和motif相同
        if (motif_cmp(temp_str, motif_str)>=min_cmp_num) {
          count += 1
          current_pos += motif_len
        } else {
          var break_flag = true
          if(gap_time < MAX_INSERT_TIMES) {
            if (gap_flag == false) {
              var temp_gap = 1
              var current_pos_2 = current_pos
              val loop2 = Breaks
              loop2.breakable {
                while (temp_gap <= MAX_GAP) {
                  current_pos_2 = current_pos + temp_gap
                  if (current_pos_2 + motif_len - 1 >= seq._1.length) {
                    loop2.break()
                  }
                  val temp_str_2 = seq._1.substring(current_pos_2, current_pos_2 + motif_len)
                  if (motif_cmp(temp_str_2, motif_str) >= min_cmp_num) {
                    count += 1
                    break_flag = false
                    gap = temp_gap
                    gap_time += 1
                    current_pos += (motif_len + gap)
                    gap_flag = true
                    temp_gap = MAX_GAP + 1
                  }
                  temp_gap += 1
                }
              }
            } else {
              if (current_pos + gap + motif_len < seq._1.length) {
                val temp_str_2 = seq._1.substring(current_pos + gap, current_pos + gap + motif_len)
                if (motif_cmp(temp_str_2, motif_str) >= min_cmp_num) {
                  break_flag = false
                  current_pos += (motif_len + gap)
                  gap_time += 1
                  count += 1
                }
              }
            }
          }
          if(break_flag == true) {
            loop.break()
          }
        }
      }
    }
    //判断motif重复次数是否达到最小阈值
    if(count >= min_repeat){
      return (count, current_pos - 1)
    }
    return (0, 0)
  }

  //从current_pos开始往右查找SSR， 第一个返回值是该motif重复次数，第二个返回值是ssr结束为止，返回(0, 0)代表未找到
  def searchSSR(seq: (String, Int), motif_pos: Int, motif_len: Int): (Int, Int)={
    var count = 1
    val min_cmp_num = motif_len - miss
    var gap = 0 //记录motif之间的gap距离
    var gap_flag = false //标记是否出现过gap
    var gap_time = 0 //标记gap出现此处
    if(motif_pos + motif_len - 1 >= seq._1.length()){
      return (0, 0)
    }

    val motif_str = seq._1.substring(motif_pos, motif_pos+motif_len)
    val loop = Breaks

    //motif原子性判断
    if(!ifAcid(motif_str, acid_is_open)){
      return (0, 0)
    }

    //往右查找
    var current_pos = motif_pos + motif_len
    loop.breakable{
      while(current_pos + motif_len - 1 < seq._1.length()){
        val temp_str = seq._1.substring(current_pos, current_pos + motif_len)
        //判断当前片段是否和motif相同
        if (motif_cmp(temp_str, motif_str)>=min_cmp_num) {
          count += 1
          current_pos += motif_len
        } else {
          var break_flag = true
          if(gap_time < MAX_INSERT_TIMES) {
            if (gap_flag == false) {
              var temp_gap = 1
              var current_pos_2 = current_pos
              val loop2 = Breaks
              loop2.breakable {
                while (temp_gap <= MAX_GAP) {
                  current_pos_2 = current_pos + temp_gap
                  if (current_pos_2 + motif_len - 1 >= seq._1.length) {
                    loop2.break()
                  }
                  val temp_str_2 = seq._1.substring(current_pos_2, current_pos_2 + motif_len)
                  if (motif_cmp(temp_str_2, motif_str) >= min_cmp_num) {
                    count += 1
                    break_flag = false
                    gap = temp_gap
                    gap_time += 1
                    current_pos += (motif_len + gap)
                    gap_flag = true
                    temp_gap = MAX_GAP + 1
                  }
                  temp_gap += 1
                }
              }
            } else {
              if (current_pos + gap + motif_len < seq._1.length) {
                val temp_str_2 = seq._1.substring(current_pos + gap, current_pos + gap + motif_len)
                if (motif_cmp(temp_str_2, motif_str) >= min_cmp_num) {
                  break_flag = false
                  current_pos += (motif_len + gap)
                  gap_time += 1
                  count += 1
                }
              }
            }
          }
          if(break_flag == true) {
            loop.break()
          }
        }
      }
    }
    //判断motif重复次数是否达到最小阈值
    if(count >= min_repeat){
      //进行重复覆盖SSR判断
      var flag = false
      for(i <- motif_pos + seq._2 to seq._2 + current_pos - 1){
        if(overlapping_check_array(i)==false){
          flag = true
        }
      }
      if(flag==true){
        return (count, current_pos - 1)
      }
    }
    return (0, 0)
  }

  //使用Kmer-SSR算法，返回SSRs，允许覆盖
/***********
  def searchSSRs_ao(seq_id:Int, seq_name:String, seq:(String, Int), kmer_k:Int): ArrayBuffer[(Int, String, String, Int, Int, Int)] ={
    var current_pos = 0
    val loop = Breaks
    var overlapp_arr:Array[Boolean] = new Array[Boolean](seq._1.length)
    for(i <- 0 to overlapp_arr.length-1){
      overlapp_arr(i) = false
    }
//    println(broadcast_len)
    for(k <- 0 to broadcast_len.value - 1){
      if(broadcast_ssr_split.value(k)._1==seq_id){
        for(j <- broadcast_ssr_split.value(k)._2 to broadcast_ssr_split.value(k)._3){
          overlapp_arr(j) = true
        }
      }
    }
    //    var ssr_rdd:RDD[String] = sc.parallelize(List())
//    var ssr_list:List[(Int, String, String, Int, Int, Int)] = List()
    val ssr_arr:ArrayBuffer[(Int, String, String, Int, Int, Int)] = new ArrayBuffer[(Int, String, String, Int, Int, Int)]()

    while(current_pos < seq._1.length()){
      val temp_res = searchSSR_ao(seq, current_pos, kmer_k)
      //      println("test2222")
      //      println("test@@@"+current_pos)
      if(temp_res._1==0 && temp_res._2==0){
        current_pos += 1
      } else{
        var flag = false
        for(j <- current_pos to temp_res._2){
          if(overlapp_arr(j)==false){
            flag = true
            overlapp_arr(j) = true
          }
        }
        if(flag==true) {
          //        ssr_list = ssr_list ++: List((seq_id, seq_name, seq._1.substring(current_pos, current_pos + kmer_k), temp_res._1, current_pos+seq._2, temp_res._2+seq._2))
          ssr_arr += ((seq_id, seq_name, seq._1.substring(current_pos, current_pos + kmer_k), temp_res._1, current_pos + seq._2, temp_res._2 + seq._2))
          if (is_ssr == true) {
            current_pos = temp_res._2 - kmer_k + 1
          } else {
            current_pos += 1
          }
        }
        //        ssr_rdd = ssr_rdd.union(sc.parallelize(List(seq._1.substring(current_pos, current_pos + kmer_k*temp_res))))
      }
    }
    //    println(ssr_list.length)
    //    sum += ssr_list.length
    ssr_arr
  }

*******************/
  //使用Kmer-SSR算法，返回SSRs
  def searchSSRs(seq_name:String, seq:(String, Int), kmer_k:Int): List[(String, String, Int, Int)] ={
    var current_pos = 0
    val loop = Breaks
//    var ssr_rdd:RDD[String] = sc.parallelize(List())
    var ssr_list:List[(String, String, Int, Int)] = List()

    while(current_pos < seq._1.length()){
      val temp_res = searchSSR(seq, current_pos, kmer_k)
//      println("test2222")
//      println("test@@@"+current_pos)
      if(temp_res._1==0 && temp_res._2==0){
        current_pos += 1
      } else{
        //将该SSR区域全部置为true
//        println("yes!!!!!!!!!!!!"+seq._1.substring(current_pos, current_pos + kmer_k*temp_res))
        for(i <- current_pos + seq._2 to temp_res._2 + seq._2){
          overlapping_check_array(i) = true
        }
        ssr_list = ssr_list ++: List((seq_name, seq._1.substring(current_pos, current_pos + kmer_k), temp_res._1, current_pos+seq._2))
        if(is_ssr==true) {
          current_pos = temp_res._2 - kmer_k + 1
        }else{
          current_pos += 1
        }
//        ssr_rdd = ssr_rdd.union(sc.parallelize(List(seq._1.substring(current_pos, current_pos + kmer_k*temp_res))))
      }
    }
//    println(ssr_list.length)
//    sum += ssr_list.length
    ssr_list
  }


  def getSSRs(seq_name:String, seq:String, k_list:List[Int], miss:Int = 0, MAX_GAP:Int = 0): RDD[(String,String, Int, Int)]={
    init()
    var res_rdd:RDD[(String, String, Int, Int)] = sc.parallelize(List()).repartition(num_partitions)
//    var res_list:List[String] = List()
//    res_rdd.repartition(8)
    for(k <- k_list) {
      var seq_rdd:RDD[(String, String, Int)] = sc.parallelize(List()).repartition(num_partitions)
//      var seq_list:List[(String, Int)] = List()
//      seq_rdd = seq_rdd.repartition(8)
      val res0 = split_non_recursive(seq_name, (seq, 0), k, miss, MAX_GAP)
      seq_rdd = seq_rdd.union(sc.parallelize(res0._1)).repartition(num_partitions)
      res_rdd = res_rdd.union(sc.parallelize(res0._2)).repartition(num_partitions)
//      res_list = res_list ++: res0._2

//    val seq_rdd = sc.parallelize(seq_list)
      println("Kmer-SSR-"+k.toString+":")
//      val temp = seq_rdd.collect().toList
//      temp.foreach(f=>println(f))
//    for(k <- k_list){
      println("test11111")
      val kmer_ssr_res = seq_rdd.flatMap(f=>{
        val res1:List[(String, String, Int, Int)] = searchSSRs(f._1, (f._2, f._3), k)
        res1
      }).repartition(num_partitions)
      res_rdd = res_rdd.union(kmer_ssr_res).repartition(num_partitions)
//      println("partitions num:" + res_rdd.getNumPartitions)
//      seq_rdd.foreach(f=>{
////        println(f._1)
//        val res1:List[String] = searchSSRs(f, k)
////        res1.foreach(f=>println(res1))
////        res_rdd = res_rdd.union(res1)
////        res_list = res_list ++: res1.collect().toList
//        res_list = res_list ++: res1
//        println("%%%%%%%%%%%%%%%%%%%")
//        res_list.foreach(f=>println(f))
//      })
    }
    println("$$$$$$$$$$$$$$$$$$$$$")

//    res_rdd = res_rdd.union(sc.parallelize(res_list))
    println("!!!!!!!!!!!!!!!!!!")
//    println(res_rdd.count())
//    res_rdd.foreach(f=>println(f))
//    println(res_rdd.getNumPartitions)
    res_rdd.count()
    res_rdd
  }

  def main(args:Array[String]): Unit ={
    val conf = new SparkConf()
      .setAppName("test")
      .set("spark.driver.maxResultSize", "30g")
//      .setMaster("local")
    sc=new SparkContext(conf)
//    val accumulator = sc.accumulator(0)
//    if(args(1)!=null) {
//      num_partitions = args(1).toInt
//    }
//    if(args(2)!=null) {
//      split_thresold = args(2).toInt
//    }
//    if(args(3)!=null) {
//      miss = args(3).toInt
//    }
//    if(args(4)!=null){
//      MAX_GAP = args(4).toInt
//    }
//    if(args(5)!=null){
//      acid_is_open = args(5).toBoolean
//    }
//    if(args(6)!=null){
//      is_ssr = args(6).toBoolean
//    }
//    overlapping_check_array = sc.broadcast(new Array[Boolean](1000000000))
//    val ht = new HT_SSR()
    /**************** this on spark **********************/
    val text_rdd = sc.textFile("hdfs://192.168.130.101:9000/user/xinong/"+args(0))
//val text_rdd = sc.textFile("hdfs://192.168.130.101:9000/user/xinong/iwgsc1_seq.fsa")
//    val text_rdd = sc.textFile("E:/刘全中老师/毕设/test_0505.txt")
//    val text_arr:Array[String] = text_rdd.collect()
//    val seq_arr:Array[String] = new Array[String](text_arr.length/2)
//    val sName_arr:Array[String] = new Array[String](text_arr.length/2)
    val input_rdd = text_rdd.cache()
    println("test11111111111!!!!!!!!!!!!!!!!!!!")
    /*************collect序列名称*********************/
    /**********
    sName_arr = text_rdd.filter(f => f.contains(">")).collect()
    val broadcast_sName = sc.broadcast(sName_arr)
    for(temp_ys <- sName_arr){
      println(temp_ys+"!!!!!!!!!!!!!!!!!!!!!!!!!!")
    }
      ***************/
    println("test222222222222222222222222222222")
//    var i = -1
    val seq_rdd_0 = input_rdd.filter(f => !f.contains(">"))
    val seq_rdd = seq_rdd_0.zipWithIndex().map(f=>{
//      (f._1, f._2.toInt, broadcast_sName.value(f._2.toInt))
      (f._1, f._2.toInt, "seq"+f._2.toInt)
    })
//    seq_rdd.foreach(f=>println(f._2))
/*****************  缓存序列rdd，暂时注释掉  *********************/
//    val seq_rdd_cache = seq_rdd.cache()
    val seq_rdd_cache = seq_rdd
//    seq_num = sName_arr.length
    val k_list:List[Int] = List(6)
//val k_list:List[Int] = List(2)
    //var split_res_rdd_cache = seq_rdd_cache.map(f=>(f._2, f._3, f._1, 0, 0, f._1.length()-1))

    if(is_split==true) {
      var split_res_rdd: RDD[(Int, String, String, Int, Int, Int)] = sc.emptyRDD[(Int, String, String, Int, Int, Int)]
      split_res_rdd = split_res_rdd.union(seq_rdd_cache.flatMap(f => {
        split_non_recursive3(f._2, f._3, (f._1, 0), k_list)
      })
      ).repartition(num_partitions)
      val split_res_rdd_cache = split_res_rdd.cache()
      //保存结果
      val ssr_rdd_split = split_res_rdd_cache.filter(f => f._4 != 0).coalesce(num_partitions, false)
      val ssr_rdd_split_cache = ssr_rdd_split.cache()
      val temp_arr = ssr_rdd_split_cache.map(f => (f._1, f._5, f._6, f._3)).collect()

      val broadcast_ssr_split = sc.broadcast(temp_arr)

      val broadcast_len = sc.broadcast(temp_arr.length)
      println(broadcast_len.value)


      //    println(ssr_rdd_split_cache.count())
      //    val split_res_rdd_cache = split_res_rdd.cache()

      //    ssr_rdd.coalesce(1, false).saveAsTextFile("hdfs://192.168.130.101:9000/user/xinong/test3_4_result_split")
      input_rdd.unpersist()
      //    split_res_rdd.foreach(f=>{println(f)})
      //    val temp_rdd_0 = split_res_rdd
      //    val temp_rdd_0 = sc.textFile("hdfs://192.168.130.101:9000/user/xinong/iwgsc_refseqv1.0_result_split")
      //    var id_temp = 0
      //    var ssr_rdd_wj:RDD[(String, String, Int, Int)] = sc.parallelize(List())
      //    var seq_rdd_wj:RDD[(String, String, Int, Int)] = sc.emptyRDD[(String, String, Int, Int)]
      //      val seq_list = List(0,1,2,3)
      //    val seq_list:List[(Int, RDD[(String, String, Int, Int)])] = List((0, sc.emptyRDD[(String, String, Int, Int)]), (1, sc.emptyRDD[(String, String, Int, Int)]), (2, sc.emptyRDD[(String, String, Int, Int)]), (3, sc.emptyRDD[(String, String, Int, Int)]))
      /** ************
        * allow overlapping
        */

      //    val ssr_rdd_wj = (0 until seq_num).map(seq_id=>{
      //      val temp_rdd_1 = split_res_rdd.coalesce(num_partitions, false).filter(f=>f._1==seq_id).coalesce(num_partitions, false)
      //      val temp_rdd_1_cache = temp_rdd_1.cache().coalesce(num_partitions, false)

      //      val temp = k_list.map(k=>{
      //        val seq_seg_rdd = temp_rdd_1_cache.filter(f=>f._2 == k).coalesce(num_partitions, false).map(f=>(f._1, f._3, (f._4, f._6), k))
      //        seq_seg_rdd.flatMap(f=>searchSSRs_ao(f._1, f._2, f._3)).coalesce(num_partitions, false)
      //      }).reduce(_ union _).coalesce(num_partitions, false)

      //    val ssr_rdd_wj = split_res_rdd.flatMap(f=>searchSSRs_ao(f._1,f._3, (f._4, f._6), f._2))
      val ssr_rdd_wj = split_res_rdd_cache.filter(f => f._4 == 0).flatMap(f => {
        val loop = Breaks
        val seq_id = f._1
        val seq = (f._3, f._5)
        val seq_name = f._2
        //  val kmer_k = f._2
        var overlapp_arr: Array[Boolean] = new Array[Boolean](seq._1.length)

        for (i <- 0 to overlapp_arr.length - 1) {
          overlapp_arr(i) = false
        }
        //    println(broadcast_len)
        /** *********check the overlapping splited ssr ******************/
        var splited_ssr_flag_left = false
        var splited_ssr_flag_right = false
        for (k <- 0 to broadcast_len.value - 1) {
          if (broadcast_ssr_split.value(k)._1 == seq_id) {
            for (j <- broadcast_ssr_split.value(k)._2 to broadcast_ssr_split.value(k)._3) {
              if (j < seq._2 + seq._1.length && j >= seq._2) {
                overlapp_arr(j - seq._2) = true
                //          splited_ssr_flag = true
              }
            }
            if (is_ssr == true) {
              if (broadcast_ssr_split.value(k)._2 <= seq._2 && broadcast_ssr_split.value(k)._3 >= seq._2) {
                splited_ssr_flag_left = true
                var extend_i = 1
                while (extend_i < broadcast_ssr_split.value(k)._4.length) {
                  if (broadcast_ssr_split.value(k)._4(extend_i - 1) == seq._1(broadcast_ssr_split.value(k)._3 - seq._2 + extend_i)) {
                    overlapp_arr(broadcast_ssr_split.value(k)._3 - seq._2 + extend_i) = true
                    extend_i += 1
                  }
                  else {
                    extend_i = broadcast_ssr_split.value(k)._4.length + 1
                  }
                }
              }
              if (broadcast_ssr_split.value(k)._3 >= seq._2 + seq._1.length - 1 && broadcast_ssr_split.value(k)._2 <= seq._2 + seq._1.length - 1) {
                splited_ssr_flag_right = true
                var extend_i = 1
                val temp_len = broadcast_ssr_split.value(k)._4.length
                while (extend_i < temp_len) {
                  if (broadcast_ssr_split.value(k)._4(temp_len - extend_i) == seq._1(broadcast_ssr_split.value(k)._2 - seq._2 - extend_i)) {
                    overlapp_arr(broadcast_ssr_split.value(k)._2 - seq._2 - extend_i) = true
                    extend_i += 1
                  }
                  else {
                    extend_i = broadcast_ssr_split.value(k)._4.length + 1
                  }
                }
              }
            }
          }
        }
        //    var ssr_rdd:RDD[String] = sc.parallelize(List())
        //    var ssr_list:List[(Int, String, String, Int, Int, Int)] = List()
        val ssr_arr: ArrayBuffer[(Int, String, String, Int, Int, Int)] = new ArrayBuffer[(Int, String, String, Int, Int, Int)]()

        for (kmer_k <- k_list) {
          var current_pos = 0
          while (current_pos < seq._1.length()) {
            val temp_res = searchSSR_ao(seq, current_pos, kmer_k)
            //      println("test2222")
            //      println("test@@@"+current_pos)
            if (temp_res._1 == 0 && temp_res._2 == 0) {
              current_pos += 1
            } else {
              var flag = false

              for (j <- current_pos to temp_res._2) {
                if (overlapp_arr(j) == false) {
                  flag = true
                  overlapp_arr(j) = true
                }
              }

              /** *delete this sentence ************/
              //      flag = true
              if (flag == true) {
                //        ssr_list = ssr_list ++: List((seq_id, seq_name, seq._1.substring(current_pos, current_pos + kmer_k), temp_res._1, current_pos+seq._2, temp_res._2+seq._2))
                ssr_arr += ((seq_id, seq_name, seq._1.substring(current_pos, current_pos + kmer_k), temp_res._1, current_pos + seq._2, temp_res._2 + seq._2))
                if (is_ssr == true) {
                  current_pos = temp_res._2 - kmer_k + 1
                } else {
                  current_pos = temp_res._2 - kmer_k + 1
                }
              } else {
                current_pos += 1
              }
              //        ssr_rdd = ssr_rdd.union(sc.parallelize(List(seq._1.substring(current_pos, current_pos + kmer_k*temp_res))))
            }
          }
        }
        ssr_arr
      })

      //    val ssr_rdd_pre = ssr_rdd_split_cache.union(ssr_rdd_wj).repartition(num_partitions)

      //    val ssr_rdd_sorted = ssr_rdd_pre.sortBy(f=>MySort(f._1, f._5, f._6, f._3.length)).cache()

      //    var ssr_list:List[(String, String, Int, Int, Int)] = List()
      //    ssr_rdd_pre.coalesce(1,false).saveAsTextFile("hdfs://192.168.130.101:9000/user/xinong/"+args(0)+"_result")
      //    ssr_rdd_sorted.foreach(f=>println(f))

      //    var ssr_list:List[(String, String, Int, Int, Int)] = List()
      /** **************
        * val ssr_arr:ArrayBuffer[(String, String, Int, Int, Int)] = new ArrayBuffer[(String, String, Int, Int, Int)]
        **
        *val ssr_arr_sorted = ssr_rdd_sorted.collect().toList
        * //    val temp_ssr_arr_sorted = ssr_rdd_sorted.filter(f=>f._1==i).collect()
        * var last_end = 0
        * for(i <- ssr_arr_sorted){
        * if(i._6>last_end){
        * //          ssr_list = ssr_list ++: List((i._2, i._3, i._4, i._5, i._6))
        * ssr_arr += ((i._2, i._3, i._4, i._5, i._6))
        * }
        * last_end = i._6
        * }
        * //    var seq_index_temp = 0
        * //    var last_end = 0
        * //    for(i <- ssr_arr_sorted){
        * //      if(i._1!=seq_index_temp){
        * //        seq_index_temp = i._1
        * //        ssr_list = ssr_list ++: List((i._2, i._3, i._4, i._5, i._6))
        * //      }
        * //      else{
        * //        if(i._6>last_end){
        * //          ssr_list = ssr_list ++: List((i._2, i._3, i._4, i._5, i._6))
        * //        }
        * //      }
        * //      last_end = i._6
        * //    }
        * ******************************/
      //    val ssr_rdd = sc.parallelize(ssr_arr)
      //    println(ssr_rdd_wj.count())
      val ssr_rdd = ssr_rdd_split_cache.union(ssr_rdd_wj).repartition(num_partitions)
      ssr_rdd.saveAsTextFile("hdfs://192.168.130.101:9000/user/xinong/" + args(0) + "_result")
    }
    else{
//      if(loop_type==false) {
        val split_res_rdd_cache = seq_rdd_cache.map(f => (f._2, f._3, f._1, 0, 0, f._1.length() - 1)).repartition(num_partitions).cache()
        val ssr_rdd_wj = split_res_rdd_cache.flatMap(f => {
          val loop = Breaks
          val seq_id = f._1
          val seq = (f._3, f._5)
          val seq_name = f._2
          //  val kmer_k = f._2
          var overlapp_arr: Array[Boolean] = new Array[Boolean](seq._1.length())

          for (i <- 0 to overlapp_arr.length - 1) {
            overlapp_arr(i) = false
          }
          //    println(broadcast_len)

          //    var ssr_rdd:RDD[String] = sc.parallelize(List())
          //    var ssr_list:List[(Int, String, String, Int, Int, Int)] = List()
          val ssr_arr: ArrayBuffer[(Int, String, String, Int, Int, Int)] = new ArrayBuffer[(Int, String, String, Int, Int, Int)]()
          val ssr_arr2: ArrayBuilder[(Int, String, String, Int, Int, Int)] = new mutable.ArrayBuilder.ofRef[(Int, String, String, Int, Int, Int)]()

          for (kmer_k <- k_list) {
            var current_pos = 0
            while (current_pos < seq._1.length()) {
              val temp_res = searchSSR_ao(seq, current_pos, kmer_k)
              //      println("test2222")
              //      println("test@@@"+current_pos)
              if (temp_res._1 == 0 && temp_res._2 == 0) {
                current_pos += 1
              } else {
                var flag = false

                for (j <- current_pos to temp_res._2) {
                  if (overlapp_arr(j) == false) {
                    flag = true
                    overlapp_arr(j) = true
                  }
                }

                /** *delete this sentence ************/
                //      flag = true
                if (flag == true) {
                  //        ssr_list = ssr_list ++: List((seq_id, seq_name, seq._1.substring(current_pos, current_pos + kmer_k), temp_res._1, current_pos+seq._2, temp_res._2+seq._2))
                  ssr_arr += ((seq_id, seq_name, seq._1.substring(current_pos, current_pos + kmer_k), temp_res._1, current_pos + seq._2, temp_res._2 + seq._2))
//                  ssr_arr.append((seq_id, seq_name, seq._1.substring(current_pos, current_pos + kmer_k), temp_res._1, current_pos + seq._2, temp_res._2 + seq._2))
                  if (is_ssr == true) {
                    current_pos = temp_res._2 - kmer_k + 1
                  } else {
                    current_pos = temp_res._2 - kmer_k + 1
                  }
                } else {
                  current_pos += 1
                }
                //        ssr_rdd = ssr_rdd.union(sc.parallelize(List(seq._1.substring(current_pos, current_pos + kmer_k*temp_res))))
              }
            }
          }
          ssr_arr
        })
        ssr_rdd_wj.saveAsTextFile("hdfs://192.168.130.101:9000/user/xinong/" + args(0) + "_result")
//        ssr_rdd_wj.saveAsTextFile("E:/刘全中老师/毕设/temp_test_0511")
//      }
//      else{
//        val split_res_rdd_cache = seq_rdd_cache.map(f => (f._2, f._3, f._1, 0, 0, f._1.length() - 1)).repartition(num_partitions).cache()
//        val ssr_rdd_wj = split_res_rdd_cache.flatMap(f=>{
//          val seq_id = f._1
//          val seq = (f._3, f._5)
//          val seq_name = f._2
//          //  val kmer_k = f._2
//
//          val ssr_arr: ArrayBuffer[(Int, String, String, Int, Int, Int)] = new ArrayBuffer[(Int, String, String, Int, Int, Int)]()
//
//          var current_pos = 0
//          while (current_pos < seq._1.length()) {
//            var flag = false
//            val loop = Breaks
//            loop.breakable {
//              for (kmer_k <- k_list) {
//                val temp_res = searchSSR_ao(seq, current_pos, kmer_k)
//                if (temp_res._1 != 0 || temp_res._2 != 0) {
////                  ssr_arr += ((seq_id, seq_name, seq._1.substring(current_pos, current_pos + kmer_k), temp_res._1, current_pos + seq._2, temp_res._2 + seq._2))
//                  ssr_arr.append((seq_id, seq_name, seq._1.substring(current_pos, current_pos + kmer_k), temp_res._1, current_pos + seq._2, temp_res._2 + seq._2))
//                  current_pos = temp_res._2 - kmer_k + 1
//                  flag = true
//                  loop.break()
//                }
//              }
//            }
//            if(flag==false){
//              current_pos += 1
//            }
//          }
//
//          ssr_arr
//        })
//        ssr_rdd_wj.saveAsTextFile("hdfs://192.168.130.101:9000/user/xinong/" + args(0) + "_result")
////        ssr_rdd_wj.saveAsTextFile("E:/刘全中老师/毕设/temp_test_0505")
//      }
    }
  //  ssr_rdd.coalesce(1, false).saveAsTextFile("E:/刘全中老师/毕设/"+args(0)+"_result_"+System.currentTimeMillis())
//    ssr_rdd.coalesce(1, false).saveAsTextFile("E:/刘全中老师/毕设/"+args(0)+"_gapresult_"+System.currentTimeMillis())
//    var ssr_rdd:RDD[(String, String, Int, Int, Int)] = sc.emptyRDD[(String, String, Int, Int, Int)]

    /*******************************8
      * do not allow overlapping
      */
    /***********************
    var ssr_list_wj:List[(String, String, Int, Int)] = List()
    (0 until seq_num).foreach(seq_id=>{
        init()
        val temp_rdd_1 = split_res_rdd.coalesce(num_partitions, false).filter(f=>f._1==seq_id).coalesce(num_partitions, false)
        val temp_rdd_1_cache = temp_rdd_1.cache().coalesce(num_partitions, false)
        val over_lapped_list_0 = temp_rdd_1_cache.filter(f=>f._5 != 0).coalesce(num_partitions, false)
  //      over_lapped_list_0.count()
        val over_lapped_list_1 = over_lapped_list_0.map(f=>(f._6, f._7)).coalesce(num_partitions, false)
        val over_lapped_list = over_lapped_list_1.collect().toList
        init2(over_lapped_list)

        val temp = k_list.map(k=>{
            val seq_seg_rdd = temp_rdd_1_cache.filter(f=>f._2 == k).coalesce(num_partitions, false).map(f=>(f._3, (f._4, f._6), k))
            seq_seg_rdd.flatMap(f=>searchSSRs(f._1, f._2, f._3)).coalesce(num_partitions, false)
        }).reduce(_ union _).coalesce(num_partitions, false)
        temp_rdd_1.unpersist()
        accumulator.add(temp.count().toInt)
//        temp.count()
//        val temp_cache = temp.cache()
//        println(temp.count())
//        temp.count()
//        println(temp.count())
//        temp.collect()
//        temp.collect()
//        temp.foreach(f=>println(f))
//        seq_rdd_wj = seq_rdd_wj.union(temp)
        ssr_list_wj = ssr_list_wj ++: temp.collect().toList
    })
      ***************************/

//    val temp_rdd_1 = split_res_rdd.coalesce(num_partitions, false).filter(f=>f._1==0).coalesce(num_partitions, false)
//    val temp_rdd_1_cache = temp_rdd_1.cache().coalesce(num_partitions, false)
//    val over_lapped_list_0 = temp_rdd_1_cache.filter(f=>f._5 != 0).coalesce(num_partitions, false)
//    //      over_lapped_list_0.count()
//    val over_lapped_list_1 = over_lapped_list_0.map(f=>(f._6, f._7)).coalesce(num_partitions, false)
//    val over_lapped_list = over_lapped_list_1.collect().toList
//    init2(over_lapped_list)
//
//    val temp = k_list.map(k=>{
//      val seq_seg_rdd = temp_rdd_1_cache.filter(f=>f._2 == k).coalesce(num_partitions, false).map(f=>(f._3, (f._4, f._6), k))
//      seq_seg_rdd.flatMap(f=>searchSSRs(f._1, f._2, f._3)).coalesce(num_partitions, false)
//    }).reduce(_ union _).coalesce(num_partitions, false)
////    temp_rdd_1.unpersist()
////    temp.count()
//    val temp_cache = temp.cache()
//    val temp_rdd_2 = split_res_rdd.coalesce(num_partitions, false).filter(f=>f._1==1).coalesce(num_partitions, false)
//    val temp_rdd_2_cache = temp_rdd_2.cache().coalesce(num_partitions, false)
//    val over_lapped_list_2 = temp_rdd_2_cache.filter(f=>f._5 != 0).coalesce(num_partitions, false)
//    //      over_lapped_list_0.count()
//    val over_lapped_list_3 = over_lapped_list_2.map(f=>(f._6, f._7)).coalesce(num_partitions, false)
//    val over_lapped_list2 = over_lapped_list_3.collect().toList
//    init2(over_lapped_list2)
//
//    val temp2 = k_list.map(k=>{
//      val seq_seg_rdd = temp_rdd_2_cache.filter(f=>f._2 == k).coalesce(num_partitions, false).map(f=>(f._3, (f._4, f._6), k))
//      seq_seg_rdd.flatMap(f=>searchSSRs(f._1, f._2, f._3)).coalesce(num_partitions, false)
//    }).reduce(_ union _).coalesce(num_partitions, false)
//    temp_rdd_2.unpersist()
//    val temp2_cache = temp2.cache()
//    val union_rdd = temp.union(temp2)
//    println(union_rdd.count())
//    println(seq_rdd_wj.count())
//  .reduce(_ union _)
//    val ssr_rdd = ssr_rdd_split_cache.union(sc.parallelize(ssr_list_wj))
//  .reduce(_ union _).coalesce(num_partitions, false)
//    println(seq_rdd_wj.count())
//    ssr_list.foreach(f=>println(f.count()))
    /**************
      ***********************************
    for(i <- 0 to seq_num - 1){
      init()
      val temp_rdd_1 = split_res_rdd_cache.filter(f=>f._1==i).coalesce(num_partitions, false)
      val temp_rdd_1_cache = temp_rdd_1.cache()
//      val array_broadcast = sc.broadcast(overlapping_check_array)
      val over_lapped_list_0 = temp_rdd_1_cache.filter(f=>f._5 != 0)
//      over_lapped_list_0.count()
      val over_lapped_list_1 = over_lapped_list_0.map(f=>(f._6, f._7)).coalesce(num_partitions, false)
      val over_lapped_list = over_lapped_list_1.collect().toList
      init2(over_lapped_list)
//      temp_rdd_1.filter(f=>f._5!=0).map(f=>(f._6, f._7)).foreach(f=>{
//        for(index <- f._1 to f._2){
//          overlapping_check_array(index) = true
//        }
//      })
//      println("第"+i+"次")
//      var j = 0
      val temp_ssr_rdd_union:RDD[(String, String, Int, Int)] = k_list.map(k=>{
        val seq_seg_rdd = temp_rdd_1_cache.filter(f=>f._2 == k).map(f=>(f._3, (f._4, f._6), k)).coalesce(num_partitions, false)
        seq_seg_rdd.flatMap(f=>searchSSRs(f._1, f._2, f._3)).coalesce(num_partitions, false)
      }).reduce(_ union _)
      /*************
      for(j <- k_list){
        val seq_seg_rdd = temp_rdd_1_cache.filter(f=>f._2 == j).map(f=>(f._3, (f._4, f._6), j)).coalesce(num_partitions, false)
        val temp_ssr_rdd = seq_seg_rdd.flatMap(f=>searchSSRs(f._1, f._2, f._3)).coalesce(num_partitions, false)
//        sum += temp_ssr_rdd.count().toInt
//        ssr_rdd_cache = ssr_rdd_cache.union(temp_ssr_rdd).coalesce(num_partitions, false)
        temp_ssr_rdd_union = temp_ssr_rdd_union.union(temp_ssr_rdd).coalesce(num_partitions, false)
//        ssr_rdd_cache.count()
//        ssr_rdd.count()
//        temp_ssr_rdd.saveAsTextFile("hdfs://192.168.130.101:9000/user/xinong/iwgsc_refseqv1.0_result_"+System.currentTimeMillis())
//        temp_ssr_rdd.saveAsTextFile("hdfs://192.168.130.101:9000/user/xinong/iwgsc1_seq_result_"+System.currentTimeMillis())
      }********************/
      ssr_rdd_cache = ssr_rdd_cache.union(temp_ssr_rdd_union).coalesce(num_partitions, false)
      temp_rdd_1_cache.unpersist()
//      ssr_rdd_cache.count()
    }
      *******************************************
      *******************/
//    println(sum)
    println("debug1111111111111111111111111")

//    val num = ssr_rdd_cache.count()
//    ssr_rdd_cache.coalesce(1,false).saveAsTextFile("hdfs://192.168.130.101:9000/user/xinong/"+args(0)+"_result_"+System.currentTimeMillis())

//    println(ssr_rdd.count())
//    println(accumulator.value)
//    ssr_rdd.coalesce(1,false).saveAsTextFile("hdfs://192.168.130.101:9000/user/xinong/"+args(0)+"_result_"+System.currentTimeMillis())
//    ssr_rdd.repartition(1).saveAsTextFile("E:/刘全中老师/毕设/"+args(0)+"_result_"+System.currentTimeMillis())
    sc.stop()

//    println(searchSSR(("TATTTTGCTCGCTGCTGCTCTGATATGACTCACAACCCCCGCTCCCGGTGTTCATCAGACGTTCGGTACACATAATAATAAATGAACATTCCGCGGGCGAGACGGAAAGCAACACAGGTCCCAGGGAGGGCGAAAAAGTGAAAAAAAGAGGGGGGGTCGAAATTTAATGATCGGGATAGTAAATAGAGCACAAATCATAATAAGAGGGGG", 0), 71, 3, 1, 1))
//    println(ifContain("abcaccabcdabcabc", 0, 3, 1, 1))
//    val seq_list = seq_rdd
    /**********
    for (i <- 0 to text_arr.length-1){
      println(text_arr(i)(0))
      if(i%2==0){
        sName_arr(i/2) = text_arr(i)
        seq_arr((i/2)) = text_arr(i+1)
      }
    }

    for(i <- 0 to (text_arr.length/2)-1){
      println(sName_arr(i)(1))
      println(seq_arr(i)(1))
    }
**************/
//    text_rdd.foreach(f=>println(f))
    /***********序列拼接
    println(text_rdd.count())
    val seq_array = text_rdd.filter(f=>(!f.contains(">gi"))).collect()
    var seq_str:String = ""
    for(i <- 0 to seq_array.size - 1){
      seq_str =  seq_str + seq_array(i)
    }
    val str_rdd = sc.parallelize(List(seq_str))
    str_rdd.repartition(1).saveAsTextFile("E:/刘全中老师/毕设/test_input")
      *****************/
//    println(seq_str)88
    //    seq_rdd.foreach(f=>println(f))

//    println(seq_str.length())
//    println(ifAcid("AAAAC"))
/*************序列挖掘
    val time0 = new Date()
//    var i = 0
//    var res:RDD[(String, String, Int, Int)] = sc.parallelize(List()).repartition(num_partitions)
//    var res_list:List[String, String, Int, Int]
    var j = 0
    println("debug222222222222222222222222222222222")



    for(i <- 0 to sName_arr.length-1){

    }
//    val arr:Array[RDD[(Int, String)]] = null
     val res_rdd = seq_rdd.foreach(f=>{
      val temp_rdd = getSSRs("seq", f, List(6,5,4,3,2), 0, 0)
      println(temp_rdd.count())
    })
/************
    for(i <- sName_arr) {
      val temp_rdd = getSSRs(i, seq_rdd(j), List(6, 5, 4, 3, 2), 0, 0)
      res = res.union(temp_rdd).repartition(num_partitions)
      j += 1
//      println(temp_res.count())
      //      println(res.count())
    }*************/
    val time1 = new Date()
    val time_0_1 = time1.getTime() - time0.getTime()
    val dateFormat:SimpleDateFormat  = new SimpleDateFormat("mm:ss.sssss")
    val time_01 = dateFormat.format(time_0_1)

//    val res = getSSRs("acacacxdxxdxxdxcccvcvcvcabcdeabcdeabcdebbbbbbbbccAacacacTAacacacTAacacacTA", List(8,5,3,2), sc)
//    println("SSR:")
//    println(res.count())
//    var i = 4
//    res.foreach(f=>{println(f)})
//    println("rdd_num:"+res.getNumPartitions)
//    println("rdd_count"+res_rdd.count())
//    println(res.partitions.length)

//    res.repartition(1).saveAsTextFile("hdfs://192.168.130.101:9000/user/xinong/iwgsc_refseqv1.0_result_"+System.currentTimeMillis())
//    res.repartition(1).saveAsTextFile("E:/刘全中老师/毕设/test3_4_gap"+System.currentTimeMillis())
    println("time:"+time_01)
    sc.stop()

//    println(i)

    //    val res = ht.split(("acacacxdxxdxxdxcccvcvcvcabcdeabcdeabcdebbbbbbbbccAacacacTAacacacTAacacacT", 0), 2, sc)
    //    res._1.foreach(f=>println(f))
    //    println("#######################")
    //    res._2.foreach(f=>println(f))

*********************/
  }

}

case class MySort(val seq_id:Int, val start:Int, val end:Int,val motif_len:Int) extends Ordered[MySort] with Serializable {
  override def compare(that: MySort): Int = {
    if (this.seq_id < that.seq_id) {
      -1
    }
    else if (this.seq_id > that.seq_id){
      1
    }
    else{
      if(this.start < that.start){
        -1
      }
      else if(this.start > that.start){
        1
      }
      else{
        if(this.end > that.end){
          -1
        }
        else if(this.end < that.end){
          1
        }
        else{
          if(this.motif_len > that.motif_len){
            -1
          }
          else{
            1
          }
        }
      }
    }
  }
}
/***
object HT_SSR {
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf()
        .setAppName("map")
        .setMaster("local")
    val sc=new SparkContext(conf)
    val ht = new HT_SSR()
    val res = ht.getSSRs("acacacxdxxdxxdxcccvcvcvcabcdeabcdeabcdebbbbbbbbccAacacacTAacacacTAacacacT", List(8,5,3,2), sc)
    res.foreach(f=>println(f))
//    val res = ht.split(("acacacxdxxdxxdxcccvcvcvcabcdeabcdeabcdebbbbbbbbccAacacacTAacacacTAacacacT", 0), 2, sc)
//    res._1.foreach(f=>println(f))
//    println("#######################")
//    res._2.foreach(f=>println(f))
  }
}
***/
