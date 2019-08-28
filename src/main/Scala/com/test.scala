
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

object test {
  def main(args: Array[String]): Unit = {
    val  conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val str = "A"
    val str2="B"
    println(str.hashCode)

  }
}

//(IM: 47544899705513,
// List((LC01,1),
// (LNbanner,1), (APP女性-and,1), (CN100016,1),
// 00010001,1), (D00020005,1), (D00030004,1), (ZP浙江省,1), (ZC绍兴市,1)))


//查询
//5 定义rowkey

//		Get rowkey=new Get(Bytes.toBytes("1001"));
//
//		//5 定义结果集
//
//		Result result=table.get(rowkey);
//
//		//6 定义单元格数组，并从result结果集中拿去数据转换单元格
//		Cell[] cells=result.rawCells();
//
//		//7 遍历单元格
//
//		for(Cell cell:cells) {
//
//			//取得内容 rowkey
//
//			System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
//
//			System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
//
//			System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
//
//			System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
//
//
//		}

//import org.apache.hadoop.hbase.util.Bytes
// 4 获得表的实例// 4 获得表的实例


//val table = connection.getTable(tableName)
//
//val delete = new Nothing(Bytes.toBytes("1001"))

