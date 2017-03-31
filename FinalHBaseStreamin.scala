import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

object HBaseCPFStream extends Serializable {
	final val tableName = "trs"
	final val cfInfo = Bytes.toBytes("info")
	//final val colCPF = Bytes.toBytes("cpf")
	final val colData = Bytes.toBytes("data")
	final val colConta = Bytes.toBytes("conta")
	final val colTipo = Bytes.toBytes("tipo")

	// schema para informacao da transacao
	case class Trs(CPFid: String, date: String, conta: String, tipo: String)

	object Trs extends Serializable{
	//funcao para "parse-ar" linhas do CSV para clase Trs
		def parseTrs(str: String): Trs = {
			val p = str.split(",")
			Trs(p(0),p(1),p(2),p(3))
		}
	// Converte um linha do objecto Trs para um objeto put HBase
		def convertToPut(trs: Trs): (ImmutableBytesWritable, Put) = {
			// Criando o ID que sera inserido no HBase
			val rowkey = trs.CPFid
			val put = new Put(Bytes.toBytes(rowkey))
			// Adicionando familia de coluna Info, as colunas e seus valores em um objeto put
			put.add(cfInfo, colData, Bytes.toBytes(trs.date))
			put.add(cfInfo, colConta, Bytes.toBytes(trs.conta))
			put.add(cfInfo, colTipo, Bytes.toBytes(trs.tipo))
			return (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)
		}
	}
	
	def main(): Unit = {
		// setup da config da tabela HBase
		val conf = HBaseConfiguration.create()
		conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
		val jobConfig: JobConf = new JobConf(conf, this.getClass)
		jobConfig.set("mapreduce.output.fileoutputformat.outputdir", "file:///home/cloudera/out")
		jobConfig.setOutputFormat(classOf[TableOutputFormat])
		jobConfig.set(TableOutputFormat.OUTPUT_TABLE, tableName)

		val ssc = new StreamingContext(sc, Seconds(2))

		// "parse-ar" as linhas em objetos Trs
		val trsDStream = ssc.textFileStream("file:///home/cloudera/agulha").map(Trs.parseTrs)
		trsDStream.print()
	
		trsDStream.foreachRDD( { rdd =>
		// converte os dados de transacao em objeto put e escreve na coluna de familia info no HBase
		rdd.map(Trs.convertToPut).saveAsHadoopDataset(jobConfig)
		})
		
		ssc.start()
		ssc.awaitTermination()
	}
}

HBaseCPFStream.main()