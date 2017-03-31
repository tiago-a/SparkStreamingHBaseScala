# SparkStreamingHBaseScala
Carregando dados de uma tabela fictícia em formato .csv para uma tabela no HBase através do Spark Streaming

Abrir a VM da Cloudera Quickstart 5.8
Esperar ela carregar tudo, se não da problema

abre um terminal
```
$ service --status-all
```
```
Hadoop datanode is running                                 [  OK  ]
Hadoop journalnode is running                              [  OK  ]
Hadoop namenode is running                                 [  OK  ]
Hadoop secondarynamenode is running                        [  OK  ]
Hadoop httpfs is running                                   [  OK  ]
Hadoop historyserver is running                            [  OK  ]
Hadoop nodemanager is running                              [  OK  ]
Hadoop proxyserver is dead and pid file exists             [FAILED]
Hadoop resourcemanager is running                          [  OK  ]
hald (pid  1736) is running...
HBase master daemon is running                             [  OK  ]
hbase-regionserver is running
HBase rest daemon is running                               [  OK  ]
HBase Solr Indexer is not running                          [FAILED]
HBase thrift daemon is running                             [  OK  ]
Hive Metastore is running                                  [  OK  ]
Hive Server2 is running                                    [  OK  ]

.
.
.

zookeeper-server is running
```
Esses serviços tem que estar assim

No mesmo terminal
```
$ hbase shell
```

Criar tabela pra inserir os dados
```
> create 'trs', 'info'
```
0 row(s) in 2.5840 seconds

=> Hbase::Table - trs

Veja se ela foi criada mesmo
```
> list
```
TABLE                                                                                                                                                        
trs                                                                                                                                                          
1 row(s) in 0.0290 seconds

=> ["trs"]

Criar duas pastas, uma chamada 'agulha' e outra 'out' no home do usuario (/home/cloudera/)
copiar o arquivo output001.csv

Abrir um novo terminal
```
$ spark-shell --master local[2]
```

Copiar e colar o script no spark-shell

```
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
```

O Spark Streaming está lendo a pasta agulha que está sem arquivo nenhum

Agora abra outro terminal
```
$ cp output001.csv agulha/file1
```

Prono, agora o Spark Streaming deve consumir os dados no formato .csv, transformar em RDDs salvar na pasta out (abrindo ela um arquivo _SUCCESS deve estar lá)
depois ele pega o RDD da pasta out, cria um objeto Put pra gravar no HBase e grava na tabela 'trs'

Para ver os dados basta entrar pelo Hue ou pelo hbase shell mesmo

Pelo Hue:
Abra o Firefox
No endereço
>quickstart.cloudera:8888

caso peça senha
>usuario: cloudera

>senha: cloudera

clique em Data Browsers
depois Hbase
e depois na tabela trs

Os primeiros dados devem estar na forma
ID como o sendo o CPF da pessoa
Coluna de familia info e colunas data, conta e tipo

O primeiro registro deve se esse
```
100.106.508-32
info:data		info:conta		info:tipo
5-8-1914		9823			DOC
```
Para ver pelo hbase shell basta voltar no terminal que ele estava aberto
```
> scan 'trs', {LIMIT=>10}
```
