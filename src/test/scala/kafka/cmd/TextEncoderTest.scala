package kafka.cmd

import com.twitter.bijection.avro.GenericAvroCodecs
import com.twitter.bijection.{Base64String, Bijection, Injection}
import kafka.cmd.common.utils.TextEncoder
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by xhuang on 26/04/2017.
  */
class TextEncoderTest extends FlatSpec with Matchers {
  "TextEncoder" should "encode byte[] to base64 string correctly" in {
    val bytes = Injection.long2BigEndian(11L)
    val base64String = Bijection.bytes2Base64(bytes).str
    val myEncoded = TextEncoder.encodeBase64(bytes)
    myEncoded shouldEqual  base64String
  }

  it should "decode avro data from binary format to string" in {
    val schemaStr = "{\"namespace\":\"JavaSessionize.avro\",\"type\":\"record\",\"name\":\"LogLine\",\"fields\":[{\"name\":\"ip\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"long\"},{\"name\":\"url\",\"type\":\"string\"},{\"name\":\"referrer\",\"type\":\"string\"},{\"name\":\"useragent\",\"type\":\"string\"},{\"name\":\"sessionid\",\"type\":[\"null\",\"int\"],\"default\":null}]}"
    val now = System.currentTimeMillis()
    val schema = new Schema.Parser().parse(schemaStr)
    val record = new GenericData.Record(schema)
    record.put("ip", "192.168.0.1")
    record.put("timestamp", now)
    record.put("url", "www.google.com")
    record.put("referrer", "tom")
    record.put("useragent", "firefox")
    record.put("sessionid", 1024)
    val injection = GenericAvroCodecs.toBinary[GenericRecord](schema)
    val bytes = injection(record)
    val jsonStr = TextEncoder.encodeAvro(bytes, schemaStr)
    println(jsonStr)
    val reversedRecord = GenericAvroCodecs.toJson[GenericRecord](schema).invert(jsonStr).get
    reversedRecord shouldEqual record



  }

}
