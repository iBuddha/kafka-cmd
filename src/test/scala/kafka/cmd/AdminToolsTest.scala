package kafka.cmd

import java.util.Properties

import kafka.admin.AdminClient
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.utils.Utils

/**
  * Created by xhuang on 28/04/2017.
  */
object AdminToolsTest extends App{

  val client = createAdminClient()
  client.listAllGroupsFlattened().foreach(group => println(group.groupId))
  client.close()

  private def createAdminClient(): AdminClient = {
    val props =  new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    AdminClient.create(props)
  }
}
