package kafka.cmd.common

class JaasConfig(val username: String, val password: String) {
  private val template = "org.apache.kafka.common.security.plain.PlainLoginModule required\n" + "  username=\"__USERNAME__\"\n" + "  password=\"__PASSWORD__\";"
  private var config = template
  config = config.replace("__USERNAME__", username)
  config = config.replace("__PASSWORD__", password)

  def getConfig: String = config

  override def toString: String = config
}