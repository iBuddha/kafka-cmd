package kafka.cmd.common

import com.typesafe.config.Config
import org.apache.kafka.common.config.SaslConfigs

import java.util.Properties
import java.util.concurrent.ConcurrentHashMap

object KafkaConf {
    private val _properties = new ConcurrentHashMap[String, String]()

    def properties: Properties = {
        val result = new Properties();
        val iter = _properties.entrySet().iterator()
        while (iter.hasNext) {
            val entry = iter.next()
            result.put(entry.getKey, entry.getValue)
        }
        result
    }

    def load(config: Config) = {
        val iter = config.entrySet().iterator()
        while(iter.hasNext) {
            val entry = iter.next();
            if (entry.getKey.startsWith("kafka.property.")) {
                val property = entry.getKey.replace("kafka.property.", "")
                _properties.put(property, entry.getValue.unwrapped().toString)
            }
        }
        if (_properties.containsKey("username") && _properties.containsKey("password")) {
            val jaasConfig = new JaasConfig(_properties.get("username"), _properties.get("password"))
            _properties.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig.toString)
            _properties.remove("username")
            _properties.remove("password")
        }
    }
}
