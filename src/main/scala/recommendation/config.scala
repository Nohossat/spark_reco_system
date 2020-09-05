package recommendation
import com.typesafe.config.ConfigFactory

class Config {
    val user = ConfigFactory.load().getString("mongo.user.value")
    val pwd = ConfigFactory.load().getString("mongo.pwd.value")

    def printConfig() : Unit = {
        println(s"My user is $user")
        println(s"My pwd is $pwd")
    }
}