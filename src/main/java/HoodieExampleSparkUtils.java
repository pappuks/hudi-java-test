import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class HoodieExampleSparkUtils {

    private static Map<String, String> defaultConf() {
        Map<String, String> additionalConfigs = new HashMap<>();
        additionalConfigs.put("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        additionalConfigs.put("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar");
        additionalConfigs.put("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension");
        additionalConfigs.put("spark.kryoserializer.buffer.max", "512m");
        return additionalConfigs;
    }

    public static SparkConf defaultSparkConf(String appName) {
        return buildSparkConf(appName, defaultConf());
    }

    public static SparkConf buildSparkConf(String appName, Map<String, String> additionalConfigs) {

        SparkConf sparkConf = new SparkConf().setAppName(appName);
        additionalConfigs.forEach(sparkConf::set);
        return sparkConf;
    }

    public static SparkSession defaultSparkSession(String appName) {
        return buildSparkSession(appName, defaultConf());
    }

    public static SparkSession buildSparkSession(String appName, Map<String, String> additionalConfigs) {

        SparkSession.Builder builder = SparkSession.builder().appName(appName);
        additionalConfigs.forEach(builder::config);
        return builder.getOrCreate();
    }
}