package io.appform.dropwizard.actors.base.utils;

import io.appform.dropwizard.actors.utils.CommonUtils;
import lombok.experimental.UtilityClass;

@UtilityClass
public class NamingUtils {
    public static final String NAMESPACE_ENV_NAME = "NAMESPACE_ENV_NAME";
    public static final String NAMESPACE_PROPERTY_NAME = "rmq.actors.namespace";

    public String queueName(String prefix, String name) {
        final String nameWithPrefix = String.format("%s.%s", prefix, name);
        return prefixWithNamespace(nameWithPrefix);
    }

    public String getTenantedName(String tenantId, String name) {
        return String.format("%s.%s", tenantId, name);
    }

    public String sanitizeMetricName(String metric) {
        return metric == null ? null : metric.replaceAll("[^A-Za-z\\-0-9]", "").toLowerCase();
    }

    public String prefixWithNamespace(String name) {
        String namespace = System.getenv(NAMESPACE_ENV_NAME);
        namespace = CommonUtils.isEmpty(namespace)
                    ? System.getProperty(NAMESPACE_PROPERTY_NAME, "")
                    : namespace;
        if (CommonUtils.isEmpty(namespace)) {
            return name;
        }
        return String.format("%s.%s", namespace, name);
    }

    public String getShardedQueueName(String queueName, int shardId) {
        return queueName + "_" + shardId;
    }

    public String getSideline(String queueName) {
        return String.format("%s_%s", queueName, "SIDELINE");
    }
}
