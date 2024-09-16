package io.appform.dropwizard.actors.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.codahale.metrics.Timer;
import io.appform.dropwizard.actors.config.RMQConfig;
import io.appform.dropwizard.actors.observers.ConsumeObserverContext;
import io.appform.dropwizard.actors.observers.PublishObserverContext;
import io.appform.dropwizard.actors.observers.RMQObserver;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * An Observer that ingests queue metrics.
 */
@Slf4j
public class RMQMetricObserver extends RMQObserver {
    private static final String PUBLISH = "publish";
    private static final String CONSUME = "consume";
    private final RMQConfig rmqConfig;
    private final MetricRegistry metricRegistry;
    private final boolean tenanted;
    private String tenantId = "";

    @Getter
    private final Map<MetricKeyData, MetricData> metricCache = new ConcurrentHashMap<>();

    public RMQMetricObserver(final RMQConfig rmqConfig,
                             final MetricRegistry metricRegistry) {
        super(null);
        this.rmqConfig = rmqConfig;
        this.metricRegistry = metricRegistry;
        this.tenanted = false;
    }

    public RMQMetricObserver(final RMQConfig rmqConfig,
        final MetricRegistry metricRegistry, boolean tenanted, String tenantId) {
        super(null);
        this.rmqConfig = rmqConfig;
        this.metricRegistry = metricRegistry;
        this.tenanted = tenanted;
        this.tenantId = tenantId;
    }

    @Override
    public <T> T executePublish(final PublishObserverContext context, final Supplier<T> supplier) {
        if (!MetricUtil.isMetricApplicable(rmqConfig.getMetricConfig(), context.getQueueName())) {
            return proceedPublish(context, supplier);
        }
        MetricData metricData;
        if(!tenanted)
            metricData = getMetricData(context);
        else metricData = getMetricData(context, tenantId);
        metricData.getTotal().mark();
        val timer = metricData.getTimer().time();
        try {
            val response = proceedPublish(context, supplier);
            metricData.getSuccess().mark();
            return response;
        } catch (Throwable t) {
            metricData.getFailed().mark();
            throw t;
        } finally {
            timer.stop();
        }
    }

    @Override
    public <T> T executeConsume(final ConsumeObserverContext context, final Supplier<T> supplier) {
        if (!MetricUtil.isMetricApplicable(rmqConfig.getMetricConfig(), context.getQueueName())) {
            return proceedConsume(context, supplier);
        }
        val isRedelivered = context.isRedelivered();
        MetricData metricData, metricDataForRedelivery;
        if(!tenanted) {
            metricData = getMetricData(context);
            metricDataForRedelivery = isRedelivered ? getMetricDataForRedelivery(context) : null;
        } else {
            metricData = getMetricData(context, tenantId);
            metricDataForRedelivery = isRedelivered ? getMetricDataForRedelivery(context, tenantId) : null;
        }

        metricData.getTotal().mark();
        if (metricDataForRedelivery != null) {
            metricDataForRedelivery.getTotal().mark();
        }
        val timer = metricData.getTimer().time();
        val redeliveryTimer =  metricDataForRedelivery != null ? metricDataForRedelivery.getTimer().time(): null;
        try {
            val response = proceedConsume(context, supplier);
            metricData.getSuccess().mark();
            if (metricDataForRedelivery != null) {
                metricDataForRedelivery.getSuccess().mark();
            }
            return response;
        } catch (Throwable t) {
            metricData.getFailed().mark();
            if (metricDataForRedelivery != null) {
                metricDataForRedelivery.getFailed().mark();
            }
            throw t;
        } finally {
            timer.stop();
            if (redeliveryTimer != null) {
                redeliveryTimer.stop();
            }
        }
    }

    private MetricData getMetricData(final PublishObserverContext context) {
        return getMetricData(MetricKeyData.builder()
            .queueName(context.getQueueName())
            .operation(PUBLISH)
            .build());
    }

    private MetricData getMetricData(final PublishObserverContext context, final String tenantId) {
        return getMetricData(MetricKeyData.builder()
            .queueName(context.getQueueName())
            .operation(PUBLISH)
            .tenantId(tenantId)
            .build());
    }

    private MetricData getMetricData(final ConsumeObserverContext context) {
        return getMetricData(MetricKeyData.builder()
                .queueName(context.getQueueName())
                .operation(CONSUME)
                .build());
    }

    private MetricData getMetricData(final ConsumeObserverContext context, final String tenantId) {
        return getMetricData(MetricKeyData.builder()
            .queueName(context.getQueueName())
            .operation(CONSUME)
            .tenantId(tenantId)
            .build());
    }

    private MetricData getMetricDataForRedelivery(final ConsumeObserverContext context) {
        return getMetricData(MetricKeyData.builder()
                .queueName(context.getQueueName())
                .operation(CONSUME)
                .redelivered(context.isRedelivered())
                .build());
    }

    private MetricData getMetricDataForRedelivery(final ConsumeObserverContext context, final String tenantId) {
        return getMetricData(MetricKeyData.builder()
            .queueName(context.getQueueName())
            .operation(CONSUME)
            .redelivered(context.isRedelivered())
            .tenantId(tenantId)
            .build());
    }

    private MetricData getMetricData(MetricKeyData metricKeyData) {
        return metricCache.computeIfAbsent(metricKeyData, key ->
            getMetricData(MetricUtil.getMetricPrefixForRedelivery(metricKeyData)));
    }

    private MetricData getMetricData(final String metricPrefix) {
        return MetricData.builder()
                .timer(metricRegistry.timer(MetricRegistry.name(metricPrefix, "latency"),
                        () -> new Timer(new SlidingTimeWindowArrayReservoir(60, TimeUnit.SECONDS))))
                .success(metricRegistry.meter(MetricRegistry.name(metricPrefix, "success")))
                .failed(metricRegistry.meter(MetricRegistry.name(metricPrefix, "failed")))
                .total(metricRegistry.meter(MetricRegistry.name(metricPrefix, "total")))
                .build();
    }
}