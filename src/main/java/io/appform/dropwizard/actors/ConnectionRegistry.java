package io.appform.dropwizard.actors;

import io.appform.dropwizard.actors.common.RabbitmqActorException;
import io.appform.dropwizard.actors.config.RMQConfig;
import io.appform.dropwizard.actors.connectivity.RMQConnection;
import io.appform.dropwizard.actors.observers.RMQObserver;
import io.appform.dropwizard.actors.utils.CommonUtils;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Environment;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

@Slf4j
@Data
public class ConnectionRegistry implements Managed {

    private final ConcurrentHashMap<String, RMQConnection> connections;
    private final Environment environment;
    private final ExecutorServiceProvider executorServiceProvider;
    private final RMQConfig rmqConfig;
    private TtlConfig ttlConfig;
    private RMQObserver rootObserver;

    public ConnectionRegistry(final Environment environment,
                              final ExecutorServiceProvider executorServiceProvider,
                              final RMQConfig rmqConfig,
                              final TtlConfig ttlConfig,
                              final RMQObserver rootObserver) {
        this.environment = environment;
        this.executorServiceProvider = executorServiceProvider;
        this.rmqConfig = rmqConfig;
        this.ttlConfig = ttlConfig;
        this.connections = new ConcurrentHashMap<>();
        this.rootObserver = rootObserver;
    }

    public RMQConnection createOrGet(String connectionName) {
        val threadPoolSize = CommonUtils.determineThreadPoolSize(connectionName, this.rmqConfig);
        return createOrGet(connectionName, threadPoolSize);
    }

    public RMQConnection createOrGet(String connectionName, int threadPoolSize) {

        return connections.computeIfAbsent(connectionName, connection -> {
            log.info(String.format("Creating new RMQ connection with name [%s] having [%d] threads", connection,
                    threadPoolSize));
            val rmqConnection = new RMQConnection(
                    connection,
                    rmqConfig,
                    executorServiceProvider.newFixedThreadPool(String.format("rmqconnection-%s", connection),
                            threadPoolSize),
                    environment, ttlConfig, rootObserver);
            try {
                rmqConnection.start();
            } catch (Exception e) {
                throw RabbitmqActorException.propagate(e);
            }
            log.info(String.format("Created new RMQ connection with name [%s]", connection));
            return rmqConnection;
        });
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {
        connections.forEach(new BiConsumer<String, RMQConnection>() {
            @SneakyThrows
            @Override
            public void accept(String name, RMQConnection rmqConnection) {
                rmqConnection.stop();
            }
        });
    }
}
