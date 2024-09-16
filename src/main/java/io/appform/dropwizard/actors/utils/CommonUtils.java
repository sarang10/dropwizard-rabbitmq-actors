/*
 * Copyright (c) 2019 Santanu Sinha <santanu.sinha@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.appform.dropwizard.actors.utils;

import com.google.common.base.Strings;
import io.appform.dropwizard.actors.common.Constants;
import io.appform.dropwizard.actors.config.RMQConfig;
import io.appform.dropwizard.actors.connectivity.ConnectionConfig;
import java.util.Objects;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CommonUtils {

    public static final String UNKNOWN_TENANT = "Unknown tenant: ";

    public static boolean isEmpty(Collection<?> collection) {
        return null == collection || collection.isEmpty();
    }

    public static boolean isEmpty(Map<?, ?> map) {
        return null == map || map.isEmpty();
    }

    public static boolean isEmpty(String s) {
        return Strings.isNullOrEmpty(s);
    }

    public static boolean isRetriable(Set<String> retriableExceptions, Throwable exception) {
        return CommonUtils.isEmpty(retriableExceptions)
                || (null != exception
                && retriableExceptions.contains(exception.getClass().getSimpleName()));
    }

    public static int determineThreadPoolSize(String connectionName, RMQConfig rmqConfig) {
        if (Constants.DEFAULT_CONNECTIONS.contains(connectionName)) {
            return rmqConfig.getThreadPoolSize();
        }

        if (rmqConfig.getConnections() == null) {
            return Constants.DEFAULT_THREADS_PER_CONNECTION;
        }

        return rmqConfig.getConnections().stream()
            .filter(x -> Objects.equals(x.getName(), connectionName))
            .findAny()
            .map(ConnectionConfig::getThreadPoolSize)
            .orElse(Constants.DEFAULT_THREADS_PER_CONNECTION);
    }
}
