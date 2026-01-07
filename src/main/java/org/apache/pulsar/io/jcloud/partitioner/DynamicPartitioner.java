/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.io.jcloud.partitioner;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig;

public class DynamicPartitioner<T> extends AbstractPartitioner<T> {

    private static final Pattern TOKEN = Pattern.compile("\\$\\{([^}]+)}");

    // Defaults
    private static final String DEFAULT_TIME_PATTERN = "yyyyMMddHH";
    private static final String DEFAULT_TIME_ZONE = "UTC";

    // Settings (from config)
    private String template;
    private boolean failOnMissing;
    private ZoneId zoneId;

    // Prebuilt formatter fallback (por si no ponen patrón en token)
    private DateTimeFormatter defaultTimeFormatter;

    @Override
    public void configure(BlobStoreAbstractConfig config) {
        super.configure(config);

        this.template = StringUtils.defaultIfBlank(config.getDynamicPartitionTemplate(), "");
        if (StringUtils.isBlank(this.template)) {
            throw new IllegalArgumentException("dynamicPartitionTemplate must be set when partitionerType=DYNAMIC");
        }

        this.failOnMissing = config.isDynamicFailOnMissing();

        String tz = StringUtils.defaultIfBlank(config.getDynamicTimeZone(), DEFAULT_TIME_ZONE);
        this.zoneId = ZoneId.of(tz);

        this.defaultTimeFormatter = new DateTimeFormatterBuilder()
                .appendPattern(DEFAULT_TIME_PATTERN)
                .toFormatter();
    }

    @Override
    public String encodePartition(Record<T> sinkRecord) {
        Matcher m = TOKEN.matcher(template);
        StringBuffer out = new StringBuffer();

        while (m.find()) {
            String expr = m.group(1).trim();
            String resolved = resolve(expr, sinkRecord);

            if (resolved == null) {
                if (failOnMissing) {
                    throw new IllegalArgumentException("Unable to resolve token ${" + expr + "} for record");
                }
                resolved = "";
            }

            m.appendReplacement(out, Matcher.quoteReplacement(sanitizePathSegment(resolved)));
        }

        m.appendTail(out);
        return normalizePrefix(out.toString());
    }

    /**
     * Resolves supported tokens from the template.
     *
     * <p>Supported tokens:
     * <ul>
     *   <li>${prop:org} - properties["org"]</li>
     *   <li>${time:yyyyMMddHH} - eventTime if present, otherwise current time</li>
     *   <li>${eventTime:yyyyMMddHH} - requires eventTime</li>
     *   <li>${org} - shorthand for properties["org"]</li>
     * </ul>
     */
    private String resolve(String expr, Record<T> r) {
        if (expr.startsWith("prop:")) {
            String key = expr.substring("prop:".length());
            String v = getProp(r, key);
            if (v != null) {
                return v;
            }
            // Fallback útil: si no existe property, intenta campo en payload con el mismo key
            return getField(r, key);
        }

        if (expr.startsWith("field:")) {
            String key = expr.substring("field:".length());
            return getField(r, key);
        }

        if (expr.startsWith("time:")) {
            String pattern = StringUtils.defaultIfBlank(expr.substring("time:".length()), DEFAULT_TIME_PATTERN);
            long t = r.getEventTime().orElse(0L);
            if (t <= 0) {
                t = System.currentTimeMillis();
            }
            return formatTime(t, pattern);
        }

        if (expr.startsWith("eventTime:")) {
            String pattern = StringUtils.defaultIfBlank(expr.substring("eventTime:".length()), DEFAULT_TIME_PATTERN);
            Long t = r.getEventTime().orElse(null);
            if (t == null || t <= 0) {
                return null;
            }
            return formatTime(t, pattern);
        }

        // shorthand: ${org}
        String v = getProp(r, expr);
        if (v != null) {
            return v;
        }
        return getField(r, expr);
    }

    private String getField(Record<T> r, String key) {
        Object value = null;
        try {
            value = r.getValue();
        } catch (Throwable ignored) {
            // ignore
        }

        if (value == null) {
            return null;
        }

        // Caso 1: Pulsar GenericObject / GenericRecord (muy común con GenericAvroSchema)
        if (value instanceof GenericObject) {
            GenericObject go = (GenericObject) value;
            Object nativeObj = go.getNativeObject();

            if (nativeObj instanceof org.apache.pulsar.client.api.schema.GenericRecord) {
                org.apache.pulsar.client.api.schema.GenericRecord pr =
                        (org.apache.pulsar.client.api.schema.GenericRecord) nativeObj;
                Object field = pr.getField(key);
                return field != null ? String.valueOf(field) : null;
            }

            if (nativeObj instanceof GenericRecord) {
                GenericRecord ar = (GenericRecord) nativeObj;
                Object field = ar.get(key);
                return field != null ? String.valueOf(field) : null;
            }
        }

        // Caso 2: Avro GenericRecord directo
        if (value instanceof GenericRecord) {
            GenericRecord ar = (GenericRecord) value;
            Object field = ar.get(key);
            return field != null ? String.valueOf(field) : null;
        }

        // Caso 3: Map (si algún formato entrega map)
        if (value instanceof Map) {
            Map<?, ?> m = (Map<?, ?>) value;
            Object field = m.get(key);
            return field != null ? String.valueOf(field) : null;
        }

        return null;
    }

    private String getProp(Record<T> r, String key) {
        Map<String, String> props = r.getProperties();
        if (props == null) {
            return null;
        }
        return props.get(key);
    }

    private String formatTime(long epochMillis, String pattern) {
        DateTimeFormatter fmt;
        if (DEFAULT_TIME_PATTERN.equals(pattern)) {
            fmt = defaultTimeFormatter;
        } else {
            fmt = new DateTimeFormatterBuilder().appendPattern(pattern).toFormatter();
        }

        ZonedDateTime zdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), zoneId);
        return fmt.format(zdt);
    }

    private static String sanitizePathSegment(String s) {
        if (s == null) {
            return "";
        }
        // Avoid path injection.
        return s.trim().replace("/", "_").replace("\\", "_");
    }

    private static String normalizePrefix(String s) {
        String x = s.replaceAll("/{2,}", "/");
        while (x.startsWith("/")) {
            x = x.substring(1);
        }
        if (!x.endsWith("/")) {
            x = x + "/";
        }
        return x;
    }
}
