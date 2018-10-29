package com.cchis.dctm.util.export.util;

import com.cchis.dctm.util.export.WebcHandler;
import org.apache.log4j.Logger;

import java.util.*;

import static com.cchis.dctm.util.export.util.ExportConstants.USE_BOOK_CACHE;

public class NaiveBookCache {
    private static final Logger LOG = Logger.getLogger(NaiveBookCache.class);
    private static final Map<String, CacheValues> MAP = new HashMap<>(); // should be placed before INSTANCE

    private static final NaiveBookCache INSTANCE = new NaiveBookCache();


    private final Map<String, CacheValues> cache = USE_BOOK_CACHE ? MAP : Collections.unmodifiableMap(MAP);

    private NaiveBookCache() { }

    public static NaiveBookCache getInstance() {
        return INSTANCE;
    }

    public void put(String configName, String key, Object object) {
        if (USE_BOOK_CACHE) {
            String parentConfigName = WebcHandler.getParentConfigName(configName);
            CacheValue value = new CacheValue(key, object);
            cache.merge(parentConfigName, new CacheValues(value), CacheValues::merge);
        }
    }

    public Object get(String configName, String key) {
        if (USE_BOOK_CACHE) {
            String parentConfigName = WebcHandler.getParentConfigName(configName);
            return cache.getOrDefault(parentConfigName, CacheValues.NULL).get(key);
        } else {
            throw new UnsupportedOperationException("Book Cache is switched off");
        }
    }

    public void clearCache(String configName) {
        if (USE_BOOK_CACHE) {
            LOG.debug(String.format("Clear cache for %s", configName));
            cache.remove(configName);
        }
    }

    public void clearCache() {
        if (USE_BOOK_CACHE) {
            LOG.debug("Clear cache");
            cache.clear();
        }
    }

    static class CacheValues {
        private static final CacheValues NULL = new CacheValues(new HashSet<>(), true);
        private Set<CacheValue> values;

        private Set<CacheValue> getValues() {
            return values;
        }

        CacheValues() {
            values = new HashSet<>();
        }

        CacheValues(CacheValue... values) {
            this();
            for (CacheValue value : values) {
                add(value);
            }
        }

        CacheValues(Set<CacheValue> vs, boolean unmodifiable) {
            values = new HashSet<>(vs);
            if (unmodifiable) {
                values = Collections.unmodifiableSet(values);
            }
        }

        void add(CacheValue value) {
            values.add(value);
        }

        CacheValues merge(CacheValues anotherValues) {
            values.addAll(anotherValues.getValues());
            return this;
        }

        Object get(String key) {
            return values.stream().filter(v -> v.getKey().equals(key)).findFirst().map(CacheValue::getValue).orElse(null);
        }
    }

    private static class CacheValue {
        private final String key;
        private final Object value;

        CacheValue(String key, Object value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public Object getValue() {
            return value;
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CacheValue that = (CacheValue) o;

            if (key != null ? !key.equals(that.key) : that.key != null) return false;
            return value != null ? value.equals(that.value) : that.value == null;
        }

        @Override
        public int hashCode() {
            int result = key != null ? key.hashCode() : 0;
            result = 31 * result + (value != null ? value.hashCode() : 0);
            return result;
        }
    }
}
