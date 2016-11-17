-------------------- cache loader ----------------
import java.io.Serializable;

import com.google.common.cache.CacheLoader;

public class MdcodeCacheLoader extends CacheLoader<String, Integer> implements Serializable {

    private static final long serialVersionUID = 1L;

    @Override
    public Integer load(String key) throws Exception {
        return 0;
    }

}

------------------- cache ---------------------
import java.io.Serializable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;

public class MdcodeCache implements Serializable {

    private static final long serialVersionUID = 1L;
    private static Logger logger = LoggerFactory.getLogger(MdcodeCache.class);
    
    LoadingCache<String, Integer> cache = null;

    public MdcodeCache() {
        cache = CacheBuilder.newBuilder()
                .expireAfterWrite(2, TimeUnit.HOURS)
                .build(new MdcodeCacheLoader());
    }
    
    public Integer get(String key) {
        try {
            cache.put(key, cache.get(key) + 1);
            return cache.get(key);
        } catch (ExecutionException e) {
            logger.error("MdcodeCache.get error!" + key, e);;
            return -1;
        }
    }
    
    public boolean exist(String key) {
        try {
            cache.put(key, cache.get(key) + 1);
            if (cache.get(key) > 1) {
                return true;
            }
        } catch (ExecutionException e) {
            logger.error("MdcodeCache.exit error!" + key, e);;
        }
        return false;
    }

}
