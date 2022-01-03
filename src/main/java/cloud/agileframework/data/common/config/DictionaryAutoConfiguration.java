package cloud.agileframework.data.common.config;

import cloud.agileframework.data.common.dictionary.DataExtendManager;
import cloud.agileframework.data.common.dictionary.DictionaryManager;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author 佟盟
 * 日期 2020/8/00010 15:51
 * 描述 TODO
 * @version 1.0
 * @since 1.0
 */
@Configuration
@AutoConfigureAfter(cloud.agileframework.dictionary.config.DictionaryAutoConfiguration.class)
@ConditionalOnClass(cloud.agileframework.dictionary.util.DictionaryUtil.class)
public class DictionaryAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean({DataExtendManager.class})
    DataExtendManager dataExtendManager() {
        return new DictionaryManager();
    }
}
