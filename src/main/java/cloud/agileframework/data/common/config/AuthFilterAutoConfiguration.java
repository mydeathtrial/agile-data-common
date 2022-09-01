package cloud.agileframework.data.common.config;

import cloud.agileframework.data.common.auth.AuthDataAround;
import cloud.agileframework.data.common.auth.AuthDataProperties;
import cloud.agileframework.data.common.auth.AuthFilter;
import com.alibaba.druid.pool.DruidDataSource;
import org.springframework.beans.BeansException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(prefix = "agile.data.auth", name = {"enable"})
@EnableConfigurationProperties(AuthDataProperties.class)
public class AuthFilterAutoConfiguration implements ApplicationContextAware {
    private ApplicationContext applicationContext;

    public AuthFilterAutoConfiguration() {
    }

    @Bean
    @ConditionalOnClass(name = "org.springframework.security.core.userdetails.UserDetails")
    public AuthFilter authFilter(AuthDataProperties authDataProperties) {
        return new AuthFilter(authDataProperties);
    }

    @Bean
    @ConditionalOnBean(AuthFilter.class)
    public AuthDataAround authDataAround(AuthFilter authFilter) {
        applicationContext.getBeanProvider(DruidDataSource.class)
                .forEach(dataSource -> dataSource.setPoolPreparedStatements(false));

        return new AuthDataAround(authFilter);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
