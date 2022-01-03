package cloud.agileframework.data.common.config;

import com.alibaba.druid.filter.logging.LogFilterMBean;
import com.alibaba.druid.sql.SQLUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * @author 佟盟
 * 日期 2020-11-17 19:46
 * 描述 TODO
 * @version 1.0
 * @since 1.0
 */
@Configuration
@ConditionalOnClass(LogFilterMBean.class)
public class DruidExtendAutoConfiguration implements InitializingBean {
    @Autowired(required = false)
    private List<LogFilterMBean> logFilterList;

    @Override
    public void afterPropertiesSet() {
        if (logFilterList == null) {
            return;
        }
        logFilterList.forEach(n -> n.setStatementSqlFormatOption(new SQLUtils.FormatOption(false, false)));
    }
}
