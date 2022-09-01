package cloud.agileframework.data.common.auth.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface AuthData {
    /**
     * 默认为开启状态
     *
     * @return false为关闭数据权限过滤
     */
    boolean enable() default true;

    /**
     * 场景
     *
     * @return 传递给数据权限过滤器用于sql模板渲染
     */
    String[] group() default {};
}
