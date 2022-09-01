package cloud.agileframework.data.common.auth;

import cloud.agileframework.data.common.auth.annotation.AuthData;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.data.util.ProxyUtils;

import java.lang.reflect.Method;


@Aspect
public class AuthDataAround {
    private final AuthFilter authFilter;

    public AuthDataAround(AuthFilter authFilter) {
        this.authFilter = authFilter;
    }

    @Around("@annotation(cloud.agileframework.data.common.auth.annotation.AuthData)")
    public Object roundAsp(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        Signature signature = proceedingJoinPoint.getSignature();

        MethodSignature methodSignature = (MethodSignature) signature;
        Method method = methodSignature.getMethod();
        AuthData authData = method.getAnnotation(AuthData.class);
        if (authData == null) {
            Object bean = proceedingJoinPoint.getTarget();
            authData = ProxyUtils.getUserClass(bean).getAnnotation(AuthData.class);
        }
        authFilter.setConfig(authData);

        Object proceed;
        try {
            Object[] args = proceedingJoinPoint.getArgs();
            proceed = proceedingJoinPoint.proceed(args);
        } catch (Throwable throwable) {
            authFilter.clear();
            throw throwable;
        }

        authFilter.clear();
        return proceed;
    }
}
