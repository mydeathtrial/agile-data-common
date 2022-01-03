package cloud.agileframework.data.common.dao;

import cloud.agileframework.common.util.object.ObjectUtil;
import org.springframework.data.annotation.Transient;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TableWrapper<T> {
    private final T o;
    private final List<ColumnName> columns;
    private final String tableName;

    public TableWrapper(T o, Function<Class<T>, List<ColumnName>> toColumnNamesFunction, Function<Class<T>, String> toTableName) {
        this.o = o;
        Class<T> tClass = (Class<T>) o.getClass();
        this.tableName = toTableName.apply(tClass);
        columns = toColumnNamesFunction.apply(tClass).stream()
                .filter(c -> c.getMember().getDeclaringClass().getAnnotation(Transient.class) == null)
                .collect(Collectors.toList());


        columns.stream().filter(f -> f.getMember() instanceof Field)
                .forEach(f -> f.setValue(Optional.ofNullable(ObjectUtil.getFieldValue(o, (Field) f.getMember()))));

        columns.stream().filter(f -> f.getMember() instanceof Method)
                .forEach(f -> {
                    try {
                        Method method = (Method) f.getMember();
                        method.setAccessible(true);
                        f.setValue(Optional.ofNullable(method.invoke(o)));
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        e.printStackTrace();
                    }
                    f.setValue(Optional.empty());
                });
    }

    public T getO() {
        return o;
    }

    public List<ColumnName> getColumns() {
        return columns;
    }

    public String getTableName() {
        return tableName;
    }
}
