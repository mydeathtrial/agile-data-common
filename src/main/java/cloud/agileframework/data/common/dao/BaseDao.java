package cloud.agileframework.data.common.dao;

import cloud.agileframework.common.DataException;
import cloud.agileframework.common.util.clazz.ClassUtil;
import cloud.agileframework.common.util.clazz.TypeReference;
import cloud.agileframework.common.util.object.ObjectUtil;
import cloud.agileframework.data.common.dictionary.DataExtendManager;
import cloud.agileframework.dictionary.util.TranslateException;
import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.PagerUtils;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.PagingAndSortingRepository;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author 佟盟 on 2017/11/15
 */
public interface BaseDao {

    Map<Class<?>, PagingAndSortingRepository> REPOSITORY_CACHE = new HashMap<>();

    DataExtendManager dictionaryManager();

    /**
     * 根据java类型获取对应的数据库表的JpaRepository对象
     *
     * @param tableClass 表对应的实体类型
     * @param <T>        表对应的实体类型
     * @param <ID>       主键类型
     * @return 对应的数据库表的JpaRepository对象
     */
    @SuppressWarnings("unchecked")
    <T, ID> PagingAndSortingRepository<T, ID> getRepository(Class<T> tableClass);

    /**
     * 保存
     *
     * @param o ORM对象
     */
    default <T> void save(T o) {
        if (o instanceof Class) {
            throw new DataException(new IllegalArgumentException("Parameter must be of type POJO"));
        }
        saveAndReturn(o);
    }

    /**
     * 批量保存
     *
     * @param list 表对应的实体类型的对象列表
     * @param <T>  表对应的实体类型
     * @return 是否保存成功
     */
    @SuppressWarnings("unchecked")
    default <T> boolean save(Iterable<T> list) {
        boolean isTrue = false;
        Iterator<T> iterator = list.iterator();
        if (iterator.hasNext()) {
            T obj = iterator.next();
            Class<T> tClass = (Class<T>) obj.getClass();
            getRepository(tClass).saveAll(list);
            isTrue = true;

        }
        return isTrue;
    }

    /**
     * 获取数据库连接
     *
     * @return Connection
     */
    Connection getConnection() throws SQLException;

    default <T> boolean contains(T o) {
        if (o instanceof Class) {
            throw new DataException(new IllegalArgumentException("Parameter must be of type POJO"));
        }
        Class<T> aClass = (Class<T>) o.getClass();
        PagingAndSortingRepository<T, Object> r = getRepository(aClass);

        Object id = getId(o);
        return r.existsById(id);
    }

    /**
     * 保存或更新
     *
     * @param o   已经有的对象更新，不存在的保存
     * @param <T> 泛型
     * @return 被跟踪对象
     */
    default <T> T saveOrUpdate(T o) {
        if (o instanceof Class) {
            throw new DataException(new IllegalArgumentException("Parameter must be of type POJO"));
        }
        Object id = getId(o);
        if (existsById(o.getClass(), id)) {
            deleteById(o.getClass(), id);
            return saveAndReturn(o, true);
        }
        return saveAndReturn(o);
    }


    /**
     * 保存并刷新
     *
     * @param o       表对应的实体类型的对象
     * @param isFlush 是否刷新
     * @param <T>     泛型
     * @return 保存后的对象
     */
    @SuppressWarnings("unchecked")
    default <T> T saveAndReturn(T o, boolean isFlush) {
        if (o instanceof Class) {
            throw new DataException(new IllegalArgumentException("Parameter must be of type POJO"));
        }
        Class<T> aClass = (Class<T>) o.getClass();
        PagingAndSortingRepository<T, Object> r = getRepository(aClass);
        T newObject = r.save(o);
        dictionaryManager().cover(newObject);
        return newObject;
    }

    /**
     * 保存
     *
     * @param o   要保存的对象
     * @param <T> 泛型
     * @return 保存后的对象
     */
    default <T> T saveAndReturn(T o) {
        if (o instanceof Class) {
            throw new DataException(new IllegalArgumentException("Parameter must be of type POJO"));
        }
        return saveAndReturn(o, Boolean.FALSE);
    }

    /**
     * 批量保存
     *
     * @param list 要保存的对象列表
     * @param <T>  表对应的实体类型
     * @return 保存后的数据集
     */
    @SuppressWarnings("unchecked")
    default <T> List<T> saveAndReturn(Iterable<T> list) {
        Iterator<T> iterator = list.iterator();
        if (iterator.hasNext()) {
            T obj = iterator.next();
            Class<T> clazz = (Class<T>) obj.getClass();
            return Lists.newArrayList(getRepository(clazz).saveAll(list));
        }
        return new ArrayList<>(0);
    }

    /**
     * 根据表实体类型与主键值，判断数据是否存在
     *
     * @param tableClass 表对应的实体类型
     * @param id         数据主键
     * @return 是否存在
     */
    default <T> boolean existsById(Class<T> tableClass, Object id) {
        PagingAndSortingRepository<T, Object> r = getRepository(tableClass);
        return r.existsById(toIdType(tableClass, id));
    }

    /**
     * 更新或新增
     *
     * @param o   ORM对象，瞬态对象时不会被跟踪
     * @param <T> 表对应的实体类型
     * @return 是否更新成功
     */
    default <T> boolean update(T o) {
        if (o instanceof Class) {
            throw new DataException(new IllegalArgumentException("Parameter must be of type POJO"));
        }
        Class<T> aClass = (Class<T>) o.getClass();
        Object id = getId(o);
        if (existsById(aClass, id)) {
            deleteById(aClass, id);
            saveAndReturn(o, true);
            return true;
        }

        return false;
    }

    /**
     * 更新或新增非空字段，空字段不进行更新
     *
     * @param o   表映射实体类型的对象
     * @param <T> 表映射实体类型的对象
     * @return 返回更新后的数据
     */
    @SuppressWarnings("unchecked")
    default <T> T updateOfNotNull(T o) {
        if (o instanceof Class) {
            throw new DataException(new IllegalArgumentException("Parameter must be of type POJO"));
        }
        Class<T> aClass = (Class<T>) o.getClass();
        Object id = getId(o);
        if (existsById(aClass, id)) {
            T old = findOne(aClass, id);
            ObjectUtil.copyProperties(old, o, ObjectUtil.Compare.DIFF_TARGET_NULL);
            deleteById(aClass, id);
            return saveAndReturn(o, true);
        }

        return null;
    }

    /**
     * 根据提供的对象参数，作为例子，查询出结果并删除
     *
     * @param o 表实体对象
     */
    default <T> void delete(T o) {
        if (o instanceof Class) {
            throw new DataException(new IllegalArgumentException("Parameter must be of type POJO"));
        }
        Class<T> aClass = (Class<T>) o.getClass();
        getRepository(aClass).delete(o);
    }

    /**
     * 删除
     *
     * @param tableClass 查询的目标表对应实体类型，Entity
     * @param id         删除的主键标识
     * @param <T>        查询的目标表对应实体类型
     */
    default <T> boolean deleteById(Class<T> tableClass, Object id) {
        PagingAndSortingRepository<T, Object> repository = getRepository(tableClass);
        try {
            repository.deleteById(toIdType(tableClass, id));
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    /**
     * 删除全部(逐一删除)
     *
     * @param tableClass 查询的目标表对应实体类型，Entity
     * @param <T>        查询的目标表对应实体类型
     */
    default <T> void deleteAll(Class<T> tableClass) {
        getRepository(tableClass).deleteAll();
    }

    /**
     * 删除全部(一次性删除)
     *
     * @param tableClass 查询的目标表对应实体类型，Entity
     * @param <T>        查询的目标表对应实体类型
     */
    default <T> void deleteAllInBatch(Class<T> tableClass) {
        deleteAll(tableClass);
    }

    /**
     * 根据主键与实体类型，部分删除，删除对象集(一次性删除)
     *
     * @param tableClass 查询的目标表对应实体类型，Entity
     * @param <T>        查询的目标表对应实体类型
     * @param ids        主键数组
     */
    default <T> void deleteInBatch(Class<T> tableClass, Object[] ids) {
        deleteInBatch(tableClass, Sets.newHashSet(ids));
    }

    default <T> void deleteInBatch(Class<T> tableClass, Iterable<?> ids) {
        if (ids == null) {
            return;
        }
        PagingAndSortingRepository<T, Object> repository = getRepository(tableClass);
        Set<Object> set = Sets.newHashSet();
        for (Object id : ids) {
            set.add(toIdType(tableClass, id));
        }
        repository.deleteAllById(set);
    }

    /**
     * 根据表映射类型的对象集合，部分删除，删除对象集(一次性删除)，无返回值
     *
     * @param list 需要删除的对象列表
     * @param <T>  删除对象集合的对象类型，用于生成sql语句时与对应的表进行绑定
     */
    default <T> void deleteInBatch(Iterable<T> list) {
        if (Iterables.isEmpty(list)) {
            return;
        }
        PagingAndSortingRepository<T, Object> repository = (PagingAndSortingRepository<T, Object>) getRepository(list.iterator().next().getClass());
        repository.deleteAll(list);
    }

    /**
     * 根据主键，查询单条
     *
     * @param clazz 查询的目标表对应实体类型，Entity
     * @param id    主键
     * @param <T>   查询的目标表对应实体类型
     * @return clazz类型对象
     */
    default <T> T findOne(Class<T> clazz, Object id) {
        PagingAndSortingRepository<T, Object> repository = getRepository(clazz);
        T newObject = repository.findById(id).orElse(null);
        dictionaryManager().cover(newObject);
        return newObject;
    }

    /**
     * 按照例子查询单条
     *
     * @param <T>    查询的表的映射实体类型
     * @param object 查询一句的例子对象
     * @return 返回查询结果
     */
    @SuppressWarnings("unchecked")
    default <T> T findOne(T object) {
        if (object instanceof Class) {
            throw new DataException(new IllegalArgumentException("Parameter must be of type POJO"));
        }
        PagingAndSortingRepository<T, Object> repository = (PagingAndSortingRepository<T, Object>) getRepository(object.getClass());
        T newObject = repository.findById(getId(object)).orElse(null);
        dictionaryManager().cover(newObject);
        return newObject;
    }

    /**
     * 根据sql查询出单条数据，并映射成指定clazz类型
     *
     * @param <T>        查询的表的映射实体类型
     * @param sql        sql
     * @param clazz      查询的目标表对应实体类型，Entity
     * @param parameters 对象数组格式的sql语句中的参数集合，使用?方式占位
     * @return 查询的结果
     */
    @SuppressWarnings("unchecked")
    default <T> T findOne(String sql, Class<T> clazz, Object... parameters) {
        T newObject = findBySQL(sql, clazz, parameters).stream().findFirst().orElse(null);
        dictionaryManager().cover(newObject);
        return newObject;
    }

    /**
     * 按照例子查询多条
     *
     * @param <T>    查询的表的映射实体类型
     * @param object 例子对象
     * @return 查询结果数据集合
     */
    default <T> List<T> findAll(T object) {
        if (object instanceof Class) {
            throw new DataException(new IllegalArgumentException("Parameter must be of type POJO"));
        }
        List<T> content = findBySQL(toSelectSql(object, Sort.unsorted(), DbType.mysql), (Class<T>) object.getClass(), null);
        dictionaryManager().cover(content);
        return content;
    }

    default <T> List<T> findAll(T object, Sort sort) {
        if (object instanceof Class) {
            throw new DataException(new IllegalArgumentException("Parameter must be of type POJO"));
        }
        List<T> content = findBySQL(toSelectSql(object, sort, DbType.mysql), (Class<T>) object.getClass(), null);
        dictionaryManager().cover(content);
        return content;
    }

    /**
     * 按照例子查询多条分页
     *
     * @param <T>    查询的表的映射实体类型
     * @param object 例子对象
     * @param page   第几页
     * @param size   每页条数
     * @return 分页对象
     */
    default <T> Page<T> page(T object, int page, int size) {
        if (object instanceof Class) {
            throw new DataException(new IllegalArgumentException("Parameter must be of type POJO"));
        }
        return page(object, page, size, Sort.unsorted());
    }

    /**
     * 按照例子对象查询多条分页
     *
     * @param <T>    查询的表的映射实体类型
     * @param object 例子对象
     * @param page   第几页
     * @param size   每页条数
     * @param sort   排序对象
     * @return 分页信息
     */
    default <T> Page<T> page(T object, int page, int size, Sort sort) {
        if (object instanceof Class) {
            throw new DataException(new IllegalArgumentException("Parameter must be of type POJO"));
        }
        return page(object, PageRequest.of(page, size, sort));
    }

    <T> Page<T> page(T object, PageRequest pageRequest);

    /**
     * 查询指定tableClass对应表的全表分页
     *
     * @param tableClass  查询的目标表对应实体类型，Entity
     * @param pageRequest 分页信息
     * @param <T>         目标表对应实体类型
     * @return 内容为实体的Page类型分页结果
     */
    default <T> Page<T> pageByClass(Class<T> tableClass, PageRequest pageRequest) {
        Page<T> pageInfo = getRepository(tableClass).findAll(pageRequest);
        dictionaryManager().cover(pageInfo.getContent());
        return pageInfo;
    }

    /**
     * 查询指定tableClass对应表的全表分页
     *
     * @param tableClass 查询的目标表对应实体类型，Entity
     * @param page       第几页
     * @param size       页大小
     * @param <T>        目标表对应实体类型
     * @return 内容为实体的Page类型分页结果
     */
    default <T> Page<T> pageByClass(Class<T> tableClass, int page, int size) {
        return pageByClass(tableClass, PageRequest.of(page - 1, size));
    }

    /**
     * 分页查询
     *
     * @param sql        查询的sql语句
     * @param pageable   分页信息
     * @param parameters 对象数组类型的参数集合
     * @return Page类型的查询结果
     */
    @SuppressWarnings("unchecked")
    <T> Page<T> pageBySQL(String sql, PageRequest pageable, Class<T> clazz, Object... parameters);

    default <T> Page<T> pageBySQL(String sql, int page, int size, Class<T> clazz, Object... parameters) {
        return pageBySQL(sql, PageRequest.of(page - 1, size, Sort.unsorted()), clazz, parameters);
    }

    /**
     * 指定tableClass对应表的全表查询
     *
     * @param tableClass 查询的目标表对应实体类型，Entity
     * @param <T>        目标表对应实体类型
     * @return 内容为实体的List类型结果集
     */
    default <T> List<T> findAllByClass(Class<T> tableClass) {
        Iterable<T> list = getRepository(tableClass).findAll();
        dictionaryManager().cover(list);
        return Lists.newArrayList(list);
    }

    /**
     * 指定tableClass对应表的全表查询,并排序
     *
     * @param tableClass 查询的目标表对应实体类型，Entity
     * @param sort       排序信息
     * @param <T>        目标表对应实体类型
     * @return 内容为实体的List类型结果集
     */
    default <T> List<T> findAllByClass(Class<T> tableClass, Sort sort) {
        Iterable<T> list = getRepository(tableClass).findAll(sort);
        dictionaryManager().cover(list);
        return Lists.newArrayList(list);
    }

    <T> List<T> findBySQL(String sql, Class<T> clazz, Object... parameters);

    /**
     * 根据sql语句查询指定类型clazz列表
     *
     * @param sql         查询的sql语句，参数使用？占位
     * @param clazz       希望查询结果映射成的实体类型
     * @param <T>         指定返回类型
     * @param firstResult 第一条数据
     * @param maxResults  最大条数据
     * @param parameters  对象数组类型的参数集合
     * @return 结果集
     */
    @SuppressWarnings("unchecked")
    <T> List<T> findBySQL(String sql, Class<T> clazz, Integer firstResult, Integer maxResults, Object... parameters);

    /**
     * 根据sql语句查询列表，结果类型为List<Map<String, Object>>
     *
     * @param sql        查询sql语句，参数使用{Map的key值}形式占位
     * @param parameters Map类型参数集合
     * @return 结果类型为List套Map的查询结果
     */
    List<Map<String, Object>> findBySQL(String sql, Object... parameters);

    /**
     * sql形式写操作
     *
     * @param sql        查询的sql语句，参数使用？占位
     * @param parameters 对象数组形式参数集合
     * @return 影响条数
     */
    int updateBySQL(String sql, Object... parameters);

    /**
     * 根据实体类型tableClass与主键值集合ids，查询实体列表
     *
     * @param tableClass 查询的目标表对应实体类型，Entity
     * @param ids        主键值集合
     * @param <T>        目标表对应实体类型
     * @return 返回查询出的实体列表
     */
    default <T> List<T> findAllById(Class<T> tableClass, Iterable<Object> ids) {
        Iterable<T> list = getRepository(tableClass).findAllById(ids);
        dictionaryManager().cover(list);
        return Lists.newArrayList(list);
    }

    /**
     * 根据实体类型tableClass与主键值集合ids，查询实体列表
     *
     * @param tableClass 查询的目标表对应实体类型，Entity
     * @param ids        主键值集合，数组类型
     * @param <T>        目标表对应实体类型
     * @return 返回查询出的实体列表
     */
    default <T> List<T> findAllByArrayId(Class<T> tableClass, Object... ids) {
        Iterable<T> list = getRepository(tableClass).findAllById(Sets.newHashSet(ids));
        dictionaryManager().cover(list);
        return Lists.newArrayList(list);
    }

    /**
     * 查询指定tableClass对应表的总数
     *
     * @param tableClass 查询的目标表对应实体类型，Entity
     * @return 查询条数
     */
    default long count(Class<?> tableClass) {
        return getRepository(tableClass).count();
    }


    /**
     * 批量插入
     *
     * @param list 要保存的数据集合
     */
    default <T> void batchInsert(List<T> list) {
        batchInsert(list, 1000);
    }

    /**
     * 批量更新
     *
     * @param list 要更新的数据集合
     */
    default <T> void batchUpdate(List<T> list) {
        batchUpdate(list, 1000);
    }

    /**
     * 批量删除
     *
     * @param list 要删除的数据集合
     */
    default <T> void batchDelete(List<T> list) {
        batchDelete(list, 1000);
    }

    /**
     * 批量插入
     *
     * @param list      要保存的数据集合
     * @param batchSize 多少条执行一次插入
     */
    default <T> void batchInsert(List<T> list, int batchSize) {
        save(list);
    }

    /**
     * 批量更新
     *
     * @param list      要更新的数据集合
     * @param batchSize 多少条执行一次更新
     */
    default <T> void batchUpdate(List<T> list, int batchSize) {
        for (Object o : list) {
            update(o);
        }
    }

    /**
     * 批量删除
     *
     * @param list      要删除的数据集合
     * @param batchSize 多少条执行一次删除
     */
    default <T> void batchDelete(List<T> list, int batchSize) {
        PagingAndSortingRepository<Object, Object> re = getRepository((Class<Object>) list.iterator().next().getClass());
        re.deleteAll(list);
    }

    /**
     * 获取ORM中的主键字段
     *
     * @param clazz 查询的目标表对应实体类型，Entity
     * @return 主键属性
     */
    default Field getIdField(Class<?> clazz) {
        Set<ClassUtil.Target<Id>> e = ClassUtil.getAllEntityAnnotation(clazz, Id.class);
        Member member = e.iterator().next().getMember();
        if (member instanceof Field) {
            return (Field) member;
        }
        String methodName = member.getName();
        if (methodName.startsWith("get")) {
            return ClassUtil.getField(clazz, methodName.substring(3));
        }
        throw new DataException(new NoSuchFieldException("没找到主键字段"));
    }

    default Object getId(Object o) {
        try {
            return getIdField(o.getClass()).get(o);
        } catch (IllegalAccessException e) {
            throw new DataException(e);
        }
    }

    default void setId(Object o, Object id) {
        try {
            final Field idField = getIdField(o.getClass());
            idField.setAccessible(true);
            idField.set(o, ObjectUtil.to(id, new TypeReference<>(idField.getType())));
        } catch (IllegalAccessException e) {
            throw new DataException(e);
        }
    }

    /**
     * 根据ORM类型取主键类型
     *
     * @param clazz 主键java类型
     * @return 主键java类型
     */
    default Class<?> getIdType(Class<?> clazz) {
        return getIdField(clazz).getType();
    }

    /**
     * 把id转换为clazz实体的主键类型
     *
     * @param clazz 实体类型
     * @param id    主键
     * @return 转换后的主键
     */
    default Object toIdType(Class<?> clazz, Object id) {
        return ObjectUtil.to(id, new TypeReference<>(getIdType(clazz)));
    }

    /**
     * 对象转换为sql
     *
     * @param o      对象
     * @param dbType 数据库类型
     * @param <T>    泛型
     * @return sql
     */
    default <T> String toSelectSql(T o, Sort sort, DbType dbType) {
        if (o instanceof Class || o == null) {
            throw new DataException(new IllegalArgumentException("Parameter must be of type POJO"));
        }

        TableWrapper<T> tableWrapper = new TableWrapper<>(o, this::toColumnNames, this::toTableName);

        SQLSelectQueryBlock query = new SQLSelectQueryBlock();
        query.setFrom(new SQLExprTableSource(tableWrapper.getTableName()));
        tableWrapper.getColumns().stream().map(ColumnName::getName).forEach(e -> query.addSelectItem(new SQLSelectItem(SQLUtils.toSQLExpr(e))));
        tableWrapper.getColumns().stream().filter(e -> e.getValue().isPresent())
                .map(e -> e.sql(dbType))
                .reduce((a, b) -> new SQLBinaryOpExpr(a, SQLBinaryOperator.BooleanAnd, b)).ifPresent(query::setWhere);

        sort.stream().forEach(s -> {
            query.addOrderBy(new SQLOrderBy(SQLUtils.toSQLExpr(s.getProperty()), s.getDirection().isAscending() ? SQLOrderingSpecification.ASC : SQLOrderingSpecification.DESC));
        });

        return SQLUtils.toSQLString(query, dbType);
    }

    default <T> String toUpdateSql(T o, DbType dbType) {
        if (o instanceof Class || o == null) {
            throw new DataException(new IllegalArgumentException("Parameter must be of type POJO"));
        }

        TableWrapper<T> tableWrapper = new TableWrapper<>(o, this::toColumnNames, this::toTableName);

        SQLUpdateStatement update = new SQLUpdateStatement();
        //from
        update.setTableSource(new SQLExprTableSource(tableWrapper.getTableName()));

        //where
        tableWrapper.getColumns().stream().filter(ColumnName::isPrimaryKey)
                .filter(e -> e.getValue().isPresent())
                .map(e -> e.sql(dbType))
                .reduce((a, b) -> new SQLBinaryOpExpr(a, SQLBinaryOperator.BooleanAnd, b)).ifPresent(update::setWhere);

        //item
        tableWrapper.getColumns().stream().filter(e -> !e.isPrimaryKey()).filter(e -> e.getValue().isPresent())
                .forEach(f -> {
                    SQLUpdateSetItem updateSetItem = new SQLUpdateSetItem();
                    updateSetItem.setColumn(SQLUtils.toSQLExpr(f.getName(), dbType));
                    updateSetItem.setValue(f.sqlValue());
                    update.addItem(updateSetItem);
                });
        return SQLUtils.toSQLString(update, dbType);
    }

    default <T> String toInsertSql(T o, DbType dbType) {
        if (o instanceof Class || o == null) {
            throw new DataException(new IllegalArgumentException("Parameter must be of type POJO"));
        }

        TableWrapper<T> tableWrapper = new TableWrapper<>(o, this::toColumnNames, this::toTableName);

        SQLInsertStatement insert = new SQLInsertStatement();
        //from
        insert.setTableSource(new SQLExprTableSource(tableWrapper.getTableName()));

        SQLInsertStatement.ValuesClause values = new SQLInsertStatement.ValuesClause();
        //item
        tableWrapper.getColumns().stream().filter(e -> e.getValue().isPresent())
                .forEach(f -> {
                    insert.addColumn(SQLUtils.toSQLExpr(f.getName(), dbType));
                    values.addValue(f.sqlValue());
                });
        insert.addValueCause(values);

        return SQLUtils.toSQLString(insert, dbType);
    }

    default <T> String toInsertSql(List<T> list, DbType dbType) {
        if (list == null || list.isEmpty()) {
            throw new DataException(new IllegalArgumentException("Parameter contains at least one element"));
        }

        List<TableWrapper<T>> rows = list.stream().map(c -> new TableWrapper<>(c, this::toColumnNames, this::toTableName)).collect(Collectors.toList());

        SQLInsertStatement insert = new SQLInsertStatement();

        rows.forEach(row -> {
            if (insert.getTableName() == null) {
                //from
                insert.setTableSource(new SQLExprTableSource(row.getTableName()));

                SQLInsertStatement.ValuesClause values = new SQLInsertStatement.ValuesClause();
                //item
                row.getColumns().stream().filter(e -> e.getValue().isPresent())
                        .forEach(f -> {
                            insert.addColumn(SQLUtils.toSQLExpr(f.getName(), dbType));
                            values.addValue(f.sqlValue());
                        });

                insert.addValueCause(values);
            }
        });

        return SQLUtils.toSQLString(insert, dbType);
    }

    default <T> String toDeleteSql(T o, DbType dbType) {
        if (o instanceof Class || o == null) {
            throw new DataException(new IllegalArgumentException("Parameter must be of type POJO"));
        }

        TableWrapper<T> tableWrapper = new TableWrapper<>(o, this::toColumnNames, this::toTableName);

        SQLDeleteStatement delete = new SQLDeleteStatement();
        //from
        delete.setTableSource(new SQLExprTableSource(tableWrapper.getTableName()));

        //where
        tableWrapper.getColumns().stream()
                .filter(e -> e.getValue().isPresent())
                .map(e -> e.sql(dbType))
                .reduce((a, b) -> new SQLBinaryOpExpr(a, SQLBinaryOperator.BooleanAnd, b)).ifPresent(delete::setWhere);

        return SQLUtils.toSQLString(delete, dbType);
    }

    <T> List<ColumnName> toColumnNames(Class<T> clazz);

    <T> String toTableName(Class<T> clazz);

    default <T> String toPageSQL(T o, PageRequest pageRequest, DbType dbType) {
        if (o instanceof Class) {
            throw new DataException(new IllegalArgumentException("Parameter must be of type POJO"));
        }
        String select = toSelectSql(o, pageRequest.getSort(), dbType);
        int pageSize = pageRequest.getPageSize();
        int pageNumber = pageRequest.getPageNumber();
        return PagerUtils.limit(select, DbType.mysql, pageNumber * pageSize, pageSize);
    }

    default <T> String toPageCountSQL(T o, PageRequest pageRequest, DbType dbType) {
        if (o instanceof Class) {
            throw new DataException(new IllegalArgumentException("Parameter must be of type POJO"));
        }
        String select = toSelectSql(o, pageRequest.getSort(), dbType);
        return PagerUtils.count(select, DbType.mysql);
    }

    /**
     * 将对象转换为字段与值的映射结构
     *
     * @param o   需要转换的对象
     * @param <T> 泛型
     * @return 映射
     */
    default <T> Map<String, Optional<Object>> toColumnValueMapping(T o) {
        if (o instanceof Class || o == null) {
            throw new DataException(new IllegalArgumentException("Parameter must be of type POJO"));
        }

        Class<?> tableClass = o.getClass();
        Set<ColumnName> fields = toColumnNames(tableClass)
                .stream()
                .filter(c -> Arrays.stream(((AccessibleObject) (c.getMember())).getAnnotations()).noneMatch(annotation ->
                        "Transient".equals(annotation.annotationType().getSimpleName())))
                .collect(Collectors.toSet());
        Map<String, Optional<Object>> map = fields.stream().filter(f -> f.getMember() instanceof Field)
                .collect(Collectors.toMap(ColumnName::getName, f -> Optional.ofNullable(ObjectUtil.getFieldValue(o, (Field) f.getMember()))));

        Map<String, Optional<Object>> map2 = fields.stream().filter(f -> f.getMember() instanceof Method)
                .collect(Collectors.toMap(ColumnName::getName, f -> {
                    try {
                        Method method = (Method) f.getMember();
                        method.setAccessible(true);
                        return Optional.ofNullable(method.invoke(o));
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        e.printStackTrace();
                    }
                    return Optional.empty();
                }));

        map.putAll(map2);
        return map;
    }

    class ColumnValueMapping {
        private ColumnName column;
        private String value;
    }
}
