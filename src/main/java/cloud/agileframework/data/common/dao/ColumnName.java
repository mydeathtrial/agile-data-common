package cloud.agileframework.data.common.dao;

import cloud.agileframework.common.util.number.NumberUtil;
import cloud.agileframework.common.util.string.StringUtil;
import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLTimestampExpr;

import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.Objects;
import java.util.Optional;
import java.util.TimeZone;

public class ColumnName {
    //字段名
    private String name;
    //标注字段注解的属性或者方法，如@Column或@Field
    private Member member;
    private Optional<Object> value;
    private boolean primaryKey;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Member getMember() {
        return member;
    }

    public void setMember(Member member) {
        this.member = member;
        if (name != null) {
            return;
        }
        if (member instanceof Field) {
            name = member.getName();
        }
        if (member instanceof Method && member.getName().startsWith("get")) {
            name = StringUtil.toLowerName(name.substring(3));
        }
        if (member instanceof Method && member.getName().startsWith("is") && ((Method) member).getReturnType() == Boolean.class) {
            name = StringUtil.toLowerName(name.substring(2));
        }
    }

    public Optional<Object> getValue() {
        return value;
    }

    public void setValue(Optional<Object> value) {
        this.value = value;
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(boolean primaryKey) {
        this.primaryKey = primaryKey;
    }

    public SQLBinaryOpExpr sql(DbType dbType) {
        Object v = getValue().orElse(null);
        if (v == null) {
            return new SQLBinaryOpExpr(SQLUtils.toSQLExpr(getName(), dbType), SQLBinaryOperator.Is, SQLUtils.toSQLExpr(null, dbType));
        }
        SQLExpr right = sqlValue();
        return new SQLBinaryOpExpr(SQLUtils.toSQLExpr(getName()), SQLBinaryOperator.Equality, right);
    }

    public SQLExpr sqlValue() {
        Object v = getValue().orElse(null);
        if (v == null) {
            return null;
        }

        SQLExpr right;
        if (NumberUtil.isNumber(v.getClass()) || v instanceof Boolean) {
            right = SQLUtils.toSQLExpr(Objects.toString(v));
        } else if (v instanceof Date) {
            right = new SQLTimestampExpr((Date) v, TimeZone.getDefault());
        } else {
            right = SQLUtils.toSQLExpr(String.format("'%s'", v));
        }
        return right;
    }


}
