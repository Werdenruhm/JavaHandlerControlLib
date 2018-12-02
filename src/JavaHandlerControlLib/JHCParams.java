package JavaHandlerControlLib;

import CommonLib.Common;
import CommonModelLib.dbModel.IEnumCache;
import DBmethodsLib.DBNull;
import DBmethodsLib.DBmethodsCommon;
import DBmethodsLib.DataRow;
import DBmethodsLib.DataTable;
import DBmethodsLib.AnyTable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Objects;

/**
 *
 * 
 */
public class JHCParams 
{
    final DBmethodsCommon dbmethods;
    final String handlerTypeAlias;
    final String handlerInstanceTypeAlias;
    final Object[] paramss;
    final DBmethodsLib.AnyTable paramsTable;
    public JHCParams(DBmethodsCommon dbmethods, String handlerTypeAlias, String handlerInstanceTypeAlias, Object[] paramss, Common.Action1<DataTable> onParamsMustBeRefreshed)
    {
        this.dbmethods = dbmethods;
        this.handlerTypeAlias = handlerTypeAlias;
        this.handlerInstanceTypeAlias = handlerInstanceTypeAlias;
        this.paramss = paramss;
        paramsTable = new AnyTable(dbmethods, 
            "select * from JavaHandlerControlParam where handlerType_en is null" 
                + (handlerTypeAlias == null 
                    ? 
                        "" 
                    : 
                        " or (handlerType_en = " + IEnumCache.enum_idSQL(handlerTypeAlias) + " and (handlerInstanceType_en is null" 
                        + (handlerInstanceTypeAlias == null 
                            ? 
                                "" 
                            : 
                                " or handlerInstanceType_en = " + IEnumCache.enum_idSQL(handlerInstanceTypeAlias)
                        ) 
                        + "))"
                )            
            + " order by COALESCE(handlerInstanceType_en * 1000000, 0) + COALESCE(handlerType_en, 0) desc",
            new String[] { "paramname" }, "pooled", -1, true, onParamsMustBeRefreshed
        ) 
        { 
            DBmethodsLib.AnyTable.DBCACHERESET cacheReset = new DBmethodsLib.AnyTable.DBCACHERESET(dbmethods, "select JavaHandlerControlVar_getCACHERESET()", 30);
            @Override
            protected LocalDateTime getCACHERESET() { return cacheReset.get(); }                 
        };
    }

    public static class fillParamssByDBRESULT { public boolean changedParamThatRequiresHandlerRestartOnChange; public String paramNamesThatChangedAndThatRequiresHandlerRestartOnChange; }
    public final fillParamssByDBRESULT fillParamssByDB()
    {
        fillParamssByDBRESULT result = new fillParamssByDBRESULT();
        DataTable dt = paramsTable.get();
        for(Object params : paramss)
        {
            Field[] fields = params.getClass().getDeclaredFields();
            for(Field f : fields){
                try
                {
                    JHCParamBehaviour[] behA = f.getAnnotationsByType(JHCParamBehaviour.class);
                    JHCParamBehaviour beh =  behA.length > 0 ? behA[0] : null;
                    boolean requiresHandlerRestartOnChange = beh == null ? false : beh.requiresHandlerRestartOnChange();
                    boolean isRequired = beh == null ? true : beh.isRequired();

                    DataRow[] drs = dt.Select(new DataTable.SelectFilter("paramname", DataTable.SelectFilterType.EqualsCI, f.getName()));
                    if (drs.length == 0)
                    {
                        if (isRequired)
                            throw new Common.ArtificialException("параметр не задан в JavaHandlerControlParam");
                        else
                            continue;
                    }
                    DataRow dr = drs[0];
                    Class t = f.getType();

                    if (!t.getName().equals(dr.get("paramvaljavatype").toString()))
                        throw new Common.ArtificialException("paramvaljavatype (" + dr.get("paramvaljavatype").toString() + ") не совпадает с типом сооветствующего поля объекта params (" + t.getName() + ")!");
                    
                    Object valueBefore = f.get(params);

                    if (dr.get("paramvaltype").toString().equals("c"))
                    {
                        Object vCo = dr.get("paramvalc"); 
                        String vC = vCo == DBNull.Value ? null : (String)vCo;
                        if(t == String.class) 
                        {
                            f.set(params, vC);
                        }
                        else
                        if(t.getInterfaces() != null && (new Common.Func<Boolean>() 
                            { @Override public Boolean call() 
                                { for (Class i : t.getInterfaces()) if (i ==  JHCJSONSerializable.class) return true; return false; }
                            }).call()
                        ) 
                        {
                            if (Common.stringIsNullOrEmpty(vC))
                                f.set(params, null);
                            else
                            {
                                JHCJSONSerializable instns = (JHCJSONSerializable)t.newInstance();
                                instns.fromJSONString(vC);
                                f.set(params, instns);
                            }
                        }
                        else
                            throw new Common.ArtificialException("параметр имеет тип <c> (char), но сооветствующее поле объекта params имеет несовместимый тип (" + t.getName() + ")");
                    }
                    else if (dr.get("paramvaltype").toString().equals("t"))
                    {
                        Object vTo = dr.get("paramvalt");
                        java.util.Date vT = vTo == DBNull.Value ? null : (java.util.Date)vTo;
                        if(t == LocalDateTime.class) 
                        {
                            if (vT == null)
                                f.set(params, null);
                            else
                                f.set(params, vT.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime());                        
                        }
                        else
                            throw new Common.ArtificialException("параметр имеет тип <t> (timestamp), но сооветствующее поле объекта params имеет несовместимый тип (" + t.getName() + ")");

                    }
                    else if (dr.get("paramvaltype").toString().equals("n"))
                    {
                        Object vNo = dr.get("paramvaln");
                        BigDecimal vN = vNo == DBNull.Value ? null : (BigDecimal)vNo;
                        if (t.isPrimitive() && vN == null)
                            throw new Common.ArtificialException("параметр типа <n> имеет пустое значение (null), но сооветствующее поле объекта params имеет тип (" + t.getName() + "), не допускающий null!");
                        if(t == BigDecimal.class) 
                        {
                            f.set(params, vN);
                        }
                        else 
                        if(t == boolean.class) 
                        {
                            f.setBoolean(params, !Objects.requireNonNull(vN).toString().equals("0"));
                        }
                        else
                        if(t == Boolean.class) 
                        {
                            f.set(params, vN == null ? null : !vN.toString().equals("0"));
                        }
                        else
                        if(t == byte.class) 
                        {
                            f.setByte(params, Objects.requireNonNull(vN).byteValueExact());
                        }
                        else
                        if(t == Byte.class) 
                        {
                            f.set(params, vN == null ? null : vN.byteValueExact());
                        }
                        else
                        if(t == int.class) 
                        {
                            f.setInt(params, Objects.requireNonNull(vN).intValueExact());
                        }
                        else
                        if(t == Integer.class) 
                        {
                            f.set(params, vN == null ? null : vN.intValueExact());
                        }
                        else
                        if(t == long.class) 
                        {
                            f.setLong(params, Objects.requireNonNull(vN).longValueExact());
                        }
                        else
                        if(t == Long.class) 
                        {
                            f.set(params, vN == null ? null : vN.longValueExact());
                        }
                        else
                        if(t == double.class) 
                        {
                            f.setDouble(params, Objects.requireNonNull(vN).doubleValue());
                        }
                        else
                        if(t == Double.class) 
                        {
                            f.set(params, vN == null ? null : vN.doubleValue());
                        }
                        else
                            throw new Common.ArtificialException("параметр имеет тип <n> (numeric), но сооветствующее поле объекта params имеет несовместимый тип (" + t.getName() + ")");
                    }                    
                    Object valueAfter = f.get(params);
                    boolean valueChanged = 
                        (valueBefore == null && valueAfter != null)
                        ||
                        (valueBefore != null && valueAfter == null)
                        ||
                        (valueBefore != null && valueAfter != null && !valueBefore.equals(valueAfter))
                    ;
                    if (valueChanged && requiresHandlerRestartOnChange)
                    {
                        result.changedParamThatRequiresHandlerRestartOnChange = true;
                        if (result.paramNamesThatChangedAndThatRequiresHandlerRestartOnChange == null)
                            result.paramNamesThatChangedAndThatRequiresHandlerRestartOnChange = "";
                        else
                            result.paramNamesThatChangedAndThatRequiresHandlerRestartOnChange += ", ";
                        result.paramNamesThatChangedAndThatRequiresHandlerRestartOnChange += f.getName();
                            
                    }
                    System.out.println("JHCParams: " + params.getClass().getCanonicalName() + "(" + params.toString() + ")." + f.getName() + " was set to " + valueAfter);
                }
                catch(Exception ex)
                {
                    String em = "Ошибка установки параметра '" + f.getName() + "' для обработчика '" + handlerTypeAlias + "': ";
                    if (ex instanceof Common.ArtificialException)
                        em += ex.getMessage();
                    else
                        em += ex.toString() + Common.hr + Common.getGoodStackTrace(ex, 0);
                    throw new RuntimeException(em);
                }
            }
        }
        return result;
    }  
    public interface JHCJSONSerializable
    {
        void fromJSONString(String s);
    }
    public static class LogPathInitInfoContainer implements JHCJSONSerializable
    {
        public Common.LogPathInitInfo[] LPIIs;        
        @Override
        public void fromJSONString(String s) {
            org.json.JSONArray a = new org.json.JSONArray(s);
            LPIIs = new Common.LogPathInitInfo[a.length()];
            for(int n = 0; n < a.length(); n++)
                if (a.getJSONObject(n).has("timeOutOnError_sec"))
                    LPIIs[n] = new Common.LogPathInitInfo(a.getJSONObject(n).getString("logPath"), a.getJSONObject(n).getInt("timeOutOnError_sec") );
                else
                    LPIIs[n] = new Common.LogPathInitInfo(a.getJSONObject(n).getString("logPath"));
        }        

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 59 * hash + Arrays.deepHashCode(this.LPIIs);
            return hash;
        }        
        @Override
        public boolean equals(Object thatO)
        {
            if (thatO == null || !(thatO instanceof LogPathInitInfoContainer)) return false;
            LogPathInitInfoContainer that = (LogPathInitInfoContainer)thatO;
            return Arrays.deepEquals(this.LPIIs, that.LPIIs);
        }
    }
    
    /**
    * При изменении значения этого параметра требуется перезапуск обработчика
    * 
    */
    @Retention(value = RetentionPolicy.RUNTIME)
    @Target(value = {ElementType.FIELD})
    public @interface JHCParamBehaviour {
        public boolean requiresHandlerRestartOnChange();// default false;
        public boolean isRequired() default true;
    }
}
