package JavaHandlerControlLib;

import DBmethodsLib.DBmethodsCommon;
import java.time.LocalDateTime;
import CommonLib.Common;
import DBmethodsLib.DBmethodsPostgres;
import DBmethodsLib.DbType;
import DBmethodsLib.SqlParameter;
import DBmethodsLib.SqlParameterCollection;
import java.lang.management.ManagementFactory;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;

/**
 *
 * 
 */
public class JavaHandlerControl implements Runnable 
{

    private static boolean isSingleton = true;
    public static void setIsSingleton(boolean value) throws AlreadyInitedException
    {
        if (isFirstInited)
            throw new AlreadyInitedException("MUSTNEVERTHROW: JavaHandlerControl singleton already started! You must run setIsSingleton() before startJHC()!");
        isSingleton = value;
    }
    private static volatile boolean isFirstInited;
    private static final Object startJHCLOCK = new Object();
    public static JavaHandlerControl startJHC(DBmethodsCommon Dbmethods, String typeAlias, String instanceTypeAlias, Common.Log messageLog, Common.Log errorLog, Common.Action onDied, Common.Action onThreadInterrupted, Common.Action1<Throwable> onThreadError, Common.Action onThreadExit, int timeBetweenPing, Integer consoleTcpPort) throws AlreadyInitedException, InitException
    {
        if (onThreadExit == null)
            throw new NullPointerException("MUSTNEVERTHROW: onThreadExit must not be null!");
        if (!isFirstInited || !isSingleton)
            synchronized(startJHCLOCK)
            {
                if (!isFirstInited || !isSingleton)
                {
                    messageLog.write("JavaHandlerControl.run", "JHC starting");
                    JavaHandlerControl result = new JavaHandlerControl(Dbmethods, typeAlias, instanceTypeAlias, messageLog, errorLog, onDied, onThreadInterrupted, onThreadError, onThreadExit, timeBetweenPing, consoleTcpPort);
                    Thread th = new Thread(result);
                    th.setDaemon(true);
                    th.setName("JavaHandlerControl thread");
                    th.start();
                    for(int n = 0; !result.isFirstRunSuccessful; n++)
                    {
                        if (n > 30 * 10 || result.lastException != null)
                        {
                            String errmsg = "Error starting JavaHandlerControl: ";
                            result.doStop = true;
                            if (result.lastException == null)
                            {
                                errmsg += "timeout";
                            }
                            else
                            {
                                errmsg += result.lastException;
                            }
                            throw new InitException(errmsg);
                        }
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                    isFirstInited = true;
                    if (isSingleton)
                        singleton = result;
                    return result;
                }
            }
        throw new AlreadyInitedException("MUSTNEVERTHROW: JavaHandlerControl singleton already started! To start more than one instance run setIsSingleton(false) before startJHC()!");
    }
    static JavaHandlerControl singleton;
    public static JavaHandlerControl getSingleton()
    {
        return singleton;
    }
    public static class AlreadyInitedException extends Exception { public AlreadyInitedException(String message) { super(message); } }
    public static class InitException extends Exception { public InitException(String message) { super(message); } }
    
    private final DBmethodsCommon dbmethods;
    private final String typeAlias;
    private final String instanceTypeAlias;
    private final Common.Log messageLog;
    private final Common.Log errorLog;
    private final Common.Action onDied;
    private final Common.Action onThreadInterrupted;
    private final Common.Action1<Throwable> onThreadError;
    private final Common.Action onThreadExit;
    private final int timeBetweenPing;
    private final Integer consoleTcpPort;
    private final DBmethodsCommon.ConnectionID connectionID = new DBmethodsCommon.ConnectionID("JavaHandlerControl");
    JavaHandlerControl(DBmethodsCommon dbmethods, String typeAlias, String instanceTypeAlias, Common.Log messageLog, Common.Log errorLog, Common.Action onDied, Common.Action onThreadInterrupted, Common.Action1<Throwable> onThreadError, Common.Action onThreadExit, int timeBetweenPing, Integer consoleTcpPort)
    {
        this.dbmethods = dbmethods;
        this.typeAlias = typeAlias;
        this.instanceTypeAlias = instanceTypeAlias;
        this.messageLog = messageLog;
        this.errorLog = errorLog;
        this.onDied = onDied;
        this.onThreadInterrupted = onThreadInterrupted;
        this.onThreadError = onThreadError;
        this.onThreadExit = onThreadExit;
        this.timeBetweenPing = timeBetweenPing;
        this.consoleTcpPort = consoleTcpPort;
    }

    private volatile String errorMessage = null;
    private volatile String lastException = null;
    private volatile LocalDateTime lastGoodRun = null;
    private volatile boolean isDied;
    private Long jhc_id = null;
    public Long getJhc_id()
    {
        return jhc_id;
    }
    private volatile boolean doStop;
    private volatile boolean isFirstRunSuccessful;
    private volatile boolean isRunned;
    private volatile boolean isStopped;
    public boolean isStopped()
    {
        return isStopped || !isRunned;
    }
    final Object runLOCK = new Object();
    private volatile boolean isStopping;
    @Override
    public void run() {
        try
        {
            isRunned = true;
            while (true)
            {
                Thread.sleep(100);
                synchronized(runLOCK)
                {
                    try
                    {                    
                        LocalDateTime now = LocalDateTime.now();
                        if (lastGoodRun == null || lastGoodRun.plusSeconds(timeBetweenPing).isBefore(now) || isStopping)
                        {
                            SqlParameterCollection params = new SqlParameterCollection();
                            params.add_param("p_result", null, DbType.VARCHAR, SqlParameter.ParameterDirection.Output);
                            params.add_param("p_typeAlias", typeAlias, DbType.VARCHAR);
                            params.add_param("p_instanceTypeAlias", instanceTypeAlias, DbType.VARCHAR);
                            params.add_param("p_selfId", ManagementFactory.getRuntimeMXBean().getName(), DbType.VARCHAR);
                            params.add_param("p_consoleTcpPort", consoleTcpPort, DbType.INTEGER);
                            params.add_param("p_jhc_id", jhc_id, DbType.BIGINT);
                            params.add_param("p_lastHandlerSuccess", lastHandlerSuccess, DbType.TIMESTAMP);
                            params.add_param("p_lastHandlerError", lastHandlerError, DbType.TIMESTAMP);
                            params.add_param("p_handlerErrorCount", handlerErrorCount, DbType.INTEGER);
                            params.add_param("p_paused", paused, DbType.BOOLEAN);
        
                            dbmethods.execProc(connectionID, "JavaHandlerControl_ping", params);

                            lastDbNowRefresh = LocalDateTime.now();

                            String[] resultA = params.get("p_result").Value.toString().split("[|]");
                            jhc_id = Long.parseLong(resultA[0]);
                            isDied = resultA[1].equals("1");
                            
                            lastDbNowValue = LocalDateTime.parse(resultA[2]);
                            lastDbNowBiggerThanLocal = lastDbNowRefresh.until(lastDbNowValue, ChronoUnit.MILLIS) + 1;//1мс - погрешность, возникающая при самом определении;

                            if (!isFirstRunSuccessful)
                                messageLog.write("JavaHandlerControl.run", "JHC started", new String[][]{new String[]{"jhc_id",jhc_id.toString()},new String[]{"lastDbNowBiggerThanLocal",lastDbNowBiggerThanLocal.toString()}});
                            
                            if (!handlerErrors.isEmpty())
                            {
                                synchronized (handlerErrorsLOCK)
                                {
                                    while (!handlerErrors.isEmpty())
                                    {
                                        params = new SqlParameterCollection();
                                        params.add_param("p_jhc_id", jhc_id, DbType.BIGINT);
                                        params.add_param("p_ts", handlerErrors.get(0).ts, DbType.TIMESTAMP);
                                        params.add_param("p_msg", handlerErrors.get(0).msg, DbType.VARCHAR);
                                        params.add_param("p_isWarning", handlerErrors.get(0).isWarning, DbType.BOOLEAN);
                                        dbmethods.execProc(connectionID, 
                                            "JavaHandlerError_add"
                                            , params);                                    
                                        handlerErrors.remove(0);
                                    }
                                }
                            }
                            if (isDied) 
                            {
                                if (!isStopping)
                                {
                                    messageLog.write("JavaHandlerControl.run", "JHC handler died");
                                    if (onDied != null) 
                                        try { onDied.call(); } catch (Throwable th) { errorLog.write(th, Common.getCurrentSTE(), "onDied error"); }
                                }
                                return;
                            }
                            lastGoodRun = now;
                            isFirstRunSuccessful = true;
                        }
                    }
//                    catch (InterruptedException ex)
//                    {
//                        lastException = errorMessage = Common.NowToString() + " Thread interrupted!" + "\r\n\r\n" + Common.getGoodStackTrace(ex, 0);
//                        if (!doStop && !isStopping)
//                        {
//                            errorLog.write("JavaHandlerControl.run", "JHC thread interrupted!", new String[][]{new String[]{"fatalError","true"}});
//                                if (onThreadInterrupted != null)
//                                    try { onThreadInterrupted.call(); } catch (Throwable th) { errorLog.write(th, Common.getCurrentSTE(), "onThreadInterrupted error"); }
//                        }
//                        return;
//                    }
                    catch (Exception ex)
                    {
                        lastException = Common.NowToString() + " Exception: " + ex.toString().trim() + "\r\n\r\n" + Common.getGoodStackTrace(ex, 0);
                        errorLog.write(ex, Common.getCurrentSTE());
                        try { 
                            for(int n = 1; n <= 100 && !doStop && !isStopping; n++)
                                Thread.sleep(100);
                        } catch (InterruptedException iex) {
                            lastException += "\r\n" + "(Thread interrupted!!)"; 
                            errorMessage = lastException;
                            if (!doStop && !isStopping)
                            {
                                errorLog.write("JavaHandlerControl.run", "JHC thread interrupted!!", new String[][]{new String[]{"fatalError","true"}});
                                if (onThreadInterrupted != null)
                                    try { onThreadInterrupted.call(); } catch (Throwable th) { errorLog.write(th, Common.getCurrentSTE(), "onThreadInterrupted error"); }
                            }
                            return;
                        }            
                        if (!isFirstRunSuccessful)
                            return;
                    }
                    catch (Throwable ex)
                    {
                        lastException = errorMessage = Common.NowToString() + "JHC fatal error (thread stopped): " + ex.toString().trim() + "\r\n\r\n" + Common.getGoodStackTrace(ex, 0);
                        errorLog.write(ex, Common.getCurrentSTE(), new String[][]{new String[]{"fatalError","true"}});
                        if (onThreadError != null)                             
                            try { onThreadError.call(ex); } catch (Throwable th) { errorLog.write(th, Common.getCurrentSTE(), "onThreadError error"); }

                        return;
                    }
                    if (doStop) 
                        return;
                    if (isStopping)
                        doStop = true;
                }
            }
        }
        catch (InterruptedException ex)
        {
            lastException = errorMessage = Common.NowToString() + " JHC thread interrupted!!!";
            if (!doStop && !isStopping)
            {
                errorLog.write("JavaHandlerControl.run", "JHC thread interrupted!!!", new String[][]{new String[]{"fatalError","true"}});
                if (onThreadInterrupted != null)
                    try { onThreadInterrupted.call(); } catch (Throwable th) { errorLog.write(th, Common.getCurrentSTE(), "onThreadInterrupted error"); }
            }
        }
        finally
        {
            try { onThreadExit.call(); } catch (Throwable th) { errorLog.write(th, Common.getCurrentSTE(), "onThreadExit error", true); }
            finally
            {
                try 
                {
                    messageLog.write("JavaHandlerControl.run", "JHC stopped" + (errorMessage != null ? " by error!" : ""), new String[][]{new String[]{"jhc_id",Common.toString(jhc_id)}}, true);
                }
                finally
                {
                    isStopped = true;
                }
            }
        }
    }

    
    
    
    
    
    
    LocalDateTime lastDbNowRefresh;
    LocalDateTime lastDbNowValue;
    volatile Long lastDbNowBiggerThanLocal;
    public long dbNowBiggerThanLocalMillis() { return lastDbNowBiggerThanLocal; }
    public LocalDateTime dbNow() { return LocalDateTime.now().plus(lastDbNowBiggerThanLocal, ChronoUnit.MILLIS); }
    
    public static class DbNow
    {
        private final DBmethodsCommon dbmethods;
        public DbNow(DBmethodsCommon dbmethods)
        {
            this.dbmethods = dbmethods;
        }
        LocalDateTime lastDbNowRefresh;
        LocalDateTime lastDbNowValue;
        volatile Long lastDbNowBiggerThanLocal;
        final Object setLOCK = new Object();
        public long dbTimeBiggerThanLocalMillis() { return lastDbNowBiggerThanLocal; }
        public LocalDateTime get() {
            return get(false);
        }
        public LocalDateTime get(boolean force) {
            if (lastDbNowRefresh == null || force)
                synchronized(setLOCK)
                {
                    if (lastDbNowRefresh == null || force)
                        refresh();
                }
            return LocalDateTime.now().plus(lastDbNowBiggerThanLocal, ChronoUnit.MILLIS); 
        }
        
        public void set(String value)
        {
            synchronized(setLOCK)
            {
                lastDbNowRefresh = LocalDateTime.now();
                lastDbNowValue = LocalDateTime.parse(value);
                lastDbNowBiggerThanLocal = lastDbNowRefresh.until(lastDbNowValue, ChronoUnit.MILLIS) + 1;//1мс - погрешность, возникающая при самом определении;
            }
        }
        private final DBmethodsCommon.ConnectionID DbNow_connectionID = new DBmethodsCommon.ConnectionID("DbNow");
        public void refresh() 
        {
            if (dbmethods instanceof DBmethodsPostgres)
            {
                try {            
                    set(dbmethods.select_from(DbNow_connectionID, "select localtimestamp")[0].Rows.get(0).get(0).toString());
                } catch (Exception ex) {
                    throw Common.rethrow(ex);
                }
            }
            else
                throw new RuntimeException("not supported yet: dbmethods instanceof " + dbmethods.getClass().getName());
        }
    }

    private volatile boolean signalStop_started;
    private final Object signalStopLOCK = new Object();
    private int delayBeforeMarkDied_ms = 0;
    public void setDelayBeforeMarkDied_ms(int value) { delayBeforeMarkDied_ms = value; }
    public void signalStop()      
    {
        signalStop(false);
    }
    public void signalStop(boolean waitForDone)      
    {
        synchronized (signalStopLOCK)
        {
            if (!signalStop_started)
            {
                isStopping = true;
                Common.Container<Boolean> done = new Common.Container<>(); done.value = false;
                try
                {
                    Thread t = new Thread(() ->
                        {
                            signalStop_started = true;
                            synchronized(runLOCK)
                            {
                                try
                                {                                    
                                    for (int n = 0; n < 3; n++)
                                    {
                                        try
                                        {
                                            if (!isDied)
                                            {
                                                if (delayBeforeMarkDied_ms > 0)
                                                    Thread.sleep(delayBeforeMarkDied_ms);
                                                SqlParameterCollection params = new SqlParameterCollection();
                                                params.add_param("p_jhc_id", jhc_id, DbType.BIGINT);
                                                dbmethods.execProc(connectionID, 
                                                    "JavaHandlerControl_markDied"                                        
                                                    , params);
                                                isDied = true;
                                                return;
                                            }
                                        }
                                        catch (Exception ex)
                                        {
                                            errorLog.write(ex, Common.getCurrentSTE(), "JavaHandlerControl_markDied error");
                                        }
                                        try { Thread.sleep(1000); } catch (InterruptedException ex) { }
                                    }
                                }
                                finally
                                {
                                    done.value = true;
                                }
                            }
                        }
                    );
                    t.setDaemon(true);
                    t.start();
                }
                finally
                {
                    for(int n = 0; waitForDone ? n < 30 * 100 && !done.value : n < 100 && !signalStop_started; n++)
                        try { Thread.sleep(10); } catch (InterruptedException ex) { }
                }
            }
        }
    }
    public boolean hasStopSignal()
    {
        return isDied || isStopping || doStop;
    }
        
        
        
        
        
    static class HandlerError { public LocalDateTime ts; public String msg; public boolean isWarning; public HandlerError(LocalDateTime ts, String msg, boolean isWarning) { this.ts = ts; this.msg = msg; this.isWarning = isWarning; } }
    ArrayList<HandlerError> handlerErrors = new ArrayList<>();
    volatile int handlerErrorCount = 0;
    volatile LocalDateTime lastHandlerError;
    final Object handlerErrorsLOCK = new Object();
    public void setHandlerError(String msg)
    {
        setHandlerError(msg, false);
    }
    public void setHandlerError(String msg, boolean isWarning)
    {
        synchronized (handlerErrorsLOCK)
        {
            handlerErrors.add(new HandlerError(dbNow(), msg, isWarning));
            lastHandlerError = dbNow();
            handlerErrorCount++;
        }
    }
    volatile LocalDateTime lastHandlerSuccess;
    public void setHandlerSuccess() { lastHandlerSuccess = dbNow(); }
    
    
    
    public boolean isDoPause() throws Exception
    {
        SqlParameterCollection params = new SqlParameterCollection();
        params.add_param("p_result", null, DbType.BOOLEAN, SqlParameter.ParameterDirection.Output);
        params.add_param("p_jhc_id", jhc_id, DbType.BIGINT);
        dbmethods.execProc(connectionID, 
            "JavaHandlerControl_isDoPause"                                        
            , params);
        return (Boolean)params.get("p_result").Value;
    }
    private boolean paused;
    public void markPaused(boolean v) throws Exception
    { 
        paused = v; 
        SqlParameterCollection params = new SqlParameterCollection();
        params.add_param("p_jhc_id", jhc_id, DbType.BIGINT);
        params.add_param("p_paused", v, DbType.BOOLEAN);
        dbmethods.execProc(connectionID, 
            "JavaHandlerControl_markPaused"                                        
            , params);
    }
    public boolean markWorkerStarted(int maxStartedInstances) throws Exception
    { 
        SqlParameterCollection params = new SqlParameterCollection();
        params.add_param("p_result", null, DbType.BOOLEAN, SqlParameter.ParameterDirection.Output);
        params.add_param("p_jhc_id", jhc_id, DbType.BIGINT);
        params.add_param("p_maxStartedInstances", maxStartedInstances, DbType.INTEGER);
        dbmethods.execProc(connectionID, 
            "JavaHandlerControl_markWorkerStarted"                                        
            , params);
        return (Boolean)params.get("p_result").Value;
    }
    

    
}
