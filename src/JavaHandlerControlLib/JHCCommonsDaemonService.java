package JavaHandlerControlLib;

import CommonLib.Common;
import CommonLib.XmlSettingsBase;
import DBmethodsLib.DBmethodsCommon;
//import DBmethodsLib.DBmethodsOra;
import DBmethodsLib.DBmethodsPostgres;
import JavaAsServiceLib.CommonsDaemonService;
import java.lang.management.ManagementFactory;
import java.util.Objects;
import java.util.ServiceConfigurationError;

/**
 *
 * 
 */
public abstract class JHCCommonsDaemonService extends CommonsDaemonService {
    public final String handlerTypeAlias;
    public final String handlerInstanceTypeAlias;
    final Object settingsFromDB;
    final int maxStartedInstances;
    private final int worker_normalLoopSleep_sec;
    private final int worker_exceptionLoopSleep_sec;
    final private XmlSettingsBase dbConnectionSettingsFromFile;
    protected JHCCommonsDaemonService(String jarFileName, String handlerTypeAlias, String handlerInstanceTypeEnumAlias, Object settingsFromDB)
    {
        this(jarFileName, handlerTypeAlias, handlerInstanceTypeEnumAlias, settingsFromDB, 0, null, 0, 0);
    }
    protected JHCCommonsDaemonService(String jarFileName, String handlerTypeAlias, String handlerInstanceTypeEnumAlias, Object settingsFromDB, int maxStartedInstances)
    {
        this(jarFileName, handlerTypeAlias, handlerInstanceTypeEnumAlias, settingsFromDB, maxStartedInstances, null, 0, 0);
    }
    protected JHCCommonsDaemonService(String jarFileName, String handlerTypeAlias, String handlerInstanceTypeEnumAlias, Object settingsFromDB, int maxStartedInstances, String[] requiredSettingsFromFile, int worker_normalLoopSleep_sec, int worker_exceptionLoopSleep_sec)
    {
        super(jarFileName, handlerInstanceTypeEnumAlias != null ? Common.ConcatArray(new String[] { "handlerInstanceTypeAlias" }, requiredSettingsFromFile) : requiredSettingsFromFile);
        this.dbConnectionSettingsFromFile = new XmlSettingsBase(jarFileName, "dbConnection.settings", true, new String[] { "DB_URL", "DB_USER", "DB_PASS"});
        this.handlerTypeAlias = Objects.requireNonNull(handlerTypeAlias);
        this.settingsFromDB = settingsFromDB;
        this.maxStartedInstances = maxStartedInstances;
        this.worker_normalLoopSleep_sec = worker_normalLoopSleep_sec;
        this.worker_exceptionLoopSleep_sec = worker_exceptionLoopSleep_sec;
        this.handlerInstanceTypeAlias = handlerInstanceTypeEnumAlias != null ? settingsFromFile.getProperty("handlerInstanceTypeAlias") : null;
        if (this.handlerInstanceTypeAlias != null && !this.handlerInstanceTypeAlias.startsWith(handlerInstanceTypeEnumAlias + "."))
            throw new ServiceConfigurationError("handlerInstanceTypeAlias property must start with '" + handlerInstanceTypeEnumAlias + ".'");
    }    
    protected final String settingsFromFile_DB_URL()          {            return dbConnectionSettingsFromFile.getProperty("DB_URL");         }
    protected final String settingsFromFile_DB_USER()         {            return dbConnectionSettingsFromFile.getProperty("DB_USER");         }
    protected final String settingsFromFile_DB_PASS()         {            return dbConnectionSettingsFromFile.getProperty("DB_PASS");         }    
            
    public static DBmethodsCommon dbmethods;
    protected static class JHCCommonsDaemonServiceSettings { 
        public String messagesLogName; 
        public JHCParams.LogPathInitInfoContainer messagesLogPaths; 
        public String errorsLogName; 
        public JHCParams.LogPathInitInfoContainer errorsLogPaths; 
        @JHCParams.JHCParamBehaviour(requiresHandlerRestartOnChange = true, isRequired = false)
        public int dbmethodsMaxPoolSize;

        //@Override
        public boolean equals2(JHCCommonsDaemonServiceSettings that)
        {
            if (that == null) return false;            
            if (
                (this.messagesLogName == null && that.messagesLogName != null)
                || 
                (this.messagesLogPaths == null && that.messagesLogPaths != null)
                || 
                (this.errorsLogName == null && that.errorsLogName != null)
                || 
                (this.errorsLogPaths == null && that.errorsLogPaths != null)
            ||
                (this.messagesLogName != null && that.messagesLogName == null)
                || 
                (this.messagesLogPaths != null && that.messagesLogPaths == null)
                || 
                (this.errorsLogName != null && that.errorsLogName == null)
                || 
                (this.errorsLogPaths != null && that.errorsLogPaths == null)
            ) 
                return false;
            return 
                ((this.messagesLogName == null && that.messagesLogName == null) || this.messagesLogName.equals(that.messagesLogName))
                && 
                ((this.messagesLogPaths == null && that.messagesLogPaths == null) || this.messagesLogPaths.equals(that.messagesLogPaths))
                && 
                ((this.errorsLogName == null && that.errorsLogName == null) || this.errorsLogName.equals(that.errorsLogName))
                && 
                ((this.errorsLogPaths == null && that.errorsLogPaths == null) || this.errorsLogPaths.equals(that.errorsLogPaths))
                && 
                (this.dbmethodsMaxPoolSize == that.dbmethodsMaxPoolSize)
            ;
        }
        @JHCParams.JHCParamBehaviour(requiresHandlerRestartOnChange = true, isRequired = false)
        public boolean isDebug;
        @JHCParams.JHCParamBehaviour(requiresHandlerRestartOnChange = true, isRequired = false)
        public Integer debugDelay_sec;

        public JHCCommonsDaemonServiceSettings copy()
        {
            JHCCommonsDaemonServiceSettings result = new JHCCommonsDaemonServiceSettings();
            result.messagesLogName = this.messagesLogName;
            result.messagesLogPaths = this.messagesLogPaths;
            result.errorsLogName = this.errorsLogName;
            result.errorsLogPaths = this.errorsLogPaths;
            result.dbmethodsMaxPoolSize = this.dbmethodsMaxPoolSize;            
            result.isDebug = this.isDebug;            
            result.debugDelay_sec = this.debugDelay_sec;            
            return result;
        }        
    }
    protected JHCCommonsDaemonServiceSettings jhcCommonsDaemonServiceSettings = new JHCCommonsDaemonServiceSettings();
    JHCCommonsDaemonServiceSettings isfdb_copy;
    JHCParams jhcparams;
    JHCServiceWorkerThread worker;
    volatile boolean isNewSettingsFromDBAvailable = false;
    volatile String newSettingsFromDBError = null;
    volatile boolean isNewSettingsFromDBErrorSignaled = false;
    @Override
    public final CommonsDaemonServiceStartInfo onServiceStart() {
        Common.Log mLog;
        Common.Log eLog;
        String DB_URL = DB_URL_appendPostgesApplicationName(settingsFromFile_DB_URL(), jarFileName, "MUSTNEVERTHROW: please remove ApplicationName from DB_URL parameter!");
        
        if (!DB_URL.toLowerCase().startsWith("jdbc:postgresql:"))
            throw new Error("MUSTNEVERTHROW: DB_URL must be jdbc:postgresql!");
        
        DBmethodsPostgres tmp = new DBmethodsPostgres(DB_URL, settingsFromFile_DB_USER(), settingsFromFile_DB_PASS());
        
        //получить настройки из базы;
        jhcparams = new JHCParams(tmp, handlerTypeAlias, handlerInstanceTypeAlias, Common.ConcatArraysOrObjectsToArray(jhcCommonsDaemonServiceSettings, settingsFromDB), (dt) -> { isNewSettingsFromDBAvailable = true; });
        try
        {
            jhcparams.fillParamssByDB();
            if (jhcCommonsDaemonServiceSettings.isDebug)
            {
                Common.setIsDebug(jhcCommonsDaemonServiceSettings.isDebug);
                if (jhcCommonsDaemonServiceSettings.debugDelay_sec != null)
                {
                    System.out.println("isfdb.debugDelay_sec Start (" + jhcCommonsDaemonServiceSettings.debugDelay_sec + " sec)");
                    Thread.sleep(jhcCommonsDaemonServiceSettings.debugDelay_sec * 1000);
                    System.out.println("isfdb.debugDelay_sec End");
                }
                System.out.println("2 this.toString()=" + this.toString());
            }
            mLog = new Common.Log(true, jhcCommonsDaemonServiceSettings.messagesLogName, true, jhcCommonsDaemonServiceSettings.messagesLogPaths.LPIIs);
            eLog = new Common.Log(true, jhcCommonsDaemonServiceSettings.errorsLogName, true, jhcCommonsDaemonServiceSettings.errorsLogPaths.LPIIs);
        }
        catch(Exception ex)
        {
            String faillogpath = Common.SlashOnEnd(Common.SlashOnEnd(System.getProperty("user.home")) + "java_" + jarFileName + "_BADLOGSPARAMS");
            newSettingsFromDBError = ex.toString();
            mLog = (jhcCommonsDaemonServiceSettings.messagesLogName == null || jhcCommonsDaemonServiceSettings.messagesLogPaths == null || jhcCommonsDaemonServiceSettings.messagesLogPaths.LPIIs == null) ?
                new Common.Log(false, "messagesLog", true, faillogpath)
            :
                new Common.Log(false, jhcCommonsDaemonServiceSettings.messagesLogName, true, jhcCommonsDaemonServiceSettings.messagesLogPaths.LPIIs);

            eLog = (jhcCommonsDaemonServiceSettings.errorsLogName == null || jhcCommonsDaemonServiceSettings.errorsLogPaths == null || jhcCommonsDaemonServiceSettings.errorsLogPaths.LPIIs == null) ?
                new Common.Log(false, "errorsLog", true, faillogpath)
            :
                new Common.Log(false, jhcCommonsDaemonServiceSettings.errorsLogName, true, jhcCommonsDaemonServiceSettings.errorsLogPaths.LPIIs);  
            
            String errprefix = "Ошибка чтения параметров из БД на старте сервиса";
            eLog.write(ex, Common.getCurrentSTE(), errprefix);
            newSettingsFromDBError = errprefix + ": " + newSettingsFromDBError;
        }
        
        
        dbmethods = new DBmethodsPostgres(DB_URL, settingsFromFile_DB_USER(), settingsFromFile_DB_PASS(), jhcCommonsDaemonServiceSettings.dbmethodsMaxPoolSize);
        dbmethods.setTransacParamsConstructor((arr) -> {
            String s = "do $$ begin"+"\r\n";
            for (DBmethodsCommon.SessionOrTransacParam p : arr) {
                s += "perform paramOfTranTime_set" 
                    + (p.paramType == DBmethodsCommon.SessionOrTransacParamTypeENUM.c ? "C" : (p.paramType == DBmethodsCommon.SessionOrTransacParamTypeENUM.n ? "N" : "T")) 
                    + "('" + p.paramName + "', " 
                    + (p.paramType == DBmethodsCommon.SessionOrTransacParamTypeENUM.n ? "" : "'") 
                    + p.paramValue 
                    + (p.paramType == DBmethodsCommon.SessionOrTransacParamTypeENUM.n ? "" : "'") 
                    + ");" + "\r\n";
            }
            return s + "\r\n" + "end; $$";
        });

        
        worker = new JHCServiceWorkerThread(dbmethods, handlerTypeAlias, handlerInstanceTypeAlias, jarFileName, mLog, eLog, worker_normalLoopSleep_sec, worker_exceptionLoopSleep_sec) 
        {
            @Override
            public Worker_DoWork_resultFlags JHCSWTDoWork() throws Exception {                
                if (isNewSettingsFromDBAvailable && newSettingsFromDBError == null)
                {
                    try
                    {
                        isfdb_copy = jhcCommonsDaemonServiceSettings.copy();
                        JHCParams.fillParamssByDBRESULT fpr = jhcparams.fillParamssByDB();
                        isNewSettingsFromDBAvailable = false;                        
                        workermsgLog.write(null, "Успешно применены новые параметры из БД");
                        if (!jhcCommonsDaemonServiceSettings.equals2(isfdb_copy))
                        {
                            workermsgLog.write(null, "Параметры логирования изменены. Перезапуск сервиса...");
                            throw new StopException();
                        }
                        if (fpr.changedParamThatRequiresHandlerRestartOnChange)
                        {
                            workermsgLog.write(null, "Изменены параметры обработчика, изменение которых должно вызывать перезапуск (" + fpr.paramNamesThatChangedAndThatRequiresHandlerRestartOnChange + ") . Перезапуск сервиса...");
                            throw new StopException();
                        }
                    }
                    catch(Exception ex)
                    {
                        if (ex instanceof StopException)
                            throw ex;
                        String errprefix = "Ошибка чтения параметров из БД в ходе работы сервиса";
                        newSettingsFromDBError = ex.toString();
                        newSettingsFromDBError = errprefix + ": " + newSettingsFromDBError;
                    }
                }
                if (newSettingsFromDBError != null)
                {
                    Common.setTimeout(()->{ signalStop(); }, 3000);                    
                    throw new Exception(newSettingsFromDBError);
                }
                else
                {
                    return worker_DoWork();
                }                
            }
            @Override
            public void JHCSWTDoWorkOnException(Exception ex) {
                worker_DoWorkOnException(ex);
            }
            @Override
            protected void JustBeforeStart() throws Exception {
                if (newSettingsFromDBError != null)
                    return;
                
                for (int n = 0; !getJHC().markWorkerStarted(maxStartedInstances); n++)
                {
                    if (n == 0)
                        workermsgLog.write("JHCCommonsDaemonService.onServiceStart=>$.JustBeforeStart", "Ожидание возможности запуска рабочего потока сервиса...");
                    Thread.sleep(3000);
                    if (hasStopSignal())
                        return;
                }
                try
                {
                    worker_JustBeforeStart_internal();
                    worker_JustBeforeStart();
                }
                catch(Exception ex)
                {
                    getJHC().setHandlerError("ошибка запуска рабочего потока сервиса: " + Common.throwableToString(ex, Common.getCurrentSTE()));
                    throw Common.rethrow(ex);
                }
            }
        };  
        return new CommonsDaemonServiceStartInfo(mLog, eLog, worker);
    }
    
    /**
     * Переопределяя этот метод, обязательно (!) следует включить вызов super.worker_JustBeforeStart_internal(); в начало переопределяющего метода
     * @throws Exception
     */
    protected void worker_JustBeforeStart_internal() throws Exception {}
    public abstract void worker_JustBeforeStart() throws Exception;
    public abstract Worker_DoWork_resultFlags worker_DoWork() throws Exception;
    public abstract void worker_DoWorkOnException(Exception ex);
    public JavaHandlerControl getJHC()
    {
        return worker.getJHC();
    }
    public void worker_setOnBeforeMarkPausedIfDoPause(Common.Action onBeforeMarkPausedIfDoPause)
    {
        worker.onBeforeMarkPausedIfDoPause = onBeforeMarkPausedIfDoPause;
    }
    public boolean worker_isDoPauseCACHED() 
    { 
        return worker.isDoPauseCACHED(); 
    }
    public void writeLogAndSignalStop(Throwable th)
    {
        try
        {
            worker_excLog().write(th, Common.getCurrentSTE());
            getJHC().setHandlerError(th.toString());
            if (th instanceof Error)
                throw (Error)th;
        }
        finally
        {
            worker_signalStop();
        }
    }
    
    public static String DB_URL_appendPostgesApplicationName(String DB_URL, String ApplicationName, String errMsgIfContains)
    {
        return DBmethodsPostgres.DB_URL_appendPostgesApplicationName(DB_URL, ApplicationName, errMsgIfContains);
    }

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    




    /**
     *
     * 
     * 
     * Класс рабочего потока сервиса, наследуя от которого следует задавать бизнес-логику в методе {@code JHCDoWork()}.
     * Для инициализации, следует вернуть экземпляр класса-наследника в реализованном методе {@code onServiceStart()} главного класса сервиса, наследованного от {@code CommonsDaemonService}
     * 
     */
    public static abstract class JHCServiceWorkerThread extends ServiceWorkerThread {
        final DBmethodsCommon dbmethods;
        final String typeAlias;
        final String subTypeAlias;
        protected JHCServiceWorkerThread(DBmethodsCommon dbmethods, String typeAlias, String subTypeAlias, String serviceName, Common.Log msgLog, Common.Log exLog)
        {
            super(serviceName, msgLog, exLog);
            this.dbmethods = dbmethods;
            this.typeAlias = typeAlias;
            this.subTypeAlias = subTypeAlias;
        }
        protected JHCServiceWorkerThread(DBmethodsCommon dbmethods, String typeAlias, String subTypeAlias, String serviceName, Common.Log msgLog, Common.Log exLog, int normalLoopSleep_sec, int exceptionLoopSleep_sec)
        {
            super(serviceName, msgLog, exLog, normalLoopSleep_sec, exceptionLoopSleep_sec);
            this.dbmethods = dbmethods;
            this.typeAlias = typeAlias;
            this.subTypeAlias = subTypeAlias;
        }

        private JavaHandlerControl JHC;
        public final JavaHandlerControl getJHC()
        {
            return JHC;
        }            

        @Override
        public final void start() throws Exception
        {
            JavaHandlerControl.setIsSingleton(false);

            int openedCVTPort = 0;
            for (int n = 18439; openedCVTPort == 0; n++)
            {
                try
                {
                    CommonLib.IOConcoleViaTCP cvt = new CommonLib.IOConcoleViaTCP(n);
                    openedCVTPort = n;
                }
                catch (Exception ex)
                {
                    if (n > 18439 + 1000)
                        throw new Common.ArtificialRuntimeException("невозможно открыть порт слушателя IOConcoleViaTCP: " + Common.throwableToString(ex, Common.getCurrentSTE()));
                }
            }

            JHC = JavaHandlerControl.startJHC(dbmethods, typeAlias, subTypeAlias, workermsgLog, workerexcLog, null, null, null, 
                () -> {
                    signalStop();
                },
                3, 
                openedCVTPort                
            );

            dbmethods.addConstantTransacParam("jhc_id", DBmethodsCommon.SessionOrTransacParamTypeENUM.n, getJHC().getJhc_id());

            super.start();
        }
        private volatile boolean isDoPauseCACHED;
        public boolean isDoPauseCACHED() { return isDoPauseCACHED; }
        public Common.Action onBeforeMarkPausedIfDoPause;
        private volatile boolean isOnPause;
        @Override
        public final Worker_DoWork_resultFlags DoWork() throws Exception {
            Worker_DoWork_resultFlags resultFlags = null;
            boolean pz = JHC.isDoPause();
            isDoPauseCACHED = pz;
            if (pz && onBeforeMarkPausedIfDoPause != null)
            {
                onBeforeMarkPausedIfDoPause.call();
            }
            JHC.markPaused(pz);
            if (!pz)
            {
                if (isOnPause && Common.IsDebug())
                    System.out.println(java.time.LocalDateTime.now().toString() + "    " + "Рабочий поток сервиса вышел из паузы");
                isOnPause = false;
                resultFlags = JHCSWTDoWork();
                JHC.setHandlerSuccess();
            }
            else
            {
                if (!isOnPause && Common.IsDebug())
                    System.out.println(java.time.LocalDateTime.now().toString() + "    " + "Рабочий поток сервиса в паузе");
                isOnPause = true;
            }
            return resultFlags;
        }
        public abstract Worker_DoWork_resultFlags JHCSWTDoWork() throws Exception;

        @Override
        public final void DoWorkOnException(Exception ex, StackTraceElement exSte)  {
            JHC.setHandlerError(ex.toString() + "\r\n\r\n" + Common.getGoodStackTrace(ex, exSte));
            JHCSWTDoWorkOnException(ex);
        }
        public abstract void JHCSWTDoWorkOnException(Exception ex);

        @Override
        public final boolean isStopped()
        {
            return super.isStopped() && JHC.isStopped();
        }
        @Override
        public final void signalStop() 
        {
            try
            {
                JHC.signalStop();
            }
            catch (Throwable th)
            {
                workerexcLog.write(th, Common.getCurrentSTE(), "Ошибка JHC.signalStop()", true);
            }
            super.signalStop();
        }
        @Override
        public final boolean hasStopSignal()
        {
            return super.hasStopSignal() || JHC.hasStopSignal();
        }

        @Override
        protected final void onThreadExit() throws Exception
        {   
            JHC.signalStop();
        }
    }
}
