package JavaHandlerControlLib.tasking;

import CommonLib.Common;
import DBmethodsLib.DBNull;
import DBmethodsLib.DBaggregator;
import DBmethodsLib.DBmethodsCommon;
import DBmethodsLib.DBmethodsPostgres;
import DBmethodsLib.DataRow;
import DBmethodsLib.DataTable;
import DBmethodsLib.DbType;
import DBmethodsLib.SqlCodeAnalizer;
import DBmethodsLib.SqlParameter;
import DBmethodsLib.SqlParameterCollection;
import JavaHandlerControlLib.JHCParams;
import JavaHandlerControlLib.JobThreadPoolJHCService;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import org.json.JSONObject;
import CommonModelLib.dbModel.AnyDictionaryTable;
import CommonModelLib.dbModel.IEnumCache;
import JavaHandlerControlLib.paramTypes.FileStorePathInfoContainer;
import CommonModelLib.objectModel.tasking.TaskInfo;
import CommonModelLib.objectModel.tasking.executants.TaskRetryException;
import org.postgresql.util.PSQLException;

/**
 *
 * 
 * @param <Tvars>
 * @param <Tienumcache>
 * @param <Ttaskinfo>
 */
public abstract class TaskService<Tvars, Tienumcache extends IEnumCache, Ttaskinfo extends TaskInfo> extends JobThreadPoolJHCService<ArrayList<DataRow>, Object> {
    protected final String factType_alias;
    private final Class<? extends Tienumcache> c_Tienumcache;
    protected final Class<? extends Ttaskinfo> c_Ttaskinfo;
    protected TaskService(
            String jarFileName, String handlerTypeAlias, String handlerInstanceTypeEnumAlias, Object settingsFromDB, int maxStartedInstances, String[] requiredSettingsFromFile, 
            int worker_normalLoopSleep_sec, int worker_exceptionLoopSleep_sec, String factType_alias, 
            Class<? extends Tienumcache> c_Tienumcache, Class<? extends Ttaskinfo> c_Ttaskinfo
    )
    {
        super(jarFileName, handlerTypeAlias, handlerInstanceTypeEnumAlias, Common.ConcatArraysOrObjectsToArray(taskServiceSettings = new TaskService.TaskServiceSettings(), settingsFromDB), maxStartedInstances, requiredSettingsFromFile, worker_normalLoopSleep_sec, worker_exceptionLoopSleep_sec);
        this.factType_alias = factType_alias;
        this.c_Tienumcache = c_Tienumcache;
        this.c_Ttaskinfo = c_Ttaskinfo;
    }
    
    protected static TaskServiceSettings taskServiceSettings;
    
    public static class TaskServiceSettings { 
        @JHCParams.JHCParamBehaviour(requiresHandlerRestartOnChange = true)
        public String readonlyMasterDBUser;
        @JHCParams.JHCParamBehaviour(requiresHandlerRestartOnChange = true)
        public String readonlyMasterDBPass;
        @JHCParams.JHCParamBehaviour(requiresHandlerRestartOnChange = true)
        public int readonlyDbmethodsMaxPoolSize;     
        public int rowsPerRun;
        public int taskFaultTimeout_sec;
        @JHCParams.JHCParamBehaviour(requiresHandlerRestartOnChange = false, isRequired = false)
        public FileStorePathInfoContainer sqlDebugPaths;
    }
    
    

    public DBaggregator dbaggregator; 
    public Tienumcache enumCache;
    public DBmethodsCommon readonlyMasterDB;
    int[] taskType_ens;    
    public AnyDictionaryTable taskTypes;

    @Override
    public void worker_JustBeforeStart_internal() throws Exception {
        super.worker_JustBeforeStart_internal();
        
        readonlyMasterDB = new DBmethodsPostgres(dbmethods.getDB_URL(), taskServiceSettings.readonlyMasterDBUser, taskServiceSettings.readonlyMasterDBPass, taskServiceSettings.readonlyDbmethodsMaxPoolSize);
        enumCache = Common.getAnyInstance(c_Tienumcache, new Common.ClassValuePair<>(DBmethodsCommon.class, readonlyMasterDB));
        dbaggregator = new DBaggregator.DBagregatorTest((DBmethodsPostgres)readonlyMasterDB); // TODO! !!!! разработать DBaggregator
        
        Common.Action1<DataTable> a = (dt) -> { 
            taskType_ens = new int[dt.Rows.size()];
            for (int n = 0; n < dt.Rows.size(); n++)
                taskType_ens[n] = (Integer)dt.Rows.get(n).get("taskType_en");
        };
        taskTypes = new AnyDictionaryTable(readonlyMasterDB, 
            "select t.*, e.alias \n"
            + "from en_TaskType t \n"
            + "inner join enum as e on t.taskType_en = e.enum_id and not coalesce(e.isDeleted,false) \n"
            + "where jhcType_en = " + IEnumCache.enum_idSQL(handlerTypeAlias), null, a
        );
        DataTable dt = taskTypes.get();
        if (dt.Rows.isEmpty())
            throw new RuntimeException("не заданы типы задач для обработчика " + handlerTypeAlias + "!");
        a.call(dt);
        
        enumCache.enum_id(factType_alias);

        getJHC().setDelayBeforeMarkDied_ms(10000);
    }
    
    String grab_sql = Common.getResourceAsUTF8(TaskService.class, "grab.sql").split("-----THEBEGIN-----")[1].split("-----THEEND-----")[0].trim();
    String addFaultLock_sql = Common.getResourceAsUTF8(TaskService.class, "addFaultLock.sql").split("-----THEBEGIN-----")[1].split("-----THEEND-----")[0].trim();
    String releaseAll_sql = Common.getResourceAsUTF8(TaskService.class, "releaseAll.sql").split("-----THEBEGIN-----")[1].split("-----THEEND-----")[0].trim();

    @Override
    protected Worker_getJobs_result<ArrayList<DataRow>> worker_getJobs() throws Exception 
    {
        SqlParameterCollection prms = new SqlParameterCollection();
        prms.add_param("p_jhc_ex", getJHC().getJhc_id(), DbType.BIGINT);
        prms.add_param("p_taskType_ens", taskType_ens, DbType.ARRAY);
        prms.add_param("p_limit", taskServiceSettings.rowsPerRun, DbType.INTEGER);
        prms.add_param("p_faultTimeout_sec", taskServiceSettings.taskFaultTimeout_sec, DbType.INTEGER);        
        prms.add_param("out_task_id", null, DbType.BIGINT, SqlParameter.ParameterDirection.Output);
        prms.add_param("out_task_pid", null, DbType.BIGINT, SqlParameter.ParameterDirection.Output);
        prms.add_param("out_type_en", null, DbType.INTEGER, SqlParameter.ParameterDirection.Output);
        prms.add_param("out_type_alias", null, DbType.VARCHAR, SqlParameter.ParameterDirection.Output);        
        prms.add_param("out_queue", null, DbType.INTEGER, SqlParameter.ParameterDirection.Output);
        prms.add_param("out_taskRow", null, DbType.VARCHAR, SqlParameter.ParameterDirection.Output);
        prms.add_param("out_Param_value", null, DbType.VARCHAR, SqlParameter.ParameterDirection.Output);    
        
        DataTable dt = dbmethods.execText(grab_sql, prms, true);
        if (!dt.Rows.isEmpty())
        {
            LinkedHashMap<String, ArrayList<DataRow>> queues = new LinkedHashMap<>();
            for (DataRow r : dt.Rows)
            {
                String k = r.get("out_queue") != DBNull.Value ? "q" + r.get("out_queue").toString() : "i" + r.get("out_task_id").toString();
                ArrayList<DataRow> rws;
                if (!queues.containsKey(k))
                {
                    rws = new ArrayList<>();
                    queues.put(k, rws);
                }
                else
                    rws = queues.get(k);
                rws.add(r);
            }
            return new Worker_getJobs_result<>(queues.values(), dt.Rows.size() == taskServiceSettings.rowsPerRun);
        }
        return null;
    }
    
    

    @Override
    protected void workerJob_DoWork(JobThreadPoolJHCService.JobContainer<ArrayList<DataRow>, Object> jobContainer) throws Exception 
    {
        ArrayList<DataRow> rws = jobContainer.job;
        
        Integer queue = rws.get(0).getNoDBNull("out_queue", Integer.class);
        Long single_task_id = rws.get(0).getNoDBNull("out_task_id", Long.class);
        String task_ids = null;
        for (DataRow r : rws)
            task_ids = (task_ids == null ? "" : task_ids + ", ") + r.get("out_task_id");
        boolean isOk = false;

        try
        {
            if (worker_hasStopSignal())
                return;
            for (int n = 0; n < rws.size(); n++)
            {
                DataRow r =  rws.get(n);
                TaskJob<Ttaskinfo, Tvars> tj = null;
                String fullPLpgSQL = null;
                boolean workerJob_DoTask_executed = false;
                try
                {
                    if (Common.IsDebug())
                        worker_msgLog().write("Начало выполнения задачи с task_id=" + r.get("out_task_id").toString() + ".");
                    
                    
                    
                    
                    JSONObject t_r = new JSONObject(r.getNoDBNull("out_taskRow", String.class));
                    Ttaskinfo t = Common.getAnyInstance(c_Ttaskinfo, new Common.ClassValuePair<>(JSONObject.class, t_r));
                    t.task_id = (Long)r.get("out_task_id");
                    t.task_pid = r.getNoDBNull("out_task_pid", Long.class);
                    t.type_en = (Integer)r.get("out_type_en");
                    t.type_alias = (String)r.get("out_type_alias");
                    t.queue = r.getNoDBNull("out_queue", Integer.class);
                    if (r.get("out_Param_value") != DBNull.Value)
                    {
                        t.param_value = new JSONObject(r.getNoDBNull("out_Param_value", String.class));
                    }
                      
                    tj = new TaskJob<>(
                        t,
                        n,
                        rws.size()
                    );
                    
                    
                    String taskSQL = workerJob_DoTask(tj);
                    workerJob_DoTask_executed = true;
                    
                    
                    if (taskSQL == null || taskSQL.trim().equals(""))
                        throw new NullPointerException("реализация workerJob_executeTask() вернула пустую PL/pgSQL-команду");
                    
                    fullPLpgSQL = 
                              "DO $$ DECLARE\n"
                            + "    v_st integer;\n" 
                            + "BEGIN\n"
                            + "    select status_en into v_st from Task where task_id = " + r.get("out_task_id") + " for update nowait;\n"
                            + "    if (v_st <> enum_id('en_TaskStatus.inProcess')) then\n"
                            + "        RAISE EXCEPTION 'Запись в Task имеет статус (%) отличный от inProcess!', v_st;\n"
                            + "    end if;\n"
                            + "\n"
                            + "    perform createFact(" + DBmethodsPostgres.getSqlValue(factType_alias, DbType.VARCHAR) + ");\n"
                            + "\n"
                            + "    update Task set status_en = enum_id('en_TaskStatus.done'), finish = localtimestamp"
                            + "    where task_id = " + r.get("out_task_id") + ";\n"
                            + "\n"
                            + "    --<taskSQL>\n"
                            + "    " + SqlCodeAnalizer.instance.tab(taskSQL) + "\n"
                            + "    --</taskSQL>\n"
                            + "\n"
                            + "    perform JavaHandlerControl_throwIfMeDied();\n"
                            + "\n"
                            + "END; $$\n";
                    
                    try
                    {
                        dbmethods.select_from(true, fullPLpgSQL);
                    }
                    catch (PSQLException psex)
                    {
                        if (psex.getMessage() != null && psex.getMessage().contains("Ошибка ввода/ввывода при отправке бэкенду"))
                        {
                            DataRow chk = dbmethods.select_from(true, "select (select (select jhc_ex from eventlog where event_id = changed_ev) as j from Task where task_id = " + r.get("out_task_id") + " and status_en = enum_id('en_TaskStatus.done') for update)")[0].Rows.get(0);
                            if (getJHC().getJhc_id().equals(chk.get("j")))
                            {
                                String m = "Произошел разрыв SQL-соединения при выполнении главного скрипта обработки записи task_id=" + r.get("out_task_id") + ", но при проверке статуса записи обнаружено успешное завершение транзакции.";
                                getJHC().setHandlerError(m, true);
                                worker_msgLog().write(m);
                            }
                            else
                                throw psex;
                        }
                        else
                            throw psex;
                    }
                    
                    if (Common.IsDebug())
                        worker_msgLog().write("Задача с task_id=" + r.get("out_task_id").toString() + " выполнена.");
                    if (Common.IsDebug() && taskServiceSettings.sqlDebugPaths != null)
                        try
                        {
                            taskServiceSettings.sqlDebugPaths.write(this, 100, 5, Common.SlashOnEnd(Common.NowToString("yyyyMMdd") + "_" + handlerTypeAlias) + "task_id" + r.get("out_task_id").toString() + "_" + r.get("out_type_alias").toString() + ".sql", fullPLpgSQL.getBytes(StandardCharsets.UTF_8));                    
                        }
                        catch(Exception ex)
                        {                
                            String errmsg = "Задача с task_id=" + r.get("out_task_id").toString() + " выполнена, по произошла ошибка записи в sqlDebugPaths";
                            worker_excLog().write(ex, Common.getCurrentSTE(), errmsg);
                            getJHC().setHandlerError(errmsg + ": " + Common.throwableToString(ex,Common.getCurrentSTE()));
                        }
                    
                    try
                    {
                        workerJob_DoTask_OnDbSuccess(tj);
                    }
                    catch(Exception ex)
                    {                
                        String errmsg = "Задача с task_id=" + r.get("out_task_id").toString() + " выполнена, но произошла ошибка workerJob_DoTask_OnDbSuccess";
                        worker_excLog().write(ex, Common.getCurrentSTE(), errmsg);
                        getJHC().setHandlerError(errmsg + ": " + Common.throwableToString(ex,Common.getCurrentSTE()));
                    }
                }
                catch(Exception ex)
                {       
                    if (ex instanceof TaskRetryException)
                    {
                        for (int slpN = 0; slpN < ((TaskRetryException)ex).sleep_sec; slpN++)
                        {
                            Thread.sleep(1000);
                            if (this.worker_hasStopSignal())
                                break;
                        }
                        String retry = 
                              "DO $$ DECLARE\n"
                            + "    v_st integer;\n" 
                            + "BEGIN\n"
                            + "    select status_en into v_st from Task where task_id = " + r.get("out_task_id") + " for update nowait;\n"
                            + "    if (v_st <> enum_id('en_TaskStatus.inProcess')) then\n"
                            + "        RAISE EXCEPTION 'Запись в Task имеет статус (%) отличный от inProcess!', v_st;\n"
                            + "    end if;\n"
                            + "\n"
                            + "    perform createFact('en_FactType.handleTaskRelease');\n"
                            + "\n"
                            + "    update Task set status_en = enum_id('en_TaskStatus.retry')"
                            + "    where task_id = " + r.get("out_task_id") + ";\n"
                            + "\n"
                            + "END; $$\n";                    
                    
                        dbmethods.select_from(true, retry);
                    if (Common.IsDebug())
                        System.out.println("Отправили задачу с task_id=" + r.get("out_task_id").toString() + " на повтор (retry)");
                         
                    }
                    else
                        try
                        {
                            //установка en_TaskStatus.fault задаче и блокировка очереди в таблице блокировок, но с типом fq или fi
                            String errmsg = "Задача с task_id=" + r.get("out_task_id").toString() + " вызвала исключение";
                            getJHC().setHandlerError(errmsg + ": " + Common.throwableToString(ex,Common.getCurrentSTE()) 
                                    + (ex instanceof SQLException && fullPLpgSQL != null ? Common.hr() + "fullPLpgSQL:" + Common.br() + fullPLpgSQL : ""));
                            String[][] addTag = null;
                            if (ex instanceof SQLException && fullPLpgSQL != null)
                                addTag = new String[][] { new String[] { "fullPLpgSQL", fullPLpgSQL } };
                            worker_excLog().write(ex, Common.getCurrentSTE(), errmsg, addTag);
                            SqlParameterCollection prmss = new SqlParameterCollection();
                            prmss.add_param("p_jhc_ex", getJHC().getJhc_id(), DbType.BIGINT);
                            prmss.add_param("p_task_id", r.get("out_task_id"), DbType.BIGINT);
                            prmss.add_param("p_queue", r.get("out_queue"), DbType.INTEGER);
                            dbmethods.execText(addFaultLock_sql, prmss);                            
                        }
                        finally
                        {
                            if (workerJob_DoTask_executed)
                                workerJob_DoTask_OnDbException(ex, tj);
                        }
                    return;
                }
                if (worker_hasStopSignal())
                    return;
            }      
        }
        finally
        {
            SqlParameterCollection prmss = new SqlParameterCollection();
            dbmethods.execText(
                "BEGIN\n"
                + "    perform createFact('en_FactType.handleTaskRelease');\n"
                + "delete from TaskLOCK where lockId = " + (queue == null ? single_task_id : queue) + " and lockIdType = '" + (queue == null ? "i" : "q") + "';\n"
                
                + (isOk ? "" : "update Task set status_en = enum_id('en_TaskStatus.new') where status_en = enum_id('en_TaskStatus.inProcess') and task_id in (" + task_ids + ");\n")
                + "END;"
            , prmss);                
        }
    }
    
    @Override
    protected void worker_OnJTPExceptions(ArrayList<JobThreadPoolJHCService.JobExContainer<ArrayList<DataRow>, Object>> jobExContainers)  throws Exception 
    {
        //снятие блокировок обработчика и установка статуса new всем необработанным сообщениям
        SqlParameterCollection prmss = new SqlParameterCollection();
        prmss.add_param("p_jhc_ex", getJHC().getJhc_id(), DbType.BIGINT);
        dbmethods.execText(releaseAll_sql, prmss);
    }
    
    
       
    /**
     * параметры задачи ({@link TaskExecutionJob#task }), 
     * объект-контейнер для переменных, используемых в задаче ({@link TaskExecutionJob#vars }), 
     * номер задачи в очереди, начиная с нуля ({@link TaskExecutionJob#taskIxInQueue }),
     * длина очереди ({@link TaskExecutionJob#taskQueueLength })
     * @param <Ttaskinfo>
     * @param <Tvars>
     */
    public static class TaskJob<Ttaskinfo extends TaskInfo, Tvars> 
    { 
        public final Ttaskinfo taskInfo; 
        
        /**
         * номер задачи в очереди, начиная с нуля
         */
        public final int taskIxInQueue; 

        /**
         * длина очереди
         */
        public final int taskQueueLength; 

        /**
         * объект-контейнер для переменных, используемых в задаче (требуется его инициализация в {@link TaskExecutionService#workerJob_DoTask(rsa.pvu2.handlerCommonLib.taskExecution.TaskExecutionService.TaskJob) })
         */
        public Tvars vars; 
        
        public TaskJob(Ttaskinfo taskInfo, int taskIxInQueue, int taskQueueLength)
        {
            this.taskInfo = taskInfo;
            this.taskIxInQueue = taskIxInQueue;
            this.taskQueueLength = taskQueueLength;
        }
    }
    
    /**
     * Метод, содержащий всю логику исполнения задачи, кроме записи в БД
     * @param taskJob 
     * @return PL/pgSQL, который должен выполниться по данной задаче
     * @throws Exception
     * @throws CommonModelLib.objectModel.tasking.executants.TaskRetryException
     */
    protected abstract String workerJob_DoTask(TaskJob<Ttaskinfo, Tvars> taskJob) throws Exception, TaskRetryException;
    
    /**
     * Метод обработки ошибок исполнения sql, возвращенного методом {@link TaskExecutionService#workerJob_DoTask(rsa.pvu2.handlerCommonLib.taskExecution.TasService.TaskJob) }
     * @param ex произошедшее исключение
     * @param taskJob
     */
    protected abstract void workerJob_DoTask_OnDbException(Exception ex, TaskJob<Ttaskinfo, Tvars> taskJob);
    
    /**
     * Метод обработки успешной записи в БД sql, возвращенного методом {@link TaskExecutionService#workerJob_DoTask(rsa.pvu2.handlerCommonLib.taskExecution.TaskService.TaskJob) }
     * @param taskJob
     */
    protected abstract void workerJob_DoTask_OnDbSuccess(TaskJob<Ttaskinfo, Tvars> taskJob);
    
}
