package JavaHandlerControlLib.tasking;

import CommonLib.Common;
import DBmethodsLib.DBmethodsCommon;
import DBmethodsLib.DataTable;
import CommonModelLib.dbModel.AnyDictionaryTableWithJavaClass;
import CommonModelLib.dbModel.IEnumCache;
import CommonModelLib.objectModel.Versioned;
import CommonModelLib.objectModel.tasking.TaskInfo;
import CommonModelLib.objectModel.tasking.executants.TaskExecutant;
import CommonModelLib.objectModel.tasking.executants.TaskRetryException;
import org.json.JSONObject;

/**
 * 
 * 
 * @param <Tvars> тип вспомогательного объекта-контейнера для переменных, передаваемых из метода исполнения задачи
 * ({@link TaskExecutionService#workerJob_DoTask(rsa.pvu2.handlerCommonLib.taskExecution.TaskExecutionService.TaskExecutionJob) }) в методы 
 *  обработки ошибок (в случае ошибки  исполнения скрипта БД) 
 * ({@link TaskExecutionService#workerJob_DoTask_OnDbException(java.lang.Exception, rsa.pvu2.handlerCommonLib.taskExecution.TaskExecutionService.TaskExecutionJob) })
 *  и обработки успеха (в случае успеха исполнения скрипта БД) 
 * ({@link TaskExecutionService#workerJob_DoTask_OnDbSuccess(rsa.pvu2.handlerCommonLib.taskExecution.TaskExecutionService.TaskExecutionJob) })
 * @param <Tcls> супер-класс обработчиков задач
 * @param <Tctorarg> тип агрумента для конструктора экземпляров класса <code>Tcls</code>
 * @param <Tienumcache>
 * @param <Ttaskinfo>
 */
public abstract class TaskExecutionService<Tvars, Tienumcache extends IEnumCache, Ttaskinfo extends TaskInfo, Tcls extends TaskExecutant<Ttaskinfo, Tienumcache>, Tctorarg extends TaskExecutant.TaskExecutantCtor<Ttaskinfo, Tienumcache>> 
extends TaskService<TaskExecutionService.TaskExecutionServiceVars<Tvars, Tcls>, Tienumcache, Ttaskinfo> 
{
    protected TaskExecutionService(
            String jarFileName, String handlerTypeAlias, String handlerInstanceTypeEnumAlias, Object settingsFromDB, int maxStartedInstances, String[] requiredSettingsFromFile, 
            int worker_normalLoopSleep_sec, int worker_exceptionLoopSleep_sec, String factType_alias, 
            Class<? extends Tienumcache> c_Tienumcache, Class<? extends Ttaskinfo> c_Ttaskinfo,
            AnyDictionaryTableWithJavaClass.JavaClassInstantiator<Tcls, Tctorarg> javaClassInstantiator, Tctorarg ctorArgsForCheck
    )
    {
        super(jarFileName, handlerTypeAlias, handlerInstanceTypeEnumAlias, Common.ConcatArraysOrObjectsToArray(taskExecutionServiceSettings = new TaskExecutionServiceSettings(), settingsFromDB), maxStartedInstances, requiredSettingsFromFile, worker_normalLoopSleep_sec, worker_exceptionLoopSleep_sec, factType_alias, c_Tienumcache, c_Ttaskinfo);
        this.javaClassInstantiator = javaClassInstantiator;
        this.ctorArgsForCheck = ctorArgsForCheck;
    }
    private final AnyDictionaryTableWithJavaClass.JavaClassInstantiator<Tcls, Tctorarg> javaClassInstantiator; 
    final Tctorarg ctorArgsForCheck;
    
    static TaskExecutionServiceSettings taskExecutionServiceSettings;
    
    public static class TaskExecutionServiceSettings { 
    }

    
    @Override
    public void worker_JustBeforeStart_internal() throws Exception {
        super.worker_JustBeforeStart_internal();
        
        ctorArgsForCheck.taskInfo = Common.getAnyInstance(c_Ttaskinfo, new Common.ClassValuePair<>(JSONObject.class, TaskInfo.forCheck));
        ctorArgsForCheck.dbaggregator = dbaggregator;
        ctorArgsForCheck.enumCache = enumCache;
        ctorArgsForCheck.worker_msgLog = worker_msgLog();
        ctorArgsForCheck.worker_excLog = worker_excLog();
        ctorArgsForCheck.worker_hasStopSignal = this::worker_hasStopSignal;
        ctorArgsForCheck.dbNow = () -> getJHC().dbNow();
        
        en_TaskType = new AnyDictionaryTableWithJavaClass__en_TaskType<>(this, readonlyMasterDB, javaClassInstantiator, ctorArgsForCheck);
        en_TaskType.checkJavaClasses();

    }
    
    static class AnyDictionaryTableWithJavaClass__en_TaskType<Tcls extends Versioned, Tctorarg> extends CommonModelLib.dbModel.AnyDictionaryTableWithJavaClass<Tcls, Tctorarg>
    {         
        AnyDictionaryTableWithJavaClass__en_TaskType(TaskExecutionService parent, DBmethodsCommon dbmethods,
            JavaClassInstantiator<Tcls, Tctorarg> javaClassInstantiator, Tctorarg ctorArgsForCheck)
        {
            super(parent::writeLogAndSignalStop, dbmethods, 
                parent.taskTypes.sqlExpr,
                new String[] { "taskType_en" },
                "javaClass", "javaClassVer", "taskType_en", 
                javaClassInstantiator,
                ctorArgsForCheck
            );            
        }        
    }
    AnyDictionaryTableWithJavaClass__en_TaskType<Tcls, Tctorarg> en_TaskType;



  
    
    public static class TaskExecutionJob<Tvars, Tcls extends TaskExecutant> 
    { 
        /**
         * параметры задачи
         */
        public final Tcls taskExecutant; 

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
        
        public TaskExecutionJob(Tcls taskExecutant, int taskIxInQueue, int taskQueueLength)
        {
            this.taskExecutant = taskExecutant;
            this.taskIxInQueue = taskIxInQueue;
            this.taskQueueLength = taskQueueLength;
        }
    }
    
    /**
     * Инициализация конструктора для {@code <Tcls extends TaskExecutant> } (исполняется каждый раз перед {@linkplain TaskExecutionService#workerJob_DoTask(rsa.pvu2.handlerCommonLib.taskExecution.TaskExecutionService.TaskJob) } )
     * @param taskInfo
     * @return 
     * @throws java.lang.Exception 
     * @throws CommonModelLib.objectModel.tasking.executants.TaskRetryException 
     */
    protected abstract Tctorarg workerJob_InstantiateTaskExecutantCtor(Ttaskinfo taskInfo) throws Exception, TaskRetryException;    
    protected void workerJob_RefillTaskExecutantCtor(Tctorarg ctorarg) { }
    /**
     * Метод, содержащий всю логику исполнения задачи, кроме записи в БД
     * @param taskExecutionJob 
     * @return PL/pgSQL, который должен выполниться для выполнения задачи
     * @throws Exception
     * @throws CommonModelLib.objectModel.tasking.executants.TaskRetryException
     */
    protected abstract String workerJob_DoTask(TaskExecutionJob<Tvars, Tcls> taskExecutionJob) throws Exception, TaskRetryException;
    
    /**
     * Метод обработки ошибок исполнения sql, возвращенного методом {@link TaskExecutionService#workerJob_DoTask(rsa.pvu2.handlerCommonLib.taskExecution.TaskExecutionService.TaskExecutionJob) }
     * @param ex произошедшее исключение
     * @param taskExecutionJob
     */
    protected abstract void workerJob_DoTask_OnDbException(Exception ex, TaskExecutionJob<Tvars, Tcls> taskExecutionJob);
    
    /**
     * Метод обработки успешной записи в БД sql, возвращенного методом {@link TaskExecutionService#workerJob_DoTask(rsa.pvu2.handlerCommonLib.taskExecution.TaskExecutionService.TaskExecutionJob) }
     * @param taskExecutionJob
     */
    protected abstract void workerJob_DoTask_OnDbSuccess(TaskExecutionJob<Tvars, Tcls> taskExecutionJob);
    
    
    
    
    @Override
    protected final String workerJob_DoTask(TaskService.TaskJob<Ttaskinfo, TaskExecutionService.TaskExecutionServiceVars<Tvars, Tcls>> taskJob) throws Exception, TaskRetryException
    {    
        Tctorarg arg = workerJob_InstantiateTaskExecutantCtor(taskJob.taskInfo);
        if (arg.dbaggregator == null)
            arg.dbaggregator = dbaggregator;
        arg.enumCache = enumCache;
        arg.taskInfo = taskJob.taskInfo;
        arg.worker_msgLog = worker_msgLog();
        arg.worker_excLog = worker_excLog();
        arg.worker_hasStopSignal = this::worker_hasStopSignal;
        arg.dbNow = () -> getJHC().dbNow();
        workerJob_RefillTaskExecutantCtor(arg);
        
        taskJob.vars = new TaskExecutionService.TaskExecutionServiceVars<>();
        taskJob.vars.childTaskExecutionJob = new TaskExecutionJob<>(
                en_TaskType.newInstance(en_TaskType.get().Select(new DataTable.SelectFilter("taskType_en", DataTable.SelectFilterType.EqualsCS, (Integer)taskJob.taskInfo.type_en))[0], arg),
                taskJob.taskIxInQueue, 
                taskJob.taskQueueLength
        );
        
        return workerJob_DoTask(taskJob.vars.childTaskExecutionJob);
    }
    
    public static class TaskExecutionServiceVars<Tvars, Tcls extends TaskExecutant> { 
        TaskExecutionJob<Tvars, Tcls> childTaskExecutionJob;
    }
    
    @Override
    protected final void workerJob_DoTask_OnDbException(Exception ex, TaskService.TaskJob<Ttaskinfo, TaskExecutionService.TaskExecutionServiceVars<Tvars, Tcls>> taskJob)
    {
        workerJob_DoTask_OnDbException(ex, taskJob.vars.childTaskExecutionJob);
    }
    
    @Override
    protected final void workerJob_DoTask_OnDbSuccess(TaskService.TaskJob<Ttaskinfo, TaskExecutionService.TaskExecutionServiceVars<Tvars, Tcls>> taskJob)
    {        
        workerJob_DoTask_OnDbSuccess(taskJob.vars.childTaskExecutionJob);
    }

}
