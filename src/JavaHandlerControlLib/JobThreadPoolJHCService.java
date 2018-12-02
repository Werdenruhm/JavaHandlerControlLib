package JavaHandlerControlLib;

import CommonLib.Common;
import java.util.ArrayList;
import java.util.Collection;

/**
 *
 * 
 * @param <Tjob>
 * @param <Tcustomjobdata>
 */
public abstract class JobThreadPoolJHCService<Tjob, Tcustomjobdata> extends JHCCommonsDaemonService
{
    protected JobThreadPoolJHCService(String jarFileName, String handlerTypeAlias, String handlerInstanceTypeEnumAlias, Object settingsFromDB)
    {
        super(jarFileName, handlerTypeAlias, handlerInstanceTypeEnumAlias, Common.ConcatArraysOrObjectsToArray(jobThreadPoolJHCServiceSettings = new JobThreadPoolJHCServiceSettings(), settingsFromDB));
    }
    protected JobThreadPoolJHCService(String jarFileName, String handlerTypeAlias, String handlerInstanceTypeEnumAlias, Object settingsFromDB, int maxStartedInstances)
    {
        super(jarFileName, handlerTypeAlias, handlerInstanceTypeEnumAlias, Common.ConcatArraysOrObjectsToArray(jobThreadPoolJHCServiceSettings = new JobThreadPoolJHCServiceSettings(), settingsFromDB), maxStartedInstances);
    }
    protected JobThreadPoolJHCService(String jarFileName, String handlerTypeAlias, String handlerInstanceTypeEnumAlias, Object settingsFromDB, int maxStartedInstances, String[] requiredSettingsFromFile, int worker_normalLoopSleep_sec, int worker_exceptionLoopSleep_sec)
    {
        super(jarFileName, handlerTypeAlias, handlerInstanceTypeEnumAlias, Common.ConcatArraysOrObjectsToArray(jobThreadPoolJHCServiceSettings = new JobThreadPoolJHCServiceSettings(), settingsFromDB), maxStartedInstances, requiredSettingsFromFile, worker_normalLoopSleep_sec, worker_exceptionLoopSleep_sec);
    }
    
    static JobThreadPoolJHCServiceSettings jobThreadPoolJHCServiceSettings;

    public static class JobThreadPoolJHCServiceSettings { 
        @JHCParams.JHCParamBehaviour(requiresHandlerRestartOnChange = true)
        public int maxMainJobThreads;
    }
    Common.JobThreadPool<JobContainer<Tjob, Tcustomjobdata>> mainJTP;
    final ArrayList<JobExContainer<Tjob, Tcustomjobdata>> mainJTPerrors = new ArrayList<>();
    
    @Override
    public void worker_JustBeforeStart_internal() throws Exception {
        super.worker_JustBeforeStart_internal();
        
        mainJTP = new Common.JobThreadPool<>(jobThreadPoolJHCServiceSettings.maxMainJobThreads, "mainJTP");
        worker_addOnSignalStop(()->
        {
            mainJTP.finishAndStop();
            try
            {
                handleMainJTPerrors();
            }
            catch (Exception ex)
            {
                worker_excLog().write(ex.getMessage());
                getJHC().setHandlerError(ex.toString());
            }
                
        });
        worker_addIsStopped_addon(()->
        {
            return !mainJTP.isGoOnRun();
        });
    }
    
    protected enum JobsResult { ok, exceptions }
    protected Common.Action1THROWS<JobsResult> waitForJobs;
            
    @Override
    public final Worker_DoWork_resultFlags worker_DoWork() throws Exception
    {
        handleMainJTPerrors();

        Worker_getJobs_result<Tjob> gjr = worker_getJobs();
        Collection<Tjob> jobsToDo = gjr == null ? null : gjr.jobs;
        if (jobsToDo != null && !jobsToDo.isEmpty())
        {
            for (Tjob jobToDo : jobsToDo)
            {
                JobContainer<Tjob, Tcustomjobdata> jobContainer = new JobContainer<>();
                jobContainer.job = jobToDo;
                mainJTP.go(this::workerJob_DoWork, jobContainer
                    , (th, jc) -> 
                    {
                        synchronized(mainJTPerrors)
                        {
                            JobExContainer<Tjob, Tcustomjobdata> jec = new JobExContainer<>();
                            jec.job = jc.job;
                            jec.customJobData = jc.customJobData;
                            jec.exMsgPrefix = jc.exMsgPrefix;
                            jec.ex = th;
                            mainJTPerrors.add(jec);
                            if (Common.IsDebug())
                                System.out.println(Common.NowToString() + "   " + th.toString());
                        }
                    }
                );
            }
        }
        if (waitForJobs != null)
        {
            while(mainJTP.isGoOnRun())
                Thread.sleep(1000);
            
            waitForJobs.call(mainJTPerrors.isEmpty() ? JobsResult.ok : JobsResult.exceptions);
        }

        handleMainJTPerrors();
        return gjr == null ? null : new Worker_DoWork_resultFlags(gjr.doFastLoop);
    }
    void handleMainJTPerrors() throws Exception
    {
        if (!mainJTPerrors.isEmpty())
        {
            long ts1 = System.currentTimeMillis();
            while (mainJTP.isGoOnRun())
            {
                Common.sleep(30);
                if ((System.currentTimeMillis() - ts1) > (5 * 60 * 1000))
                {
                    String msg = "В потоках обработки (JTP) произошли исключения, но превышено время ожидания исполняющихся потоков обработки сообщений!";
                    if (worker_hasStopSignal())
                        throwMainJTPerrors(msg + " Подробности: ");
                    else
                        throw new Exception(msg);
                }
            }
            
            try {
                worker_OnJTPExceptions(mainJTPerrors);
            } catch (Exception ex) {
                JobExContainer<Tjob, Tcustomjobdata> jec = new JobExContainer<>();
                jec.exMsgPrefix = "Ошибка в handleMainJTPerrors";
                jec.ex = ex;
                mainJTPerrors.add(jec);
            }
            
            throwMainJTPerrors(null);
        }

    }
    void throwMainJTPerrors(String msgPrefix) throws Exception
    {
        synchronized(mainJTPerrors)
        {
            String inboxJTPerrorsS = "";
            boolean isError = false;
            for (JobExContainer<Tjob, Tcustomjobdata> jec : mainJTPerrors)
            {
                inboxJTPerrorsS += "\n" + (jec.exMsgPrefix == null ? "" : jec.exMsgPrefix + ":\n") + Common.throwableToString(jec.ex, Common.getCurrentSTE()) + Common.hr();
                if (!(jec.ex instanceof Exception))
                    isError = true;
            }
            mainJTPerrors.clear();
            if (isError)
                throw new Error((msgPrefix == null ? "В" : msgPrefix + " в") + " потоках обработки (JTP) произошли критические ошибки:" + inboxJTPerrorsS);
            else
                throw new Exception((msgPrefix == null ? "В" : msgPrefix + " в") + " потоках обработки (JTP) произошли исключения:" + inboxJTPerrorsS);
        }
    }
    
    public static final class Worker_getJobs_result<Tjob> { public final Collection<Tjob> jobs; public final boolean doFastLoop; 
        public Worker_getJobs_result(Collection<Tjob> jobs, boolean doFastLoop) { this.jobs = jobs; this.doFastLoop = doFastLoop; } 
    } 
    protected abstract Worker_getJobs_result<Tjob> worker_getJobs() throws Exception;
    public static class JobContainer<Tjob, Tcustomjobdata> { public Tjob job; public String exMsgPrefix; public Tcustomjobdata customJobData; }
    public static class JobExContainer<Tjob, Tcustomjobdata> extends JobContainer<Tjob, Tcustomjobdata> { public Throwable ex; }
    protected abstract void workerJob_DoWork(JobContainer<Tjob, Tcustomjobdata> jobContainer) throws Exception;
    protected abstract void worker_OnJTPExceptions(ArrayList<JobExContainer<Tjob, Tcustomjobdata>> jobExContainers) throws Exception;
    
    @Override
    public void worker_DoWorkOnException(Exception ex) { }
}
