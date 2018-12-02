package JavaHandlerControlLib.tasking;

import CommonLib.Common;
import DBmethodsLib.DBmethodsPostgres;
import DBmethodsLib.DataTable;
import DBmethodsLib.DbType;
import DBmethodsLib.SqlCodeAnalizer;
import static JavaHandlerControlLib.JHCCommonsDaemonService.dbmethods;
import JavaHandlerControlLib.JHCParams;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import CommonModelLib.dbModel.AnyDictionaryTableWithJavaClass;
import CommonModelLib.dbModel.IEnumCache;
import CommonModelLib.objectModel.FileStorePaths;
import CommonModelLib.objectModel.tasking.TaskInfo;
import JavaHandlerControlLib.paramTypes.FileStorePathInfoContainer;
import CommonModelLib.objectModel.tasking.executants.TaskProducingFilesExecutant;
import CommonModelLib.objectModel.tasking.executants.TaskRetryException;

/**
 *
 * 
 * @param <Tvars> тип вспомогательного объекта-контейнера для переменных, передаваемых из метода исполнения задачи
 * ({@link TaskExecutionService#workerJob_DoTask(rsa.pvu2.handlerCommonLib.taskExecution.TaskExecutionService.TaskJob) }) в метод обработки ошибок 
 * ({@link TaskExecutionService#workerJob_DoTaskOnException(java.lang.Exception, rsa.pvu2.handlerCommonLib.taskExecution.TaskExecutionService.TaskJob)  })
 * @param <Tienumcache>
 * @param <Ttaskinfo>
 * @param <Tcls> супер-класс обработчиков задач
 * @param <Tctorarg> тип агрумента для конструктора экземпляров класса <code>Tcls</code>
 * @param <Ttaskproducingfilesfile>
 * @param <Ttaskproducingfilesresult>
 */
public abstract class TaskProducingFilesExecutionService<Tvars, Tienumcache extends IEnumCache, Ttaskinfo extends TaskInfo, Tcls extends TaskProducingFilesExecutant<Ttaskinfo, Tienumcache>, Tctorarg  extends TaskProducingFilesExecutant.TaskProducingFilesExecutantCtor<Ttaskinfo, Tienumcache>, Ttaskproducingfilesfile extends TaskProducingFilesExecutant.TaskProducingFilesResult.TaskProducingFilesFile<Ttaskinfo>, Ttaskproducingfilesresult extends TaskProducingFilesExecutant.TaskProducingFilesResult<Ttaskproducingfilesfile>> 
extends TaskExecutionService<TaskProducingFilesExecutionService.TaskProducingFilesExecutionServiceVars<Tvars, Tcls>, Tienumcache, Ttaskinfo, Tcls, Tctorarg>
{    
    public TaskProducingFilesExecutionService(String jarFileName, String handlerTypeAlias, String handlerInstanceTypeEnumAlias, Object settingsFromDB, int maxStartedInstances, String[] requiredSettingsFromFile, 
            int worker_normalLoopSleep_sec, int worker_exceptionLoopSleep_sec, String factType_alias, 
            Class<? extends Tienumcache> c_Tienumcache, Class<? extends Ttaskinfo> c_Ttaskinfo,
            AnyDictionaryTableWithJavaClass.JavaClassInstantiator<Tcls, Tctorarg> javaClassInstantiator, Tctorarg ctorArgsForCheck) 
    {
        super(jarFileName, handlerTypeAlias, handlerInstanceTypeEnumAlias, Common.ConcatArraysOrObjectsToArray(taskProducingFilesExecutionServiceSettings = new TaskProducingFilesExecutionServiceSettings(), settingsFromDB), maxStartedInstances, requiredSettingsFromFile, worker_normalLoopSleep_sec, worker_exceptionLoopSleep_sec, factType_alias, c_Tienumcache, c_Ttaskinfo, javaClassInstantiator, ctorArgsForCheck);
    }
    
    static TaskProducingFilesExecutionServiceSettings taskProducingFilesExecutionServiceSettings;
    
    public static class TaskProducingFilesExecutionServiceSettings {
        @JHCParams.JHCParamBehaviour(requiresHandlerRestartOnChange = true)
        public FileStorePathInfoContainer fileStorePaths;
        @JHCParams.JHCParamBehaviour(requiresHandlerRestartOnChange = true, isRequired = false)
        public String msgId_prefix;
    } 
    
    FileStorePaths fsps;
    Common.Func1<Integer, String[]> midg;
    
    @Override
    public final void worker_JustBeforeStart_internal() throws Exception {
        ctorArgsForCheck.fileStorePaths = new FileStorePathInfoContainer().newFileStorePaths(null, 0, 0);
        ctorArgsForCheck.msgIdsGenerator = (cnt) -> null;
        
        super.worker_JustBeforeStart_internal();  
        
        taskProducingFilesExecutionServiceSettings.fileStorePaths.testWriteAvailable(this, 100);
        
        fsps = taskProducingFilesExecutionServiceSettings.fileStorePaths.newFileStorePaths(this, 100, 5);
        
        midg = (cnt) -> 
        {
            if (taskProducingFilesExecutionServiceSettings.msgId_prefix == null)
                throw new RuntimeException("Настройка msgId_prefix не была задана для обработчика, но потребовалась!");
            String[] ids = new String[cnt];
            String idsSQL = null;
            for (int n = 0; n < cnt; n++)
                idsSQL = (idsSQL == null ? "" : idsSQL + "\n union all \n") + "select nextval('msgid_seq')";                        
            DataTable dt;
            try {
                dt = dbmethods.select_from(true, idsSQL)[0];
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            for (int n = 0; n < cnt; n++)
                ids[n] = taskProducingFilesExecutionServiceSettings.msgId_prefix + dt.Rows.get(n).get(0).toString();            
            return ids;
        };
    }
    
    @Override
    protected void workerJob_RefillTaskExecutantCtor(Tctorarg ctorarg) {
        ctorarg.fileStorePaths = fsps;
        ctorarg.msgIdsGenerator = midg;
    }
    
    @Override
    protected final String workerJob_DoTask(TaskExecutionJob<TaskProducingFilesExecutionService.TaskProducingFilesExecutionServiceVars<Tvars, Tcls>, Tcls> taskExecutionJob) throws Exception, TaskRetryException
    {
        FileStorePathInfoContainer.FileStorePathInfo fsp = taskProducingFilesExecutionServiceSettings.fileStorePaths.getWriteAvailableCached(this, 100, 5);
        taskExecutionJob.vars = new TaskProducingFilesExecutionServiceVars<>();
        
        taskExecutionJob.vars.childTaskExecutionJob = new TaskExecutionJob<>(
                taskExecutionJob.taskExecutant, 
                taskExecutionJob.taskIxInQueue, 
                taskExecutionJob.taskQueueLength
        );        
        Ttaskproducingfilesresult res = workerJob_DoTaskProducingFiles(taskExecutionJob.vars.childTaskExecutionJob);

        if (res.PLpgSQL == null || res.PLpgSQL.trim().equals(""))
            throw new NullPointerException("реализация workerJob_DoTaskProducingFiles() вернула пустую PL/pgSQL-команду");

        String insSQL = "\n";
        Long[] file_ids;
        try
        {            
            if (res.getFiles() == null || res.getFiles().length == 0)
                throw new NullPointerException("реализация doTask() вернула пустой набор Result.files");
            for (int n = 0; n < res.getFiles().length; n++)
                if (res.getFiles()[n] == null)
                    throw new NullPointerException("реализация doTask() вернула пустой Result.files[" + n + "]");

            file_ids = Common.arrayTransform(res.getFiles(), Long[].class, (f) -> (Long)f.getFile_id(dbmethods));

            ArrayList<Ttaskproducingfilesfile> newFiles = new ArrayList<>();
            for (int n = 0; n < res.getFiles().length; n++)
                if (!res.getFiles()[n].isExistingFile)
                    newFiles.add(res.getFiles()[n]);

            taskExecutionJob.vars.fullNewFilePaths = new Path[newFiles.size()];
            for (int n = 0; n < newFiles.size(); n++)
            {            
                Ttaskproducingfilesfile f = newFiles.get(n);
                byte[] newFile_zip = Common.GZip(f.fileBody());

                String newFile_path = f.filePath(getJHC().dbNow(), dbmethods, taskExecutionJob.taskExecutant.taskInfo);

                insSQL +=
                      "    insert into File ("
                    + "file_id, "
                    + "format_en, "
                    + "filePath, "
                    + "fileSize"
                    + ")\n"
                    + "    values (" 
                    + f.getFile_id(dbmethods) + ", "
                    + "enum_id(" + DBmethodsPostgres.getSqlValue(f.en_FileFormat(), DbType.VARCHAR) + "), "
                    + DBmethodsPostgres.getSqlValue(newFile_path, DbType.VARCHAR) + ", "                    
                    + newFile_zip.length
                    + ");\n"
                    + "\n";

                Path fullfilepath = Paths.get(Common.SlashOnEnd(fsp.fileStorePath) + newFile_path);

                // создание папки если нет
                if (!Files.isDirectory(fullfilepath.getParent()))
                    Files.createDirectories(fullfilepath.getParent());

                // сохранение
                Files.write(fullfilepath, newFile_zip);
                if (Common.IsDebug())
                    worker_msgLog().write("Записан файл " + fullfilepath + "");

                taskExecutionJob.vars.fullNewFilePaths[n] = fullfilepath;
            }
        }
        catch (TaskProducingFilesExecutant.TaskProducingFilesResult.MustBeNoFilesException ex) 
        { 
            file_ids = null;
        }
        
        return    "DECLARE\n"
                + "    " + TaskProducingFilesExecutant.file_ids_varName + " bigint[] := " + DBmethodsPostgres.getSqlValue(file_ids, DbType.ARRAY, DbType.BIGINT) + ";\n"//newFiles
                + "BEGIN\n"
                + insSQL + "\n"
                + "    --<TaskProducingFilesExecutant PLpgSQL>\n" 
                + "    " + SqlCodeAnalizer.instance.tab(res.PLpgSQL) + "\n"
                + "    --</TaskProducingFilesExecutant PLpgSQL>\n"
                + "END;\n";
    }
    /**
     * Метод, содержащий всю логику исполнения задачи, кроме записи в БД и файловое хранилище
     * @param taskExecutionJob 
     * @return PL/pgSQL, который должен выполниться для выполнения задачи
     * @throws Exception
     * @throws CommonModelLib.objectModel.tasking.executants.TaskRetryException
     */
    protected abstract Ttaskproducingfilesresult workerJob_DoTaskProducingFiles(TaskExecutionJob<Tvars, Tcls> taskExecutionJob) throws Exception, TaskRetryException;
    
    /**
     * Метод обработки ошибок исполнения sql, возвращенного методом {@link TaskProducingFilesExecutionService#workerJob_DoTaskProducingFiles(rsa.pvu2.handlerCommonLib.taskExecution.TaskExecutionService.TaskJob) }
     * @param ex произошедшее исключение
     * @param taskExecutionJob
     */
    protected abstract void workerJob_DoTaskProducingFiles_OnDbException(Exception ex, TaskExecutionJob<Tvars, Tcls> taskExecutionJob);
    
    /**
     * Метод обработки успешной записи в БД sql, возвращенного методом {@link TaskProducingFilesExecutionService#workerJob_DoTaskProducingFiles(rsa.pvu2.handlerCommonLib.taskExecution.TaskExecutionService.TaskJob) }
     * @param taskExecutionJob
     */
    protected abstract void workerJob_DoTaskProducingFiles_OnDbSuccess(TaskExecutionJob<Tvars, Tcls> taskExecutionJob);
    

    public static class TaskProducingFilesExecutionServiceVars<Tvars, Tcls extends TaskProducingFilesExecutant> { 
        public Path[] fullNewFilePaths; 
        TaskExecutionJob<Tvars, Tcls> childTaskExecutionJob;
    }
    
    @Override
    protected final void workerJob_DoTask_OnDbException(Exception ex, TaskExecutionJob<TaskProducingFilesExecutionService.TaskProducingFilesExecutionServiceVars<Tvars, Tcls>, Tcls> taskExecutionJob) {
        if (taskExecutionJob.vars != null) 
        { 
            if (taskExecutionJob.vars.fullNewFilePaths != null)
            {
                for(Path f : taskExecutionJob.vars.fullNewFilePaths)
                {
                    if (f != null)
                    {
                        try
                        {
                            Files.delete(f);
                            if (Common.IsDebug())
                                worker_msgLog().write("Удален файл " + f.toString() + "");

                        }
                        catch(Exception fex)
                        {
                            worker_excLog().write(fex, Common.getCurrentSTE(), "Ошибка удаления файла " + f);
                        }
                    }
                }
            }
            workerJob_DoTaskProducingFiles_OnDbException(ex, taskExecutionJob.vars.childTaskExecutionJob);
        }
    }
    
    @Override
    protected final void workerJob_DoTask_OnDbSuccess(TaskExecutionJob<TaskProducingFilesExecutionService.TaskProducingFilesExecutionServiceVars<Tvars, Tcls>, Tcls> taskExecutionJob) {
        workerJob_DoTaskProducingFiles_OnDbSuccess(taskExecutionJob.vars.childTaskExecutionJob);
    }
}
