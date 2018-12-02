package JavaHandlerControlLib.paramTypes;

import CommonLib.Common;
import CommonModelLib.objectModel.FileStorePaths;
import JavaHandlerControlLib.JHCCommonsDaemonService;
import JavaHandlerControlLib.JHCParams;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Objects;

/**
 *
 * 
 */
public class FileStorePathInfoContainer  implements JHCParams.JHCJSONSerializable
{
    static final int FileStorePathInfoWriteTimeOutOnError_sec = 5;
    public static class FileStorePathInfo 
    {
        public final String fileStorePath;
        public final int timeOutOnWriteError_sec;
        public final boolean isReadOnly;
        public FileStorePathInfo(String fileStorePath)                              { this(fileStorePath, FileStorePathInfoWriteTimeOutOnError_sec); }
        public FileStorePathInfo(String fileStorePath, int timeOutOnWriteError_sec) { this(fileStorePath, timeOutOnWriteError_sec, false); }
        public FileStorePathInfo(String fileStorePath, boolean isReadOnly)          { this(fileStorePath, FileStorePathInfoWriteTimeOutOnError_sec, isReadOnly); }
        public FileStorePathInfo(String fileStorePath, int timeOutOnWriteError_sec, boolean isReadOnly)
        {
            if (timeOutOnWriteError_sec < 0 || timeOutOnWriteError_sec > 1800)
                throw new Error("MUSTNEVERTHROW: TimeOutOnError_sec must be between 0 and 1800!");
            this.fileStorePath = fileStorePath; 
            this.timeOutOnWriteError_sec = timeOutOnWriteError_sec;
            this.isReadOnly = isReadOnly;
        }
        private long lastWriteErrorTS = 0;
        private String lastWriteError;
        public void setLastWriteError(String v)
        {
            lastWriteErrorTS = System.currentTimeMillis();
            lastWriteError = v==null?"<null>":v;
        }
        public boolean readyToWrite()
        {
            return !isReadOnly && millisecToReady() == 0;
        }
        public long millisecToReady()
        {
            long result = (lastWriteErrorTS == 0 ? 0 : (timeOutOnWriteError_sec * 1000) - (System.currentTimeMillis() - lastWriteErrorTS));            
            return (result < 0 ? 0 : result);
        }

        @Override
        public boolean equals(Object thatO)
        {
            if (thatO == null || !(thatO instanceof FileStorePathInfo)) return false;
            FileStorePathInfo that = (FileStorePathInfo)thatO;
            return 
                (
                    (this.fileStorePath == null && that.fileStorePath == null)
                    ||
                    (this.fileStorePath != null && this.fileStorePath.equals(that.fileStorePath))
                )
                && 
                this.timeOutOnWriteError_sec == that.timeOutOnWriteError_sec
                && 
                this.isReadOnly == that.isReadOnly
            ;
        }        
        @Override
        public int hashCode() {
            int hash = 7;
            hash = 13 * hash + Objects.hashCode(this.fileStorePath);
            hash = 13 * hash + this.timeOutOnWriteError_sec;
            hash = 13 * hash + (this.isReadOnly?3:5);
            return hash;
        }
    }














    public FileStorePathInfo[] FSPIs;        
    @Override
    public void fromJSONString(String s) {
        org.json.JSONArray a = new org.json.JSONArray(s);
        FSPIs = new FileStorePathInfo[a.length()];
        for(int n = 0; n < a.length(); n++)
            if (a.getJSONObject(n).has("isReadOnly"))
            {
                if (a.getJSONObject(n).has("timeOutOnWriteError_sec"))
                    FSPIs[n] = new FileStorePathInfo(a.getJSONObject(n).getString("fileStorePath"), a.getJSONObject(n).getInt("timeOutOnWriteError_sec"), a.getJSONObject(n).getBoolean("isReadOnly") );
                else
                    FSPIs[n] = new FileStorePathInfo(a.getJSONObject(n).getString("fileStorePath"), a.getJSONObject(n).getBoolean("isReadOnly"));
            }
            else
            {
                if (a.getJSONObject(n).has("timeOutOnWriteError_sec"))
                    FSPIs[n] = new FileStorePathInfo(a.getJSONObject(n).getString("fileStorePath"), a.getJSONObject(n).getInt("timeOutOnWriteError_sec") );
                else
                    FSPIs[n] = new FileStorePathInfo(a.getJSONObject(n).getString("fileStorePath"));
            }
    }        

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 59 * hash + Arrays.deepHashCode(this.FSPIs);
        return hash;
    }        
    @Override
    public boolean equals(Object thatO)
    {
        if (thatO == null || !(thatO instanceof FileStorePathInfoContainer)) return false;
        FileStorePathInfoContainer that = (FileStorePathInfoContainer)thatO;





        return Arrays.deepEquals(this.FSPIs, that.FSPIs);
    }

    public void testWriteAvailable(JHCCommonsDaemonService that, long minFreeSpaceMB) throws Exception
    {
        boolean atLeastOneFileStorePathOK = false;
        for (FileStorePathInfo fsp : FSPIs)
            if (!fsp.isReadOnly)
            {//запрос свободного места на диске        
                String e = Common.writeTest(fsp.fileStorePath, minFreeSpaceMB);
                if (e == null)
                    atLeastOneFileStorePathOK = true;
                else
                {
                    e = "Ошибка проверки папки хранения файлов АИС ПВУ (" + fsp.fileStorePath + "): " + e;
                    that.worker_excLog().write(e);
                    that.getJHC().setHandlerError(e);
                    fsp.setLastWriteError(e);
                }
            }
        if (!atLeastOneFileStorePathOK)
        {
            String e = "Ни одна из папок хранения файлов АИС ПВУ не доступна на запись!";
            that.getJHC().setHandlerError(e);
            throw new Exception(e);
        }
    }
    public FileStorePathInfo getWriteAvailable(JHCCommonsDaemonService that, long minFreeSpaceMB) throws UncheckedIOException
    {
        return getWriteAvailable(that.worker_excLog(), that.getJHC()::setHandlerError, minFreeSpaceMB);
    }
    public FileStorePathInfo getWriteAvailable(Common.Log exclog, Common.Action1<String> addExcWriter, long minFreeSpaceMB) throws UncheckedIOException
    {
        for (FileStorePathInfo fsp : FSPIs)
            if (fsp.readyToWrite())
            {//запрос свободного места на диске        
                String e = Common.writeTest(fsp.fileStorePath, minFreeSpaceMB);
                if (e == null)
                    return fsp;
                else
                {
                    e = "Ошибка проверки папки хранения файлов АИС ПВУ (" + fsp.fileStorePath + "): " + e;
                    exclog.write(e);
                    if (addExcWriter != null)
                        addExcWriter.call(e);
                    fsp.setLastWriteError(e);
                }
            }
        String e = "Ни одна из папок хранения файлов АИС ПВУ не доступна на запись!";
        if (addExcWriter != null)
            addExcWriter.call(e);
        throw new UncheckedIOException(e, new IOException(e));
    }
    private FileStorePathInfo getWriteAvailableCache;
    private final Object getWriteAvailableCacheLOCK = new Object();
    private long getWriteAvailableCacheTS;
    public FileStorePathInfo getWriteAvailableCached(JHCCommonsDaemonService that, long minFreeSpaceMB, int cacheTTL_sec) throws Exception
    {
        if (getWriteAvailableCache == null || getWriteAvailableCacheTS == 0 || System.currentTimeMillis() > (getWriteAvailableCacheTS + (cacheTTL_sec * 1000)))
        {
            synchronized (getWriteAvailableCacheLOCK) 
            {
                if (getWriteAvailableCache == null || getWriteAvailableCacheTS == 0 || System.currentTimeMillis() > (getWriteAvailableCacheTS + (cacheTTL_sec * 1000)))
                {
                    getWriteAvailableCache = getWriteAvailable(that, minFreeSpaceMB);
                    getWriteAvailableCacheTS = System.currentTimeMillis();
                }
            }
        }
        return getWriteAvailableCache;
    }
    public String write(JHCCommonsDaemonService that, long minFreeSpaceMB, int cacheTTL_sec, String filePathSuffix, byte[] fileBody) throws Exception
    {
        FileStorePathInfo fsp = getWriteAvailableCached(that, minFreeSpaceMB, cacheTTL_sec);
        Path fullfilepath = Paths.get(Common.SlashOnEnd(fsp.fileStorePath) + filePathSuffix);

        if (!Files.isDirectory(fullfilepath.getParent()))
            Files.createDirectories(fullfilepath.getParent());  

        Files.write(fullfilepath, fileBody);

        return fullfilepath.toString();
    }
    public String existingFile(String filePathSuffix)
    {
        for (FileStorePathInfo fsp : FSPIs)
        {
            String f = Common.SlashOnEnd(fsp.fileStorePath) + filePathSuffix;
            if (Files.exists(Paths.get(f)))
                return f;
        }
        return null;
    }    
    public Path existingFileTHROWS(String filePathSuffix) throws NoSuchFileException
    {
        String f = existingFile(filePathSuffix);
        if (f == null)
            throw new NoSuchFileException("Файл '" + filePathSuffix + "' не найден в файловом хранилище");
        return Paths.get(f);
    }    
    /**
     * Метод считывает и распаковывает файл из файлового хранилища
     * @param filePathSuffix путь, считанный из таблицы File.filePath
     * @return
     * @throws java.io.IOException
     */
    public byte[] readAndUngzipFileBody(String filePathSuffix) throws IOException
    {
        return Common.UnGZip(readFile(filePathSuffix));
    }
    
    /**
     * Метод считывает файл из файлового хранилища (для чтения мигрированных файлов сканов)
     * @param filePathSuffix путь, считанный из таблицы File.filePath
     * @return
     * @throws java.io.IOException
     */
    public byte[] readFile(String filePathSuffix) throws IOException
    {
        Path f = existingFileTHROWS(filePathSuffix);
        return Files.readAllBytes(f);
    }
    
    ////////////////////////////////
    public FileStorePaths newFileStorePaths(JHCCommonsDaemonService that, long minFreeSpaceMB, int cacheTTL_sec)
    {
        return new FileStorePaths() { 
            @Override public Path getExistingFile(String filePathSuffix) throws NoSuchFileException
            {
                return existingFileTHROWS(filePathSuffix);
            } 
        };
    }

}
