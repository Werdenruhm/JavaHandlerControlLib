package JavaHandlerControlLib;

import CommonLib.Common;
import CommonLib.XmlSettingsBase;
import DBmethodsLib.DBmethodsCommon;
import DBmethodsLib.DBmethodsPostgres;
import java.awt.SystemColor;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.util.Objects;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JTextField;
import javax.swing.JTextPane;
import javax.swing.WindowConstants;
import javax.swing.border.BevelBorder;
import javax.swing.text.BadLocationException;

/**
 *
 * 
 */
public abstract class JHCApplication 
{    
    private static volatile Class<? extends JHCApplication> singletonClass;
    private static volatile JHCApplication singleton;
    private static final Object singletonLOCK = new Object();
    protected static void setSingletonClass(Class<? extends JHCApplication> c) 
    {
        synchronized (singletonLOCK)
        {
            if (singletonClass != null)
                throw new Error("MUSTNEVERTHROW: попытка вызвать setSingletonClass() повторно!");
            singletonClass = c; 
        }
    }
    private static void setSingleton() 
    {                 
        try {
            setSingleton(singletonClass.newInstance());
        } catch (Exception ex) {
            throw new Error("MUSTNEVERTHROW: при попытке вызвать конструктор по-умолчанию произошла ошибка: " + ex.toString());
        }
    }
    private static void setSingleton(JHCApplication v) 
    { 
        synchronized (singletonLOCK)
        {
            if (singleton != null)
                throw new Error("MUSTNEVERTHROW: попытка вызвать setSingleton() повторно!");
            singleton = v; 
        }
    }
    private static void chkSingletonClass(Class<? extends JHCApplication> c)
    {
        if (singletonClass == null)
            throw new Error("MUSTNEVERTHROW: класс-наследник, через который происходит запуск сервиса, должен иметь статический конструктор, в котором должен присутствовать вызов метода setSingletonClass(<класс-наследник>), например:\r\n" +
                " static { setSingletonClass(Мой_класс_наследник_от_JHCApplication.class); } \r\n\r\n"
            );   
        if (c != null && c != singletonClass)
            throw new Error("MUSTNEVERTHROW: класс-наследник singletonClass (" + singletonClass.getCanonicalName() + "), не является классом, из которого произошел запуск службы (" + c.getCanonicalName() + ")!\r\n"
            );  
    }
    
    
    
    
    public final String appName;
    public final String jarFileName;
    final String[] requiredSettingsFromFile;
    final protected XmlSettingsBase settingsFromFile;
    
    public final String handlerTypeAlias;
    final Object settingsFromDB;
    final int maxWriteableInstances;
    final private XmlSettingsBase dbConnectionSettingsFromFile;
    protected JHCApplication(String appName, String jarFileName, String handlerTypeAlias, Object settingsFromDB)
    {
        this(appName, jarFileName, handlerTypeAlias, settingsFromDB, 0, null);
    }
    protected JHCApplication(String appName, String jarFileName, String handlerTypeAlias, Object settingsFromDB, int maxWriteableInstances)
    {
        this(appName, jarFileName, handlerTypeAlias, settingsFromDB, maxWriteableInstances, null);
    }
    protected JHCApplication(String appName, String jarFileName, String handlerTypeAlias, Object settingsFromDB, int maxWriteableInstances, String[] requiredSettingsFromFile)
    {
        this.appName = appName;
        this.jarFileName = jarFileName;
        this.requiredSettingsFromFile = requiredSettingsFromFile;
        this.settingsFromFile = requiredSettingsFromFile == null || requiredSettingsFromFile.length == 0 ? null : new XmlSettingsBase(jarFileName, jarFileName + ".settings", true, requiredSettingsFromFile);

        this.dbConnectionSettingsFromFile = new XmlSettingsBase(jarFileName, "dbConnection.settings", true, new String[] { "DB_URL", "DB_USER" });
        this.handlerTypeAlias = Objects.requireNonNull(handlerTypeAlias);
        this.settingsFromDB = settingsFromDB;
        this.maxWriteableInstances = maxWriteableInstances;
    }    
    protected final String settingsFromFile_DB_URL()          {            return dbConnectionSettingsFromFile.getProperty("DB_URL");         }
    protected final String settingsFromFile_DB_USER()         {            return dbConnectionSettingsFromFile.getProperty("DB_USER");         }
            
    public static DBmethodsCommon dbmethods;

    JHCParams jhcparams;


    
    

    

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    public static void main(String[] args)
    {        
        chkSingletonClass(null);
        
        setSingleton();
        
        try {
            if (singleton.beforeStart())            
                singleton.onStart(args);
            else
                singleton.msgLog.write("выход");
        } catch (Exception ex) {
            JOptionPane.showMessageDialog(null, "Ошибка запуска приложения: " + ex.getMessage(), singleton.appName, JOptionPane.ERROR_MESSAGE);
            singleton.exit_internal(true, Common.throwableToString(ex, Common.getCurrentSTE()));
        }        
    }


    protected Common.Log msgLog;
    protected Common.Log excLog;



    private JavaHandlerControl JHC;
    public final JavaHandlerControl getJHC()
    {
        return JHC;
    }            

    public final boolean beforeStart() throws Exception
    {
        String logpath = Common.SlashOnEnd(Common.SlashOnEnd(System.getProperty("user.home")) + jarFileName);
        msgLog =  new Common.Log(false, "messagesLog", true, logpath);
        excLog = new Common.Log(false, "errorsLog", true, logpath);  

        String DB_URL = DBmethodsPostgres.DB_URL_appendPostgesApplicationName(settingsFromFile_DB_URL(), jarFileName, "MUSTNEVERTHROW: please remove ApplicationName from DB_URL parameter!");

        if (!DB_URL.toLowerCase().startsWith("jdbc:postgresql:"))
            throw new Error("MUSTNEVERTHROW: DB_URL must be jdbc:postgresql!");
        
        DBmethodsPostgres tmp;
        String DB_USER = settingsFromFile_DB_USER();
        String DB_PASS = dbConnectionSettingsFromFile.getProperty("DB_PASS");
        String DB_PASSerrmsg = "";
        for (int n = 0; true; n++)
        {
            try
            {
                //Object o = JOptionPane.showInputDialog(null, DB_PASSerrmsg + "Пользователь: " + settingsFromFile_DB_USER() + "\n\nВведите пароль:", appName, JOptionPane.QUESTION_MESSAGE, null, null, dbConnectionSettingsFromFile.getProperty("DB_PASS"));
                AppCommon.passwordCheckRESULT res = AppCommon.passwordCheck(null, DB_PASSerrmsg, appName, DB_USER, DB_PASS);
                if (res == null)
                    return false;
                DB_USER = res.user;
                DB_PASS = res.pass;
                tmp = new DBmethodsPostgres(DB_URL, DB_USER, DB_PASS);
                tmp.select_from(true, "select 1");
                break;
            }
            catch(Exception ex)
            {
                if (ex.getMessage() != null && ex.getMessage().contains("password authentication failed"))
                {
                    excLog.write("Неудачная попытка входа");
                    Thread.sleep(1000);
                    if (n > 2)
                    {
                        JOptionPane.showMessageDialog(null, "Превышено кол-во неудачных попыток!", appName, JOptionPane.ERROR_MESSAGE);
                        return false;
                    }
                    DB_PASSerrmsg = "Неудачная попытка входа: " + ex.getMessage() + Common.br() + "Попробуйте заново:\n";
                }
                else
                    throw Common.rethrow(ex);

            }
        }
            


        //получить настройки из базы;
        if (settingsFromDB != null)
        {
            jhcparams = new JHCParams(tmp, handlerTypeAlias, null, Common.ConcatArraysOrObjectsToArray(null, settingsFromDB), null);
            try
            {
                jhcparams.fillParamssByDB();
            }
            catch(Exception ex)
            {
                String errprefix = "Ошибка чтения параметров из БД на старте сервиса";
                excLog.write(ex, Common.getCurrentSTE(), errprefix);
                return false;
            }
        }


        dbmethods = new DBmethodsPostgres(DB_URL, DB_USER, DB_PASS, 0);
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



        JHC = JavaHandlerControl.startJHC(dbmethods, handlerTypeAlias, null, msgLog, excLog, null, null, null, 
            () -> {
                exit_internal(false, "JHC app died");
            },
            3, 
            0                
        );

        dbmethods.addConstantTransacParam("jhc_id", DBmethodsCommon.SessionOrTransacParamTypeENUM.n, getJHC().getJhc_id());

        readonly = !getJHC().markWorkerStarted(maxWriteableInstances);
        msgLog.write("Приложение запущено в " + (readonly ? "режиме 'только чтение'" : "полноценном режиме") + ".");
        return true;
    }   
    
    public abstract void onStart(String[] args) throws Exception;
    
    private Boolean readonly;
    public static boolean isReadonly()
    {
        return singleton.readonly; 
    }

    public final boolean hasStopSignal()
    {
        return JHC.hasStopSignal();
    }


    private synchronized void exit_internal(boolean iserr, String message)
    {
        try
        {
            String logmsg = "Завершение приложения" + (message != null ? " по причине: " + message : ".");
            (iserr ? excLog : msgLog).writeSync(logmsg);
            (iserr ? System.err : System.out).println(Common.NowToString() + "    " + logmsg);
            if (JHC != null)
                JHC.signalStop(true);
        }
        finally
        {
            System.exit(0);
        }
    }    
    public static void exit(boolean iserr, String message)    
    {
        singleton.exit_internal(iserr, message);
    }
    

    
    
    
    
    
    
    public static class AppCommon 
    {

        public static class passwordCheckRESULT { String user; String pass; }
        public static passwordCheckRESULT passwordCheck(JFrame parent, String message, String title, String defaultUser, String defaultPass) {
        final JDialog dbDlg = new JDialog(parent, title,true);
            JPanel dbpanel = new JPanel();
            dbpanel.setLayout(null);
            int dY1 = 0;
            if (message != null && message.length() > 0)
            {     
                int msgH = (int)(20 + 20 * Math.ceil(message.length() / 50));
                JTextPane msg = new JTextPane();
                msg.setEditable(false);
                msg.setContentType("text/html");
                msg.setBackground(SystemColor.control);
                try {
                    msg.getDocument().insertString(0, message, null);
                } catch (BadLocationException ex) {
                    throw new RuntimeException(ex);
                }
                //dbDlg.setTitle(msgH + " " + message.length());
                msg.setSize(300, msgH);
                msg.setLocation(20,20);
                //namelabel.setBorder(new BevelBorder(BevelBorder.RAISED));
                dbpanel.add(msg);
                dY1 = msgH + 20;
            }
            dbDlg.setSize(340,180 + dY1);
            dbDlg.setLocationRelativeTo(null);
            dbpanel.setSize(340,180 + dY1);
            dbpanel.setBorder(new BevelBorder(BevelBorder.RAISED));

            JLabel namelabel = new JLabel("Пользователь:");
            namelabel.setSize(100,20);
            namelabel.setLocation(10,20 + dY1);
            //namelabel.setBorder(new BevelBorder(BevelBorder.RAISED));
            dbpanel.add(namelabel);

            JTextField namefield = new JTextField();
            namefield.setSize(200,20);
            namefield.setLocation(110,20 + dY1);
            namefield.setText(defaultUser);
            dbpanel.add(namefield);


            JLabel passwdlabel = new JLabel("Пароль:");
            passwdlabel.setSize(100,20);
            passwdlabel.setLocation(10,50 + dY1);
            //passwdlabel.setBorder(new BevelBorder(BevelBorder.LOWERED));    
            dbpanel.add(passwdlabel);

            final JPasswordField passwdfield = new JPasswordField();
            passwdfield.setSize(200,20);
            passwdfield.setLocation(110,50 + dY1);
            passwdfield.setEchoChar('*');                    
            passwdfield.setText(defaultPass);
            dbpanel.add(passwdfield);

            namefield.addKeyListener(new KeyAdapter() { @Override public void keyPressed(KeyEvent evt) {
                if (evt.getKeyCode()==KeyEvent.VK_ESCAPE) dbDlg.dispose();
                if (evt.getKeyCode()==KeyEvent.VK_ENTER) passwdfield.requestFocus();}}
            );

            Common.Container<passwordCheckRESULT> cont = new Common.Container<>();
            Common.Action ok = () -> {
                cont.value = new passwordCheckRESULT();
                cont.value.user = namefield.getText();
                cont.value.pass = new String(passwdfield.getPassword());
            };

            passwdfield.addKeyListener(new KeyAdapter() { @Override public void keyPressed(KeyEvent evt) {
                if (evt.getKeyCode()==KeyEvent.VK_ESCAPE) dbDlg.dispose();
                if (evt.getKeyCode()==KeyEvent.VK_ENTER) {dbDlg.dispose(); ok.call();}}}
            );

            JButton okbtn = new JButton("OK");
            okbtn.setSize(120,30);
            okbtn.setLocation(45,90 + dY1);
            dbpanel.add(okbtn);

            okbtn.addActionListener(new ActionListener(){ @Override public void actionPerformed(ActionEvent e){dbDlg.dispose();ok.call();}});
            JButton canbtn = new JButton("Отмена");
            canbtn.setSize(120,30);
            canbtn.setLocation(185,90 + dY1);
            canbtn.addActionListener(new ActionListener(){ @Override public void actionPerformed(ActionEvent e){dbDlg.dispose();}});
            dbpanel.add(canbtn);

            dbDlg.getContentPane().add(dbpanel);
            dbDlg.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
            dbDlg.setVisible(true);
            return cont.value;
        }   
    }
}
