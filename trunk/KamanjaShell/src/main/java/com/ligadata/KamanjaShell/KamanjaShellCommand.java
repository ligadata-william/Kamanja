package com.ligadata.KamanjaShell;

import java.util.HashMap;
import java.util.ArrayList;
/**
 * Created by danielkozin on 9/16/15.
 */
public class KamanjaShellCommand {

    //verbs
    public static String CREATE_VERB = "create";
    public static String PUSH_VERB = "push";
    public static String CONFIGURE_VERB = "configure";
    public static String SET_VERB = "set";
    public static String START_VERB = "start";
    public static String ADD_VERB = "add";
    public static String REMOVE_VERB = "remove";
    public static String GET_VERB = "get";
    public static String UPDATE_VERB = "update";


    // Subjects
    public static String KAFKA_TOPICS = "kafka_topic";
    public static String DATA = "data";
    public static String ENGINE = "engine";
    public static String APP_PATH = "appPath";
    public static String MODEL = "model";
    public static String MESSAGE = "message";
    public static String CONTAINER = "container";

    // Options
    public static String TOPIC_NAME = "name";
    public static String NUMBER_OF_PARTITIONS = "partitions";
    public static String REPLICATION_FACTOR = "replicationFactor";
    public static String TARGET_TOPIC = "topic";
    public static String SOURCE_FILE = "fromFile";
    public static String PARTITION_ON  = "partitionId";
    public static String CONFIG  = "config";
    public static String PATH  = "path";
    public static String FILTER  = "filter";
    public static String MODEL_CONFIG  = "configName";
    public static String ABSOLUTE_PATH  = "isFullPath";
    public static String KEY  = "key";

    private static java.util.ArrayList<String> allVerbs = new java.util.ArrayList<String>();
    private static java.util.ArrayList<String> allSubjects = new java.util.ArrayList<String>();
    private static java.util.ArrayList<String> allOptions = new java.util.ArrayList<String>();


    private  static HashMap<String,ArrayList<HashMap<String,ArrayList<String>>>> commandMatrix = new HashMap<String,ArrayList<HashMap<String,ArrayList<String>>>>();

    private static ArrayList<HashMap<String,ArrayList<String>>> createVerb_subjects = new ArrayList<HashMap<String,ArrayList<String>>>();
    private static ArrayList<HashMap<String,ArrayList<String>>> pushVerb_subjects = new ArrayList<HashMap<String,ArrayList<String>>>();
    private static ArrayList<HashMap<String,ArrayList<String>>> configureVerb_subjects = new ArrayList<HashMap<String,ArrayList<String>>>();
    private static ArrayList<HashMap<String,ArrayList<String>>> setVerb_subjects = new ArrayList<HashMap<String,ArrayList<String>>>();
    private static ArrayList<HashMap<String,ArrayList<String>>> startVerb_subjects = new ArrayList<HashMap<String,ArrayList<String>>>();
    private static ArrayList<HashMap<String,ArrayList<String>>> addVerb_subjects = new ArrayList<HashMap<String,ArrayList<String>>>();
    private static ArrayList<HashMap<String,ArrayList<String>>> getVerb_subjects = new ArrayList<HashMap<String,ArrayList<String>>>();
    private static ArrayList<HashMap<String,ArrayList<String>>> removeVerb_subjects = new ArrayList<HashMap<String,ArrayList<String>>>();
    private static ArrayList<HashMap<String,ArrayList<String>>> updateVerb_subjects = new ArrayList<HashMap<String,ArrayList<String>>>();

    private static HashMap<String,ArrayList<String>> start_engine_subject = new HashMap<String,ArrayList<String>>();
    private static HashMap<String,ArrayList<String>> create_kafka_subject = new HashMap<String,ArrayList<String>>();
    private static HashMap<String,ArrayList<String>> set_apppath_subject = new HashMap<String,ArrayList<String>>();
    private static HashMap<String,ArrayList<String>> push_data_subject = new HashMap<String,ArrayList<String>>();
    private static HashMap<String,ArrayList<String>> configure_engine_subject = new HashMap<String,ArrayList<String>>();
    private static HashMap<String,ArrayList<String>> add_modeldef_subject = new HashMap<String,ArrayList<String>>();
    private static HashMap<String,ArrayList<String>> add_msgdef_subject = new HashMap<String,ArrayList<String>>();
    private static HashMap<String,ArrayList<String>> add_contdef_subject = new HashMap<String,ArrayList<String>>();
    private static HashMap<String,ArrayList<String>> get_modeldef_subject = new HashMap<String,ArrayList<String>>();
    private static HashMap<String,ArrayList<String>> get_msgdef_subject = new HashMap<String,ArrayList<String>>();
    private static HashMap<String,ArrayList<String>> get_contdef_subject = new HashMap<String,ArrayList<String>>();
    private static HashMap<String,ArrayList<String>> remove_modeldef_subject = new HashMap<String,ArrayList<String>>();
    private static HashMap<String,ArrayList<String>> remove_msgdef_subject = new HashMap<String,ArrayList<String>>();
    private static HashMap<String,ArrayList<String>> remove_contdef_subject = new HashMap<String,ArrayList<String>>();
    private static HashMap<String,ArrayList<String>> update_modeldef_subject = new HashMap<String,ArrayList<String>>();
    private static HashMap<String,ArrayList<String>> update_msgdef_subject = new HashMap<String,ArrayList<String>>();
    private static HashMap<String,ArrayList<String>> update_contdef_subject = new HashMap<String,ArrayList<String>>();

    // - CONFIGURE KAFKA_TOPICS
    private static ArrayList<String> create_kafka_options = new ArrayList<String>();
    private static ArrayList<String> start_engine_options = new ArrayList<String>();
    private static ArrayList<String> set_appPath_options = new ArrayList<String>();
    private static ArrayList<String> push_data_options = new ArrayList<String>();
    private static ArrayList<String> configure_engine_options = new ArrayList<String>();
    private static ArrayList<String> add_modelDef_options = new ArrayList<String>();
    private static ArrayList<String> add_msgDef_options = new ArrayList<String>();
    private static ArrayList<String> remove_mdDef_options = new ArrayList<String>();
    private static ArrayList<String> get_mdDef_options = new ArrayList<String>();
    private static ArrayList<String> update_mdDef_options = new ArrayList<String>();


    //configure_kafka_options.

    // SubMatrix

    // private static java.util.HashMap<String,java.util.ArrayList> createVerbInfo = new java.util.HashMap<String,java.util.ArrayList>();
   // private static java.util.HashMap<String,java.util.ArrayList> pushVerbInfo = new java.util.HashMap<String,java.util.ArrayList>();
  //  private static java.util.HashMap<String,java.util.ArrayList> configureVerbInfo = new java.util.HashMap<String,java.util.ArrayList>();
  //  private static java.util.HashMap<String,java.util.ArrayList> createVerbInfo = new java.util.HashMap<String,java.util.ArrayList>();
  //  private static java.util.HashMap<String,java.util.ArrayList> createVerbInfo = new java.util.HashMap<String,java.util.ArrayList>();
  //  private static java.util.HashMap<String,java.util.ArrayList> createVerbInfo = new java.util.HashMap<String,java.util.ArrayList>();

    public static void init() {

        // CREATE KAFKA TOPICS
        create_kafka_options.add(TOPIC_NAME);
        create_kafka_options.add(NUMBER_OF_PARTITIONS);
        create_kafka_options.add(REPLICATION_FACTOR);
        create_kafka_subject.put(KAFKA_TOPICS, create_kafka_options);


        // START ENGINE has no options

        // SET APPPATH
        set_appPath_options.add(PATH);
        set_apppath_subject.put(APP_PATH, set_appPath_options);

        // PUSH DATA
        push_data_options.add(TARGET_TOPIC);
        push_data_options.add(SOURCE_FILE);
        push_data_options.add(PARTITION_ON);
        push_data_subject.put(DATA, push_data_options);

        // CONFIGURE ENGINE
        configure_engine_options.add(CONFIG);
        configure_engine_subject.put(ENGINE, configure_engine_options);

        // ADD/Udate modelDEF
        add_modelDef_options.add(PATH);
        add_modelDef_options.add(ABSOLUTE_PATH);
        add_modelDef_options.add(MODEL_CONFIG);
        add_modeldef_subject.put(MODEL, add_modelDef_options);
        update_modeldef_subject.put(MODEL, add_modelDef_options);

        // ADD/UPdate MSG/CONTAINER DEF
        add_msgDef_options.add(PATH);
        add_msgDef_options.add(ABSOLUTE_PATH);
        add_msgdef_subject.put(MESSAGE, add_msgDef_options);
        add_contdef_subject.put(CONTAINER, add_msgDef_options);
        update_msgdef_subject.put(MESSAGE, add_msgDef_options);
        update_contdef_subject.put(CONTAINER, add_msgDef_options);

        // GET/Remove Message/Container DEF
        get_mdDef_options.add(KEY);
        get_modeldef_subject.put(MODEL, get_mdDef_options);
        get_msgdef_subject.put(MESSAGE,get_mdDef_options);
        get_contdef_subject.put(CONTAINER, get_mdDef_options);
        remove_modeldef_subject.put(MODEL, get_mdDef_options);
        remove_msgdef_subject.put(MESSAGE, get_mdDef_options);
        remove_contdef_subject.put(CONTAINER, get_mdDef_options);


        // Verb to valid Subjects mapping
        createVerb_subjects.add(create_kafka_subject);
        pushVerb_subjects.add(push_data_subject);
        configureVerb_subjects.add(configure_engine_subject);
        setVerb_subjects.add(set_apppath_subject);
        startVerb_subjects.add(start_engine_subject);
        addVerb_subjects.add(add_modeldef_subject);
        addVerb_subjects.add(add_msgdef_subject);
        addVerb_subjects.add(add_contdef_subject);
        getVerb_subjects.add(get_msgdef_subject);
        getVerb_subjects.add(get_contdef_subject);
        getVerb_subjects.add(get_modeldef_subject);
        removeVerb_subjects.add(remove_modeldef_subject);
        removeVerb_subjects.add(remove_msgdef_subject);
        removeVerb_subjects.add(remove_contdef_subject);
        updateVerb_subjects.add(update_modeldef_subject);
        updateVerb_subjects.add(update_msgdef_subject);
        updateVerb_subjects.add(update_contdef_subject);

        // Complete the matrix
        commandMatrix.put(CREATE_VERB, createVerb_subjects);
        commandMatrix.put(PUSH_VERB, pushVerb_subjects);
        commandMatrix.put(CONFIGURE_VERB, configureVerb_subjects);
        commandMatrix.put(SET_VERB, setVerb_subjects);
        commandMatrix.put(START_VERB, startVerb_subjects);
        commandMatrix.put(ADD_VERB, addVerb_subjects);
        commandMatrix.put(REMOVE_VERB, removeVerb_subjects);
        commandMatrix.put(GET_VERB, getVerb_subjects);
        commandMatrix.put(UPDATE_VERB, updateVerb_subjects);


        //List of all possible Verbs
        allVerbs.add(KamanjaShellCommand.CREATE_VERB);
        allVerbs.add(KamanjaShellCommand.PUSH_VERB);
        allVerbs.add(KamanjaShellCommand.CONFIGURE_VERB);
        allVerbs.add(KamanjaShellCommand.SET_VERB);
        allVerbs.add(KamanjaShellCommand.START_VERB);
        allVerbs.add(KamanjaShellCommand.ADD_VERB);
        allVerbs.add(KamanjaShellCommand.REMOVE_VERB);
        allVerbs.add(KamanjaShellCommand.GET_VERB);
        allVerbs.add(KamanjaShellCommand.UPDATE_VERB);

    }

    public static java.util.ArrayList<String> getVerbs() {
        return allVerbs;
    }

    public static  java.util.ArrayList<String> getSubjects(String verb) {
        ArrayList<HashMap<String,ArrayList<String>>> tempVerbs = commandMatrix.get(verb);
        java.util.ArrayList<String> retVals = new java.util.ArrayList<String>();

        java.util.Iterator<HashMap<String,ArrayList<String>>> tempIter = tempVerbs.iterator();
        while(tempIter.hasNext()) {
            HashMap<String,ArrayList<String>> tempMap = tempIter.next();
            java.util.Iterator<String> tempSubjects = tempMap.keySet().iterator();
            while(tempSubjects.hasNext())
                retVals.add(tempSubjects.next());
        }

        return retVals;
    }

    public static ArrayList<String> getOptions(String verb, String subject) {
        ArrayList<HashMap<String,ArrayList<String>>> tempVerbs = commandMatrix.get(verb);
        java.util.ArrayList<String> retVals = new java.util.ArrayList<String>();

        java.util.Iterator<HashMap<String,ArrayList<String>>> tempIter = tempVerbs.iterator();
        while(tempIter.hasNext()) {
            HashMap<String,ArrayList<String>> tempMap = tempIter.next();
            java.util.Iterator<String> tempSubjects = tempMap.keySet().iterator();
            while(tempSubjects.hasNext()) {
                if (tempSubjects.next().equalsIgnoreCase(subject)) {
                    java.util.Iterator<String> options = tempMap.get(subject).iterator();
                    while (options.hasNext()) {
                        retVals.add(options.next());
                    }
                }

            }
        }
        return retVals;
    }
}
