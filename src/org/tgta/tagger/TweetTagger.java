package org.tgta.tagger;

import org.apache.commons.daemon.*;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.commons.configuration.*;

import java.net.URLDecoder;
import java.io.*;

public class TweetTagger implements Daemon {

    private static String DB_HOST = "";
    private static String DB_PORT = "";
    private static String DB_USER = "";
    private static String DB_PASSWORD = "";
    private static String DB_NAME = "";
    private static int ROWS_READ = 500;
    private static String API_URL = "http://localhost:2222/";
    private static String REF_URL = "http://localhost:8890/resource/";
    //private final static String API_URL = "http://spotlight.dbpedia.org/";
    private static double CONFIDENCE = 0.2;
    private static int SUPPORT = 20;
    private static final String TYPES = "DBpedia:AnatomicalStructure,DBpedia:ChemicalSubstance,DBpedia:Colour,DBpedia:Database,DBpedia:Disease,DBpedia:Drug,DBpedia:Food,"
            + "DBpedia:Protein,DBpedia:Species,Freebase:/medicine,Freebase:/food,Freebase:/people/professional_field,Freebase:/biology,dbpedia:Quarantine,Freebase:/internet,Freebase:/people/cause_of_death,Freebase:/people,"
            + "DBpedia:TopicalConcept,Freebase:/medicine/disease,Freebase:/medicine/medical_treatment,Freebase:/medicine/symptom,Freebase:/medicine/risk_factor,Freebase:/medicine/disease_cause,DBpedia:Concept,DBpedia:Organisation,Schema:Organization,Freebase:/book/book_subject";
     //,DBpedia:unknown

    private static int CLIENT_NUM = -1;
    private static int NUM_OF_CLIENTS = 1;
    private static long last_id = 0;

    private static Connection db = null;

    protected List<MsgData> list = new ArrayList<MsgData>();
    protected int MAX = 500;
    private static boolean stop_execution = false;

    DBpediaSpotlightClient dpc = null;

    class Producer extends Thread {

        public Producer(String name) {
            super(name);
        }

        public void run() {
            while (true) {
                List<MsgData> list2 = getMessages();

                while (list2.isEmpty()) {
                    try {
                        //System.out.println("Producer - going to sleep ...");
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                        System.err.println("Producer INTERRUPTED - SLEEP");
                    }
                    list2 = getMessages();
                }

                if (stop_execution) {
                    System.out.println("Producer going to stop : Thread: " + this.getName());
                    break;
                }

                synchronized (list) {
                    while (list.size() > (MAX - ROWS_READ)) // queue "full"
                    {
                        try {
                            //System.out.println("Producer WAITING");
                            list.wait();   // Limit the size
                        } catch (InterruptedException ex) {
                            System.err.println("Producer INTERRUPTED");
                        }
                    }

                    list.addAll(list2);
                    last_id = list2.get(list2.size() - 1).key;

                    list.notifyAll();  // must own the lock

                    if (stop_execution) {
                        System.out.println("Producer going to stop : Thread: " + this.getName());
                        break;
                    }

                }
            }
        }

        List<MsgData> getMessages() {  // Data coellection method
            List<MsgData> list2 = new ArrayList<MsgData>();

            try {
                String tweetsQuery = "";
                if (NUM_OF_CLIENTS == 0) {
                    tweetsQuery = "SELECT * FROM public.\"NormalizedMsg\" WHERE ((\"IsProcessed\" IS NULL OR  \"IsProcessed\" = false) AND \"Id\" > " + last_id + ") ORDER BY \"Id\" ASC LIMIT " + ROWS_READ;
                } else {
                    tweetsQuery = "SELECT * FROM public.\"NormalizedMsg\" WHERE (\"IsProcessed\" IS NULL OR  \"IsProcessed\" = false) AND \"Id\"%" + NUM_OF_CLIENTS + "=" + CLIENT_NUM + " AND \"Id\" > " + last_id + " ORDER BY \"Id\" ASC LIMIT " + ROWS_READ;
                }

                Statement st = db.createStatement();
                ResultSet rs = st.executeQuery(tweetsQuery);

                while (rs.next()) {
                    long id = rs.getLong(1);
                    String msg = rs.getString(3);

                    MsgData d = new MsgData(id, msg);
                    list2.add(d);
                }
            } catch (Exception ex) {
                System.err.println("Database Connection failed....");
                System.err.println(ex);
                System.exit(0);
            }

            return list2;
        }
    }

    class Consumer extends Thread {

        public Consumer(String name) {
            super(name);
        }

        public void run() {
            while (true) {
                MsgData d = null;
                synchronized (list) {
                    while (list.size() == 0) {
                        try {
                            //System.out.println("CONSUMER WAITING");
                            list.wait();  // must own the lock
                        } catch (InterruptedException ex) {
                            System.err.println("CONSUMER INTERRUPTED");
                        }
                    }
                    d = list.remove(0);
                    list.notifyAll();

                    if (stop_execution) {
                        System.out.println("Consumer going to stop : Thread: " + this.getName());
                        break;
                    }
                }

                //process
                getAndSaveTags(d);

                if (stop_execution) {
                    System.out.println("Consumer going to stop : Thread: " + this.getName());
                    break;
                }
            }
        }

        void getAndSaveTags(MsgData d) {

            String tags = null;
            try {
                tags = dpc.evaluate(d.msg, d.key);
            } catch (Exception ex) {
                System.err.println("UnreportedException " + ex);
            }
            //System.out.println(d.msg);

            if (tags == null || tags.isEmpty()) {
                addKeyWords(d.key, "", true);
                //System.out.println("<No Tags>");
            } else {
                try {
                    tags = URLDecoder.decode(tags, "UTF-8");
                    //System.out.println(tags);

                    addKeyWords(d.key, tags, false);
                } catch (UnsupportedEncodingException ex) {
                    System.err.println("UnsupportedEncodingException " + ex);
                    System.err.println("Error in NormalizedMsg -> Id: " + d.key);
                    addKeyWords(d.key, tags, true);
                }
            }
        }

        void addKeyWords(long id, String tags, boolean noTags) {

            try {
                String updateQuery = "Update public.\"NormalizedMsg\" SET \"IsProcessed\" = ?, \"KeyWords\" = ?, \"NoTags\" = ? WHERE \"Id\" = ?;";
                PreparedStatement preparedStatement = db.prepareStatement(updateQuery);
                preparedStatement.setBoolean(1, true);//IsProcessed
                preparedStatement.setString(2, tags);//KeyWords
                preparedStatement.setBoolean(3, noTags);//noTags
                preparedStatement.setLong(4, id);//id            
                preparedStatement.executeUpdate();
            } catch (SQLException ex) {
                System.err.println("Error in SQL Query execution ....");
                System.err.println(ex);
                System.exit(0);
            }
        }
    }

//    TweetTagger(int nP, int nC) {
//        dpc = new DBpediaSpotlightClient(API_URL, REF_URL, CONFIDENCE, SUPPORT);
//
//        for (int i = 0; i < nP; i++) {
//            new Producer(Integer.toString(i)).start();
//        }
//        for (int i = 0; i < nC; i++) {
//            new Consumer(Integer.toString(i)).start();
//        }
//    }
    
    public static void main(String args[]) throws Exception{
        TweetTagger tt = new TweetTagger();
        tt.start();
    }
    
    public static void loadParams() throws IOException, InterruptedException {

        System.out.println("TweetTagger started at " + new Date().toString());

        try {
            DefaultConfigurationBuilder builder = new DefaultConfigurationBuilder();
            File f = new File("config/config.xml");
            builder.setFile(f);
            CombinedConfiguration config = builder.getConfiguration(true);

            DB_HOST = config.getString("database.host");
            DB_PORT = config.getString("database.port");
            DB_USER = config.getString("database.user");
            DB_PASSWORD = config.getString("database.password");
            DB_NAME = config.getString("database.name");
            ROWS_READ = config.getInt("tweet-tagger.rows-read", ROWS_READ);
            API_URL = config.getString("tweet-tagger.api-url", "http://localhost:2222/");
            REF_URL = config.getString("tweet-tagger.ref-url", "http://localhost:8890/resource/");
            CONFIDENCE = config.getDouble("tweet-tagger.confidence", 0.2);
            SUPPORT = config.getInt("tweet-tagger.support", 20);
            CLIENT_NUM = config.getInt("tweet-tagger.client-num", -1);
            NUM_OF_CLIENTS = config.getInt("tweet-tagger.num-of-clients", 1);

            System.out.println("DB_HOST: " + DB_HOST);
            System.out.println("DB_PORT: " + DB_PORT);
            System.out.println("DB_USER: " + DB_USER);
            System.out.println("DB_PASSWORD: " + DB_PASSWORD);
            System.out.println("DB_NAME: " + DB_NAME);

            //XMLConfiguration config = new XMLConfiguration("config\\config.xml");
            //System.out.println("consumer key: " + config.getString("database.url"));     
            Class.forName("org.postgresql.Driver");
            String dbUrl = "jdbc:postgresql://" + DB_HOST + ":" + DB_PORT + "/" + DB_NAME;

            db = DriverManager.getConnection(dbUrl, DB_USER, DB_PASSWORD);
            //System.exit(0);
        } catch (org.apache.commons.configuration.ConfigurationException cex) {
            System.err.println("Error occurred while reading configurations....");
            System.err.println(cex);
            System.exit(0);
        } catch (ClassNotFoundException ex) {
            System.err.println("Database Driver not found....");
            System.err.println(ex);
            System.exit(0);
        } catch (SQLException ex) {
            System.err.println("Database Connection failed....");
            System.err.println(ex);
            System.exit(0);
        }
    }

    @Override
    public void init(DaemonContext dc) throws DaemonInitException, Exception {
        System.out.println("initializing ...");
    }

    @Override
    public void start() throws Exception {
        System.out.println("starting ...");
        loadParams();

        // Start producers and consumers
        int numProducers = 1;
        int numConsumers = 30;//30
        //TweetTagger pc = new TweetTagger(numProducers, numConsumers);

        dpc = new DBpediaSpotlightClient(API_URL, REF_URL, CONFIDENCE, SUPPORT, TYPES);

        for (int i = 0; i < numProducers; i++) {
            new Producer("pro" + Integer.toString(i)).start();
        }
        for (int i = 0; i < numConsumers; i++) {
            new Consumer(Integer.toString(i)).start();
        }

        System.out.println("Threads started !");

//        while (!stop_execution) {
//             System.out.println("In the wait while loop!");
//            //Sleep for 1 minute
//            Thread.sleep(60 * 60 * 1000);
//        }
//
//        System.out.println("Client No. " + CLIENT_NUM + " will be stopped in 1 minute.");
//        Thread.sleep(60 * 60 * 1000);
    }

    @Override
    public void stop() throws Exception {
        System.out.println("stopping ...");

        stop_execution = true;
        synchronized (list) {
            list.notifyAll();
            System.out.println("Client No. " + CLIENT_NUM + " was stopped.");
        }
    }

    @Override
    public void destroy() {
        System.out.println("Stopped !");
    }

}
