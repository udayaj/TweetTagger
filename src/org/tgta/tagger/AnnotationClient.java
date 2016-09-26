/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.tgta.tagger;

/**
 *
 * @author udaya
 */
import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.params.HttpMethodParams;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;
import org.dbpedia.spotlight.exceptions.AnnotationException;
import org.dbpedia.spotlight.model.DBpediaResource;
import org.dbpedia.spotlight.model.Text;

import java.io.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONException;

/**
 * This class has been translate to scala. Please use the AnnotationClientScala.scala for new External Clients!
 * (AnnotationClientScala.scala is at eval/src/main/scala/org/dbpedia/spotlight/evaluation/external/)
 *
 * @author pablomendes
 */

public abstract class AnnotationClient {

    public Logger LOG = Logger.getLogger(this.getClass());
    
    // Create an instance of HttpClient.
    private static HttpClient client = new HttpClient(new MultiThreadedHttpConnectionManager());


    public String request(HttpMethod method) throws AnnotationException {

        String response = null;

        // Provide custom retry handler is necessary
        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
                new DefaultHttpMethodRetryHandler(3, false));

        try {
            // Execute the method.
            int statusCode = client.executeMethod(method);

            if (statusCode != HttpStatus.SC_OK) {
                LOG.error("Method failed: " + method.getStatusLine());
            }

            // Read the response body.
            //byte[] responseBody;
            InputStream in = method.getResponseBodyAsStream(); 
            
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            StringBuilder out = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                out.append(line);
            }
            //System.out.println(out.toString());   //Prints the string content read from input stream
            reader.close();
            
            response = out.toString();
            
            //TODO Going to buffer response body of large or unknown size. 
            //Using getResponseBodyAsStream instead is recommended.

            // Deal with the response.
            // Use caution: ensure correct character encoding and is not binary data
            //response = new String(responseBody);

        } catch (HttpException e) {
            LOG.error("Fatal protocol violation: " + e.getMessage());
            throw new AnnotationException("Protocol error executing HTTP request.",e);
        } catch (IOException e) {
            LOG.error("Fatal transport error: " + e.getMessage());
            LOG.error(method.getQueryString());
            throw new AnnotationException("Transport error executing HTTP request.",e);
        } finally {
            // Release the connection.
            method.releaseConnection();
        }
        return response;

    }

    protected static String readFileAsString(String filePath) throws java.io.IOException{
        return readFileAsString(new File(filePath));
    }
    
    protected static String readFileAsString(File file) throws IOException {
        byte[] buffer = new byte[(int) file.length()];
        BufferedInputStream f = new BufferedInputStream(new FileInputStream(file));
        f.read(buffer);
        return new String(buffer);
    }

    static abstract class LineParser {

        public abstract String parse(String s) throws ParseException;

        static class ManualDatasetLineParser extends LineParser {
            public String parse(String s) throws ParseException {
                return s.trim();
            }
        }

        static class OccTSVLineParser extends LineParser {
            public String parse(String s) throws ParseException {
                String result = s;
                try {
                    result = s.trim().split("\t")[3];
                } catch (ArrayIndexOutOfBoundsException e) {
                    throw new ParseException(e.getMessage(), 3);
                }
                return result; 
            }
        }
    }

    public String saveExtractedEntitiesSet(String str, long msgId, LineParser parser, int restartFrom) throws Exception {
        String outString = "";
        
        String text = str;
        int i=0;
        //int correct =0 ;
        //int error = 0;
        //int sum = 0;
        for (String snippet: text.split("\n")) {
            String s = parser.parse(snippet);
            if (s!= null && !s.equals("")) {
                i++;

                if (i<restartFrom) continue;

                List<DBpediaResource> entities = new ArrayList<DBpediaResource>();
                try {
                    //final long startTime = System.nanoTime();
                    entities = extract(new Text(snippet.replaceAll("\\s+"," ")));
                    if(entities == null){
                        return "";
                    }
                    //final long endTime = System.nanoTime();
                    //sum += endTime - startTime;
                    //LOG.info(String.format("(%s) Extraction ran in %s ns.", i, endTime - startTime));
                    //correct++;
                } catch (AnnotationException e) {
                    //error++;
                    LOG.error("MsgId: " + msgId);
                    LOG.error(e);
                    e.printStackTrace();
                    LOG.error("\n\n");
                    return "";
                }catch (JSONException e) {
                    //error++;
                    LOG.error("MsgId: " + msgId);
                    LOG.error(e);
                    e.printStackTrace();
                    LOG.error("\n\n");
                    return "";
                }
                for (DBpediaResource e: entities) {
                    if(!outString.isEmpty()){
                        outString = outString + " " + e.uri();
                        //System.err.println(e.support());
                    }else{
                        outString = e.uri();
                    }
                }                
            }
        }
        
        //LOG.info(String.format("Extracted entities from %s text items, with %s successes and %s errors.", i, correct, error));        
        //double avg = (new Double(sum) / i);
        //LOG.info(String.format("Average extraction time: %s ms", avg * 1000000));
        
        return outString;
    }

    //by Udaya
    /*
    public void evaluate(File inputFile, File outputFile) throws Exception {
        evaluateManual(inputFile,outputFile,0);
    }

    public void evaluateManual(File inputFile, File outputFile, int restartFrom) throws Exception {
         saveExtractedEntitiesSet(inputFile, outputFile, new LineParser.ManualDatasetLineParser(), restartFrom);
    }
    */

    public String evaluate(String str, long msgId) throws Exception {
      return evaluateManual(str, msgId, 0);
    }

    public String evaluateManual(String str, long msgId, int restartFrom) throws Exception {
       return saveExtractedEntitiesSet(str, msgId, new LineParser.ManualDatasetLineParser(), restartFrom);
    }
    
    
    
//    public void evaluateCurcerzan(File inputFile, File outputFile) throws Exception {
//         saveExtractedEntitiesSet(inputFile, outputFile, new LineParser.OccTSVLineParser());
//    }

    /**
     * Entity extraction code.
     * @param text
     * @return
     */
    public abstract List<DBpediaResource> extract(Text text) throws AnnotationException, JSONException;
}
