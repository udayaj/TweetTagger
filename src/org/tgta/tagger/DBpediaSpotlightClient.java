package org.tgta.tagger;

/**
 *
 * @author udaya
 */
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.methods.GetMethod;
import org.dbpedia.spotlight.exceptions.AnnotationException;
import org.dbpedia.spotlight.model.DBpediaResource;
import org.dbpedia.spotlight.model.Text;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;



public class DBpediaSpotlightClient extends AnnotationClient {

    private static String API_URL = "";
    private static String REF_URL = "";
    private static double CONFIDENCE = 0.2;
    private static int SUPPORT = 20;
    private static String TYPES = "";

    public DBpediaSpotlightClient(String API_URL, String REF_URL, double CONFIDENCE, int SUPPORT, String TYPES) {
        DBpediaSpotlightClient.API_URL = API_URL;
        DBpediaSpotlightClient.REF_URL = REF_URL;
        DBpediaSpotlightClient.CONFIDENCE = CONFIDENCE;
        DBpediaSpotlightClient.SUPPORT = SUPPORT;
        DBpediaSpotlightClient.TYPES = TYPES;
    }

    @Override
    public List<DBpediaResource> extract(Text text) throws AnnotationException, JSONException {
        String spotlightResponse;
        try {
            GetMethod getMethod = new GetMethod(API_URL + "rest/annotate/?"
                    + "confidence=" + CONFIDENCE
                    + "&support=" + SUPPORT
                    + "&types=" + URLEncoder.encode(TYPES, "utf-8")
                    + "&text=" + URLEncoder.encode(text.text().trim(), "utf-8"));
            getMethod.addRequestHeader(new Header("Accept", "application/json"));

            spotlightResponse = request(getMethod);
        } catch (UnsupportedEncodingException e) {
            throw new AnnotationException("Could not encode text.", e);
        }

        assert spotlightResponse != null;

        JSONObject resultJSON = null;
        JSONArray entities = null;

        try {
            //System.out.print(spotlightResponse);
            resultJSON = new JSONObject(spotlightResponse);
            if (resultJSON.has("Resources")) {
                entities = resultJSON.getJSONArray("Resources");
            }
        } catch (JSONException e) {
            throw new AnnotationException("Received invalid response from DBpedia Spotlight API.");
        }

        if (entities != null) {
            LinkedList<DBpediaResource> resources = new LinkedList<DBpediaResource>();
            Set<String> h = new HashSet<String>();            

            for (int i = 0; i < entities.length(); i++) {
                try {
                    JSONObject entity = entities.getJSONObject(i);
                    //resources.add(new DBpediaResource(entity.getString("@URI"),Integer.parseInt(entity.getString("@support"))));
                    String uri = entity.getString("@URI").replace(REF_URL, ""); 
                    if(!h.contains(uri)){
                        h.add(uri);
                        resources.add(new DBpediaResource(uri,Integer.parseInt(entity.getString("@support"))));
                    }
                    
                } catch (JSONException e) {
                    throw new JSONException("Received invalid response from DBpedia Spotlight API.");
                    //return null;
                }
            }
            return resources;
        } else {
            return null;
        }
    }

    public String ExtractKeywords(String str) throws Exception {

//        DBpediaSpotlightClient c = new DBpediaSpotlightClient ();
//        File input = new File("/home/pablo/eval/manual/AnnotationText.txt");
//        File output = new File("/home/pablo/eval/manual/systems/Spotlight.list");
        //File input = new File("/home/pablo/eval/cucerzan/cucerzan.txt");
        //File output = new File("/home/pablo/eval/cucerzan/systems/cucerzan-Spotlight.set");
//        File input = new File("/home/pablo/eval/wikify/gold/WikifyAllInOne.txt");
//        File output = new File("/home/pablo/eval/wikify/systems/Spotlight.list");
//        File input = new File("/home/udaya/dbpedia-spotlight/info/Berlin.txt");
//        File output = new File("/home/udaya/dbpedia-spotlight/info/Spotlight.list");
        //return c.evaluate(str);
        return null;

//        SpotlightClient c = new SpotlightClient(api_key);
//        List<DBpediaResource> response = c.extract(new Text(text));
//        PrintWriter out = new PrintWriter(manualEvalDir+"AnnotationText-Spotlight.txt.set");
//        System.out.println(response);
    }
}
