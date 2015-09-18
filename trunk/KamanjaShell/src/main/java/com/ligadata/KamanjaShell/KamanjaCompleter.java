package com.ligadata.KamanjaShell;

import jline.*;
import java.util.HashMap;


/**
 * Created by danielkozin on 9/16/15.
 */
public class KamanjaCompleter implements Completor {

    private final java.util.HashMap<String,java.util.ArrayList> myMap = new java.util.HashMap<String,java.util.ArrayList>();
    private String prefix = "";
    private String options_prefix = "";
    private HashMap<String,String> optionCache = new HashMap<String,String>();

   // private final java.util.ArrayList<String> verbs = new java.util.ArrayList<String>();
   // private final java.util.ArrayList<String> subjects = new java.util.ArrayList<String>();
    private final java.util.HashMap<String,java.util.HashMap<String, java.util.ArrayList<String>>> verbs = new java.util.HashMap<String,java.util.HashMap<String, java.util.ArrayList<String>>>();

    // Create lists for 'CREATE"


    public KamanjaCompleter() {

    }

    public void commandCompleted() {
        optionCache.clear();
    }

    public int complete(String buffer, int cursor, java.util.List candidates) {
        String verb;
        String subject;
        prefix = "";
        boolean isProcessingOptions = false;

        java.util.ArrayList<String> tokens =  trimTokens(buffer.split(" +"));
        boolean returnAll = false;

        // If buffer is empty, or only 1 token return the list of verbs
        java.util.ArrayList<String> allPossibilites = null;

        // first if is for the VERBS.
        if (tokens.size() <= 1 && !buffer.endsWith(" ")) {
            if(tokens.size() == 0) {
                returnAll = true;
            }
            allPossibilites = KamanjaShellCommand.getVerbs();
            optionCache.clear();
        }
        // We are on teh subject????
        else if ((tokens.size() == 2  && (!buffer.endsWith(" "))) ||
                 (tokens.size() == 1 && buffer.endsWith(" "))) {
            if(tokens.size() == 1) {
                returnAll = true;
            }
            allPossibilites = KamanjaShellCommand.getSubjects(tokens.get(0));
            prefix = prefix + tokens.get(0) + " ";
            optionCache.clear();
        }
        // Dealing with Options
        else if(tokens.size() > 2 ||
                (tokens.size() == 2 && buffer.endsWith(" "))) {
            isProcessingOptions = true;

            if(buffer.endsWith(" ")) {
                returnAll = true;
            }
            allPossibilites = KamanjaShellCommand.getOptions(tokens.get(0), tokens.get(1));
            prefix = prefix + tokens.get(0) + " " + tokens.get(1);

            int indx = 2;
            while (indx < (tokens.size())) {
                indx++;
                String tempOption = tokens.get(indx-1);
                java.util.ArrayList<String> tempOptionsTokens = trimTokens(tempOption.split(":"));
                if (tempOptionsTokens.size() == 2) {
                    // full option specified.. Add to cache
                    optionCache.put(tempOptionsTokens.get(0),tempOptionsTokens.get(1));
                }

            }
        }




        java.util.Iterator<String> vals = allPossibilites.iterator();
        while (vals.hasNext()) {

            String current= vals.next();

            if (returnAll == true) {
                if (isProcessingOptions) {
                   // options_prefix = options_prefix + " " + current;
                    prefix = prefix +  makeOptionString();
                }
                candidates.add(prefix+ " " +current);
            } else {
                if (current.startsWith(tokens.get(tokens.size() - 1).trim())) {
                    if (isProcessingOptions) {
                      //  options_prefix = options_prefix + " " + current;
                        prefix = prefix  +  makeOptionString();
                    }
                    candidates.add(prefix + " " + current);
                }
            }
        }

        return 0;


        // TODO:  Figure out the path helper logic later

    }

    // Remove all the zero length tokens that might have been produced by user typing multiple spaces in a row.
    private java.util.ArrayList<String> trimTokens(String[] tokens) {
        java.util.ArrayList<String> trimmedList = new java.util.ArrayList<String>();
        for(int i = 0; i < tokens.length; i++) {
            if (tokens[i].length() > 0) {
                trimmedList.add(tokens[i]);
            }
        }
        return trimmedList;
    }

    private String makeOptionString() {
        String retval = "";
        java.util.Iterator<String> keys = optionCache.keySet().iterator();
        while(keys.hasNext()) {
            String key = keys.next();
            retval = retval + " " + key + ":" + optionCache.get(key);
        }
        return retval;
    }
}
