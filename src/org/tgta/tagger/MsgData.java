/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.tgta.tagger;

import opennlp.tools.formats.ContractionUtility;

/**
 *
 * @author udaya
 */
public class MsgData {
    public long key;
    public String msg;   

    public MsgData(long key, String msg) {
        this.key = key;
        this.msg = msg;
    }    
}
