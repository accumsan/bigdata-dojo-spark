package com.minhdd.bigdata.dojo.spark;

import java.io.Serializable;
import java.time.LocalDate;

public class Validation implements Serializable {
    public LocalDate jour;
    public String codeStifTrns;

    public LocalDate getJour() {
        return jour;
    }

    public String getCodeStifTrns() {
        return codeStifTrns;
    }

    public String getCodeStifRes() {
        return codeStifRes;
    }

    public String getCodeStifArret() {
        return codeStifArret;
    }

    public String getLibelleArret() {
        return libelleArret;
    }

    public String getIdRefaLda() {
        return idRefaLda;
    }

    public String getTitre() {
        return titre;
    }

    public Long getValidations() {
        return validations;
    }

    public String codeStifRes;
    public String codeStifArret;
    public String libelleArret;
    public String idRefaLda;
    public String titre;
    public Long validations;


    public Validation(String validation) {
        String split[] = validation.split(";");
        this.jour = LocalDate.parse(split[0]);
        this.codeStifTrns = split[1];
        this.codeStifRes = split[2];
        this.codeStifArret = split[3];
        this.libelleArret = split[4];
        this.idRefaLda = split[5];
        this.titre = split[6];
        this.validations = split.length == 8 ? Long.valueOf(split[7]) : 0;
    }
}