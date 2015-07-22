package com.ligadata.kamanja.copd;

import com.ligadata.FatafatBase.*;
import com.ligadata.messagescontainers.System.*;
import org.joda.time.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by will on 7/1/15.
 */

public class COPDRiskAssessment extends ModelBase {
    // Messages
    private Beneficiary msg = null;
    private JavaRDD<InpatientClaim> inpatientClaimHistoryRDD = null;
    private JavaRDD<OutpatientClaim> outpatientClaimHistoryRDD = null;
    private JavaRDD<HL7> hl7HistoryRDD = null;

    // Containers
    private JavaRDD<CoughCodes> coughCodesRDD = null;
    private JavaRDD<DyspnoeaCodes> dyspnoeaCodesRDD = null;
    private JavaRDD<EnvCodes> envCodesRDD = null;
    private JavaRDD<SmokeCodes> smokeCodesRDD = null;
    private JavaRDD<SputumCodes> sputumCodesRDD = null;

    // Filtered Message Arrays
    private ArrayList<InpatientClaim> inpatientClaimHistory = new ArrayList<>();
    private ArrayList<OutpatientClaim> outpatientClaimHistory = new ArrayList<>();
    private ArrayList<HL7> hl7History = new ArrayList<>();

    // Lookup Arrays
    private ArrayList<String> coughCodes = new ArrayList<>();
    private ArrayList<String> dyspnoeaCodes = new ArrayList<>();
    private ArrayList<String> envCodes = new ArrayList<>();
    private ArrayList<String> smokeCodes = new ArrayList<>();
    private ArrayList<String> sputumCodes = new ArrayList<>();

    private String[] partitionKeys = null;

    private ModelContext mdlContext = null;

    private SimpleDateFormat yearMonthDayHourFormat = new SimpleDateFormat("yyyyMMdd");
    private Integer today = null;
    private Integer oneYearAgo = null;
    private TimeRange lookupPeriod = null;

    private void init() {
        msg = (Beneficiary) this.modelContext().msg();
        System.out.println("Executing COPD Risk Assessment against Beneficiary message:");
        System.out.println("\tMessage Name: " + msg.Name());
        System.out.println("\tMessage Version: " + msg.Version());
        System.out.println("\tMessage Desynpuf ID: " + msg.desynpuf_id());

        partitionKeys = msg.PartitionKeyData();

        lookupPeriod = new TimeRange(getOneYearAgo(), getToday());

        // Getting message RDD objects
        inpatientClaimHistoryRDD = InpatientClaimFactory.rddObject.getRDD(partitionKeys, getLookupPeriod());
        outpatientClaimHistoryRDD = OutpatientClaimFactory.rddObject.getRDD(partitionKeys, getLookupPeriod());
        hl7HistoryRDD = HL7Factory.rddObject.getRDD(partitionKeys, getLookupPeriod());

        // Filter claims down to the last year based on clm_thru_dt
        for (Iterator<InpatientClaim> ipClaimIt = inpatientClaimHistoryRDD.iterator(); ipClaimIt.hasNext(); ) {
            InpatientClaim claim = ipClaimIt.next();
            if (isDateBetween(claim.clm_thru_dt())) {
                inpatientClaimHistory.add(claim);
            }
        }

        for (Iterator<OutpatientClaim> opClaimIt = outpatientClaimHistoryRDD.iterator(); opClaimIt.hasNext(); ) {
            OutpatientClaim claim = opClaimIt.next();
            if (isDateBetween(claim.clm_thru_dt())) {
                outpatientClaimHistory.add(claim);
            }
        }

        for (Iterator<HL7> hl7Iterator = hl7HistoryRDD.iterator(); hl7Iterator.hasNext(); ) {
            HL7 claim = hl7Iterator.next();
            if (isDateBetween(claim.clm_thru_dt())) {
                hl7History.add(claim);
            }
        }

        // Getting lookup tables
        coughCodesRDD = CoughCodesFactory.rddObject.getRDD();
        dyspnoeaCodesRDD = DyspnoeaCodesFactory.rddObject.getRDD();
        envCodesRDD = EnvCodesFactory.rddObject.getRDD();
        smokeCodesRDD = SmokeCodesFactory.rddObject.getRDD();
        sputumCodesRDD = SputumCodesFactory.rddObject.getRDD();

        for (Iterator<CoughCodes> coughCodeIt = coughCodesRDD.iterator(); coughCodeIt.hasNext(); ) {
            coughCodes.add(coughCodeIt.next().icd9code());
        }
        System.out.print("Cough Codes:\t");
        for (String code : coughCodes) {
            System.out.print(code + ", ");
        }

        for (Iterator<DyspnoeaCodes> dyspCodeIt = dyspnoeaCodesRDD.iterator(); dyspCodeIt.hasNext(); ) {
            dyspnoeaCodes.add(dyspCodeIt.next().icd9code());
        }
        System.out.print("\nDyspnea Codes:\t");
        for (String code : dyspnoeaCodes) {
            System.out.print(code + ", ");
        }

        for (Iterator<EnvCodes> envCodeIt = envCodesRDD.iterator(); envCodeIt.hasNext(); ) {
            envCodes.add(envCodeIt.next().icd9code());
        }
        System.out.print("\nEnv Codes:\t");
        for (String code : envCodes) {
            System.out.print(code + ", ");
        }

        for (Iterator<SmokeCodes> smokeCodeIt = smokeCodesRDD.iterator(); smokeCodeIt.hasNext(); ) {
            smokeCodes.add(smokeCodeIt.next().icd9code());
        }
        System.out.print("\nSmoke Codes:\t");
        for (String code : smokeCodes) {
            System.out.print(code + ", ");
        }

        for (Iterator<SputumCodes> sputumCodeIt = sputumCodesRDD.iterator(); sputumCodeIt.hasNext(); ) {
            sputumCodes.add(sputumCodeIt.next().icd9code());
        }

        System.out.print("\nSputum Codes:\t");
        for (String code : sputumCodes) {
            System.out.println(code + ", ");
        }
        System.out.println();
    }

    private Integer getToday() {
        if (today == null) {
            Calendar time = Calendar.getInstance();
            today = Integer.parseInt(yearMonthDayHourFormat.format(time.getTime()));
        }
        return today;
    }

    private Date getTodayAsDate() {
        return Calendar.getInstance().getTime();
    }

    private Date getOneYearAgoAsDate() {
        Calendar time = Calendar.getInstance();
        time.add(Calendar.YEAR, -1);
        return time.getTime();
    }

    private Date intToDate(Integer date) {
        try {
            return yearMonthDayHourFormat.parse(date.toString());
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

    private boolean isDateBetween(Integer date) {
        Date tDate = intToDate(date);
        return ((tDate.before(getTodayAsDate()) || tDate.equals(getTodayAsDate())) && (tDate.after(getOneYearAgoAsDate()) || tDate.equals(getOneYearAgoAsDate())));
    }

    private Integer getOneYearAgo() {
        if (oneYearAgo == null) {
            Calendar time = Calendar.getInstance();
            time.add(Calendar.YEAR, -1);
            oneYearAgo = Integer.parseInt(yearMonthDayHourFormat.format(time.getTime()));
        }
        return oneYearAgo;
    }

    private TimeRange getLookupPeriod() {
        if (lookupPeriod == null) {
            lookupPeriod = new TimeRange(getOneYearAgo(), getToday());
        }
        return lookupPeriod;
    }

    static COPDRiskAssessmentObj objSingleton = new COPDRiskAssessmentObj();

    private Boolean age40OrOlder() {
        org.joda.time.LocalDate birthdate = new org.joda.time.LocalDate(msg.bene_birth_dt() / 10000, (msg.bene_birth_dt() % 1000) / 100, msg.bene_birth_dt() % 100);
        Integer age = Years.yearsBetween(birthdate, new LocalDate()).getYears();
        if (age > 40) {
            return true;
        }
        return false;
    }

    private Boolean hasSmokingHistory() {
        for (InpatientClaim ic : inpatientClaimHistory) {
            if (smokeCodes.contains(ic.admtng_icd9_dgns_cd()))
                return true;

            for (String code : ic.icd9_dgns_cds()) {
                if (smokeCodes.contains(code))
                    return true;
            }
        }

        for (OutpatientClaim oc : outpatientClaimHistory) {
            for(String code: oc.icd9_dgns_cds()){
                System.out.println("\t" + code);
            }
            if (smokeCodes.contains(oc.admtng_icd9_dgns_cd())) {
                return true;
            }

            for (String code : oc.icd9_dgns_cds()) {
                if (smokeCodes.contains(code)) {
                    return true;
                }
            }
        }
        return false;
    }

    private Boolean hasEnvironmentalExposure() {
        for (InpatientClaim ic : inpatientClaimHistory) {
            if (envCodes.contains(ic.admtng_icd9_dgns_cd())) {
                return true;
            }

            for (String code : ic.icd9_dgns_cds()) {
                if (envCodes.contains(code)) {
                    return true;
                }
            }
        }

        for (OutpatientClaim oc : outpatientClaimHistory) {
            if (envCodes.contains(oc.admtng_icd9_dgns_cd())) {
                return true;
            }

            for (String code : oc.icd9_dgns_cds()) {
                if (envCodes.contains(code)) {
                    return true;
                }
            }
        }
        return false;
    }

    private Boolean hasDyspnea() {
        for (InpatientClaim ic : inpatientClaimHistory) {
            if (dyspnoeaCodes.contains(ic.admtng_icd9_dgns_cd())) {
                return true;
            }

            for (String code : ic.icd9_dgns_cds()) {
                if (dyspnoeaCodes.contains(code)) {
                    return true;
                }
            }
        }

        for (OutpatientClaim oc : outpatientClaimHistory) {
            if (dyspnoeaCodes.contains(oc.admtng_icd9_dgns_cd())) {
                return true;
            }

            for (String code : oc.icd9_dgns_cds()) {
                if (dyspnoeaCodes.contains(code)) {
                    return true;
                }
            }
        }
        return false;
    }

    private Boolean hasChronicCough() {
        for (InpatientClaim ic : inpatientClaimHistory) {
            if (coughCodes.contains(ic.admtng_icd9_dgns_cd())) {
                return true;
            }

            for (String code : ic.icd9_dgns_cds()) {
                if (coughCodes.contains(code)) {
                    return true;
                }
            }
        }

        for (OutpatientClaim oc : outpatientClaimHistory) {
            if (coughCodes.contains(oc.admtng_icd9_dgns_cd())) {
                return true;
            }

            for (String code : oc.icd9_dgns_cds()) {
                if (coughCodes.contains(code)) {
                    return true;
                }
            }
        }
        return false;
    }

    private Boolean hasChronicSputum() {
        for (InpatientClaim ic : inpatientClaimHistory) {
            if (sputumCodes.contains(ic.admtng_icd9_dgns_cd())) {
                return true;
            }

            for (String code : ic.icd9_dgns_cds()) {
                if (sputumCodes.contains(code)) {
                    return true;
                }
            }
        }

        for (OutpatientClaim oc : outpatientClaimHistory) {
            if (sputumCodes.contains(oc.admtng_icd9_dgns_cd())) {
                return true;
            }

            for (String code : oc.icd9_dgns_cds()) {
                if (sputumCodes.contains(code)) {
                    return true;
                }
            }
        }
        return false;
    }

    private Boolean hasAATDeficiency() {
        for (HL7 hl7 : hl7History) {
            if (hl7.aatdeficiency() == 1) {
                return true;
            }
        }
        return false;
    }

    private Boolean hasFamilyHistory() {
        if (msg.sp_copd() == 1) {
            return true;
        }

        for (HL7 hl7 : hl7History) {
            if (hl7.chroniccough() > 0 || hl7.sp_copd() > 0 || hl7.shortnessofbreath() > 0 || hl7.chronicsputum() > 0) {
                return true;
            }
        }
        return false;
    }

    private double inpatientClaimCosts() {
        double totalCost = 0;

        for (InpatientClaim claim : inpatientClaimHistory) {
            totalCost += claim.clm_pmt_amt() + claim.nch_prmry_pyr_clm_pd_amt() + claim.clm_pass_thru_per_diem_amt() +
                    claim.nch_bene_ip_ddctbl_amt() + claim.nch_bene_pta_coinsrnc_lblty_am() + claim.nch_bene_blood_ddctbl_lblty_am();
        }
        return totalCost;
    }

    private double outpatientClaimCosts() {
        double totalCost = 0d;
        for (OutpatientClaim claim : outpatientClaimHistory) {
            totalCost += claim.clm_pmt_amt() + claim.nch_prmry_pyr_clm_pd_amt() + claim.nch_bene_blood_ddctbl_lblty_am() +
                    claim.nch_bene_ptb_ddctbl_amt() + claim.nch_bene_ptb_coinsrnc_amt();
        }
        return totalCost;
    }


    private MappedModelResults copdRiskLevel() {
        Boolean hasSmokingHistory = hasSmokingHistory();
        Boolean hasEnvironmentalExposure = hasEnvironmentalExposure();
        Boolean hasDyspnea = hasDyspnea();
        Boolean hasChronicCough = hasChronicCough();
        Boolean hasChronicSputum = hasChronicSputum();
        Boolean hasAATDeficiency = hasAATDeficiency();
        Boolean hasFamilyHistory = hasFamilyHistory();
        Boolean ageOver40 = age40OrOlder();

        Boolean hasCOPDSymptoms = hasDyspnea || hasChronicCough || hasChronicSputum;

        String riskLevel = "";

        if (ageOver40 && hasSmokingHistory && hasAATDeficiency && hasEnvironmentalExposure && hasCOPDSymptoms) {
            riskLevel = "1b";
        } else if (ageOver40 && hasSmokingHistory && (hasAATDeficiency || hasEnvironmentalExposure || hasCOPDSymptoms)) {
            riskLevel = "1a";
        } else if (!ageOver40 && (hasCOPDSymptoms || hasAATDeficiency || hasFamilyHistory)) {
            riskLevel = "2";
        }

        Result[] results = new Result[]{
                new Result("Desynpuf ID", msg.desynpuf_id()),
                new Result("COPD Risk Level", riskLevel),
                new Result("Is Over 40 Years Old", ageOver40),
                new Result("Has Smoking History", hasSmokingHistory),
                new Result("Has Environmental Exposure", hasEnvironmentalExposure),
                new Result("Has Dyspnea", hasDyspnea),
                new Result("Has Chronic Cough", hasChronicCough),
                new Result("Has Chronic Sputum", hasChronicSputum),
                new Result("Has AAT Deficiency", hasAATDeficiency),
                new Result("Inpatient Claim Costs", inpatientClaimCosts()),
                new Result("Outpatient Claim Costs", outpatientClaimCosts())
        };

        for (Result result : results) {
            System.out.println(result.name() + ": " + result.result());
        }
        System.out.println("****************************************************");

        return ((MappedModelResults) new COPDRiskAssessmentObj().CreateResultObject()).withResults(results);
    }

    @Override
    public MappedModelResults execute(boolean emitAllResults) {
        MappedModelResults result = copdRiskLevel();
        if(!emitAllResults) {
            if (result.get("COPD Risk Level") == "") {
                return null;
            }
        }
        return result;
    }

    public COPDRiskAssessment(ModelContext mdlContext) {
        super(mdlContext, objSingleton);
        this.mdlContext = mdlContext;
        init();
    }

    @Override
    public ModelContext modelContext() {
        return mdlContext;
    }

    @Override
    public ModelBaseObj factory() {
        return objSingleton;
    }

    public static class COPDRiskAssessmentObj implements ModelBaseObj {
        public boolean IsValidMessage(MessageContainerBase msg) {
            return (msg instanceof Beneficiary);
        }

        public ModelBase CreateNewModel(ModelContext mdlContext) {
            return new COPDRiskAssessment(mdlContext);
        }

        public String ModelName() {
            return "COPDRiskAssessment";
        }

        public String Version() {
            return "0.0.2";
        }

        public ModelResultBase CreateResultObject() {
            return new MappedModelResults();
        }

    }
}