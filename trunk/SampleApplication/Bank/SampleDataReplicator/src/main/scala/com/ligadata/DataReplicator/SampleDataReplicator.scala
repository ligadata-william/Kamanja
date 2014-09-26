package com.ligadata.DataReplicator

import scala.util.control.Breaks._
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.FileInputStream
import java.util.zip.GZIPInputStream
import java.io.OutputStream
import java.io.FileOutputStream
import java.util.zip.GZIPOutputStream
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object SampleDataReplicator {

  def GetCurDtTmStr: String = {
    new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date(System.currentTimeMillis))
  }

  type OptionMap = Map[Symbol, Any]

  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s(0) == '-')
    list match {
      case Nil => map
      case "--sampleinput" :: value :: tail =>
        nextOption(map ++ Map('sampleinput -> value), tail)
      case "--outputdir" :: value :: tail =>
        nextOption(map ++ Map('outputdir -> value), tail)
      case "--outputrows" :: value :: tail =>
        nextOption(map ++ Map('outputrows -> value), tail)
      case "--startaccountnumber" :: value :: tail =>
        nextOption(map ++ Map('startaccountnumber -> value), tail)
      case option :: tail => {
        println(GetCurDtTmStr + " Unknown option " + option)
        sys.exit(1)
      }
    }
  }

  def isCompressed(inputeventfile: String): Boolean = {
    var is: FileInputStream = null
    try {
      is = new FileInputStream(inputeventfile)
    } catch {
      case e: Exception =>
        println(GetCurDtTmStr + " Failed to open Input File %s. Message:%s".format(inputeventfile, e.getMessage))
        throw e
        return false
    }

    val maxlen = 2
    val buffer = new Array[Byte](maxlen)
    val readlen = is.read(buffer, 0, maxlen)

    is.close // Close before we really check and return the data

    if (readlen < 2)
      return false;

    val b0: Int = buffer(0)
    val b1: Int = buffer(1)

    val head = (b0 & 0xff) | ((b1 << 8) & 0xff00)

    return (head == GZIPInputStream.GZIP_MAGIC);
  }

  def MakeNDigits(randVal: Int, nDigits: Int): String = {
    if (nDigits <= 0) return ""
    val str = randVal.toString
    if (str.size == nDigits) return str
    if (str.size > nDigits) return str.substring(0, nDigits)
    return (str + "0000000000000000000000000000000000000000000000000000000".substring(0, nDigits - str.size)) // Padding with 0s 
  }

  def main(args: Array[String]): Unit = {
    val options = nextOption(Map(), args.toList)
    val inputeventfile = options.getOrElse('sampleinput, "").toString.replace("\"", "").trim
    if (inputeventfile.size == 0) {
      println(GetCurDtTmStr + " Need sample input events file")
      sys.exit(1)
    }

    val outputdir = options.getOrElse('outputdir, "").toString.replace("\"", "").trim
    if (outputdir.size == 0) {
      println(GetCurDtTmStr + " Need outputdir")
      sys.exit(1)
    }

    val outputrows = options.getOrElse('outputrows, "").toString.replace("\"", "").trim.toLong
    if (outputrows <= 0) {
      println(GetCurDtTmStr + " Need positive outputrows")
      sys.exit(1)
    }

    val startaccountnumber = options.getOrElse('startaccountnumber, "").toString.replace("\"", "").trim.toLong
    if (startaccountnumber <= 0) {
      println(GetCurDtTmStr + " Need positive startaccountnumber")
      sys.exit(1)
    }

    val custPreferences = outputdir + "/CustPreferences.csv";
    val alertHistory = outputdir + "/AlertHistory.csv";
    val factData = outputdir + "/FactData.csv.gz";

    // AlertHistory format
    // CUSTID,ENT_ACC_NUM,LB001Sent,OD001Sent,OD002Sent,OD003Sent,NO001Sent,EB001Sent,EB002Sent,UTFSent,PTFSent,EventDate
    // Ex: 1686535849,12106697,0,0,0,0,0,0,0,0,0,0
    val alertHistHdr = "CUSTID,ENT_ACC_NUM,LB001Sent,OD001Sent,OD002Sent,OD003Sent,NO001Sent,EB001Sent,EB002Sent,UTFSent,PTFSent,EventDate\n"
    val alertHistFmt = "%d,%d,0,0,0,0,0,0,0,0,0,0\n"

    // CustPreferences format
    // CUST_ID,SORT_CODE,ENT_ACC_NUM,ACCT_SHORT_NM,RISK_TIER_ID,LB_REG_FLG,LB_LIMIT,OD_REG_FLG,MAX_EB_CNT,LAST_UPDATE_TS,CONT_ID,MOBILE_NUMBER,DECEASED_MARKER,OD_T2_LIMIT,OD_T1_LIMIT,NO_FACTOR
    // Ex: 1686535849,0,12106697,Andrevena,TS1,0,10,0,0,0,0,+36-200-300-4000,0,0,0,80
    // Format: <CustId>,0,<AccNm>,<AccShortNm>,{TS0|TS1|TS2},0,<RandomInt(5000)>,0,0,0,0,<MobileNum>,0,0,0,[80-90]
    // From sample collect Sets RISK_TIER_ID, ACCT_SHORT_NM
    // Generated Fields are CUST_ID, ENT_ACC_NUM & MOBILE_NUMBER
    val custPrefHdr = "CUST_ID,SORT_CODE,ENT_ACC_NUM,ACCT_SHORT_NM,RISK_TIER_ID,LB_REG_FLG,LB_LIMIT,OD_REG_FLG,MAX_EB_CNT,LAST_UPDATE_TS,CONT_ID,MOBILE_NUMBER,DECEASED_MARKER,OD_T2_LIMIT,OD_T1_LIMIT,NO_FACTOR\n"
    val custPrefFmt = "%d,0,%d,%s,%s,0,%d,0,0,0,0,%s,0,0,0,%d\n"

    // Fact format
    // CUST_ID,ENT_ACCT_NUM,ENT_SEG_TYP,ENT_DTE,ENT_TME,ENT_SRC,ENT_AMT_TYP,ENT_AMT,ODR_LMT,ANT_LMT,UNP_BUFF,EB_BUFFER,OD_BUFFER,LB_LIMIT,RUN_LDG_XAU,Alert
    // Ex: 810138187,13351747,FD,14001,0,SOI,4,-10,-3000,0,0,0,0,100,110,
    // Ex: 810138187,13351747,FD,14001,6300000,SOI,3,-90,-3000,0,0,0,0,100,20,
    // Just replace CUST_ID,ENT_ACCT_NUM, rest of the fields take from input. No changes at all
    val factHdr = "CUST_ID,ENT_ACCT_NUM,ENT_SEG_TYP,ENT_DTE,ENT_TME,ENT_SRC,ENT_AMT_TYP,ENT_AMT,ODR_LMT,ANT_LMT,UNP_BUFF,EB_BUFFER,OD_BUFFER,LB_LIMIT,RUN_LDG_XAU,Alert\n"

    var br: BufferedReader = null
    println(GetCurDtTmStr + " Opening Input File %s.".format(inputeventfile))
    // Open Input file

    try {
      if (isCompressed(inputeventfile))
        br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(inputeventfile))))
      else
        br = new BufferedReader(new InputStreamReader(new FileInputStream(inputeventfile)))
    } catch {
      case e: Exception =>
        println(GetCurDtTmStr + " Failed to open Input File %s. Message:%s".format(inputeventfile, e.getMessage))
        throw e
        return
    }

    println(GetCurDtTmStr + " Opening Output Files %s, %s & %s.".format(alertHistory, custPreferences, factData))
    // Open Output files
    var osAlertHist: OutputStream = null
    var osCustPref: OutputStream = null
    var osFacts: OutputStream = null

    try {
      osAlertHist = new FileOutputStream(alertHistory)
      osCustPref = new FileOutputStream(custPreferences)
      osFacts = new GZIPOutputStream(new FileOutputStream(factData))
    } catch {
      case e: Exception => {
        println(GetCurDtTmStr + " Failed to open files. Message:" + e.getMessage)
        if (osAlertHist != null)
          osAlertHist.close
        if (osCustPref != null)
          osCustPref.close
        if (osFacts != null)
          osFacts.close
        br.close
        throw e
        return
      }
    }

    val inputTemplateRows = new ArrayBuffer[String]

    println(GetCurDtTmStr + " Loading Input File %s into memory.".format(inputeventfile))
    var headerLines: Long = 0
    var validInputLines: Long = 0
    var invalidInputLines: Long = 0
    val ReadStart = System.nanoTime
    // Read Input file
    breakable {
      while (true) {
        val line = br.readLine
        if (line == null)
          break
        if (headerLines == 0) {
          headerLines = headerLines + 1
        } else {
          val firstIdxPos = line.indexOf(',')
          var validLn = (firstIdxPos != -1)
          if (validLn) {
            val secondIdxPos = line.indexOf(',', firstIdxPos + 1)
            validLn = (secondIdxPos != -1)
            if (validLn) {
              inputTemplateRows += "%d,%d" + line.substring(secondIdxPos) + "\n"
            }
          }
          if (validLn) {
            validInputLines = validInputLines + 1
          } else {
            invalidInputLines = invalidInputLines + 1
          }
        }
      }
    }
    val ReadEnd = System.nanoTime
    // Closing input reader
    br.close();
    println(GetCurDtTmStr + " Loaded %d valid lines & %d invalid lines from Input File %s. Elapsed Time:%dms.".format(validInputLines, invalidInputLines, inputeventfile, (ReadEnd - ReadStart) / 1000000))

    val rand = new Random(hashCode)

    // Write headers into files
    osAlertHist.write(alertHistHdr.getBytes("UTF8"));
    osCustPref.write(custPrefHdr.getBytes("UTF8"));
    osFacts.write(factHdr.getBytes("UTF8"));

    var generatedRows: Long = 0
    var accno: Long = startaccountnumber
    // Generate Output
    // Generate upto 7 accounts per user

    val generateMultipleRowsPerAcc = true

    println(GetCurDtTmStr + " Generating Data...")

    val WriteStart = System.nanoTime

    while (generatedRows < outputrows) {
      val cust_accs = rand.nextInt(199)
      val cust_id = accno

      if (generateMultipleRowsPerAcc) {
        val AccShortNm = "SHTNM_" + accno
        val ts = "TS" + rand.nextInt(3)
        val mobile_no = "+%d-%s-%s-%s".format(rand.nextInt(100), MakeNDigits(rand.nextInt(97979797) + 1, 3), MakeNDigits(rand.nextInt(97997977) + 1, 3), MakeNDigits(rand.nextInt(99797797) + 1, 4))
        val no_factor = 40 + rand.nextInt(60)
        osAlertHist.write(alertHistFmt.format(cust_id, accno).getBytes("UTF8"));
        osCustPref.write(custPrefFmt.format(cust_id, accno, AccShortNm, ts, rand.nextInt(5000), mobile_no, no_factor).getBytes("UTF8"));

        breakable {
          for (accs <- 0 until cust_accs) {
            osFacts.write(inputTemplateRows(rand.nextInt(inputTemplateRows.size)).format(cust_id, accno).getBytes("UTF8"))
            generatedRows = generatedRows + 1
            if (generatedRows >= outputrows)
              break
            if (generatedRows % 25000 == 0)
              println(GetCurDtTmStr + " Generated %10d lines.".format(generatedRows))
          }
        }

        accno = accno + 1
      } else {
        breakable {
          for (accs <- 0 until cust_accs) {
            val AccShortNm = "SHTNM_" + accno
            val ts = "TS" + rand.nextInt(3)
            val mobile_no = "+%d-%s-%s-%s".format(rand.nextInt(100), MakeNDigits(rand.nextInt(97979797) + 1, 3), MakeNDigits(rand.nextInt(97997977) + 1, 3), MakeNDigits(rand.nextInt(99797797) + 1, 4))
            val no_factor = 40 + rand.nextInt(60)
            osAlertHist.write(alertHistFmt.format(cust_id, accno).getBytes("UTF8"));
            osCustPref.write(custPrefFmt.format(cust_id, accno, AccShortNm, ts, rand.nextInt(5000), mobile_no, no_factor).getBytes("UTF8"));
            osFacts.write(inputTemplateRows(rand.nextInt(inputTemplateRows.size)).format(cust_id, accno).getBytes("UTF8"))
            generatedRows = generatedRows + 1
            accno = accno + 1
            if (generatedRows >= outputrows)
              break
            if (generatedRows % 25000 == 0)
              println(GetCurDtTmStr + " Generated %10d lines.".format(generatedRows))
          }
        }
      }
    }

    val WriteEnd = System.nanoTime

    println(GetCurDtTmStr + " Generated %d lines with header into fact file %s and corresponding Account history in %s and Customer Preferences into %s. Elapsed Time:%dms.".format(generatedRows, factData, alertHistory, custPreferences, (WriteEnd - WriteStart) / 1000000))

    // Close Output files
    if (osAlertHist != null)
      osAlertHist.close
    if (osCustPref != null)
      osCustPref.close
    if (osFacts != null)
      osFacts.close
    br.close

  }
}
