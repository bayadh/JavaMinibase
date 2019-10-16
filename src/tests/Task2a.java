package tests;
//originally from : joins.C

import iterator.*;
import heap.*;
import global.*;
import index.*;
import java.io.*;
import java.util.*;

import com.sun.javafx.image.impl.IntArgbPre;

// import com.sun.xml.internal.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;

import java.lang.*;
import diskmgr.*;
import bufmgr.*;
import btree.*; 
import catalog.*;

/**
   Here is the implementation for the tests. There are N tests performed.
   We start off by showing that each operator works on its own.
   Then more complicated trees are constructed.
   As a nice feature, we allow the user to specify a selection condition.
   We also allow the user to hardwire trees together.
*/
class Table {
  private AttrType[] attrTypes;
  private short[]  sizes;
  private String heapfile = "";
  private short numberOfAttr;
  private String TableName;
  private short numberOfStrAttr;
  private Integer numberOfRecord;

  public AttrType[] getAttrType(){
    return attrTypes;
  }
  public short[] getStrAttrSize(){
    return sizes;
  }
  public String getHeapFile(){
    return heapfile;
  }
  public String getTableName(){
    return TableName;
  }
  public short getnumberOfAttr(){
    return numberOfAttr;
  }
  public short getNumberOfStrAttr(){
    return numberOfStrAttr;
  }
  public Integer getNumberOfRecord(){
    return numberOfRecord;
  }

  public void Info(){
    if (numberOfAttr == 0 ){
      System.out.println("Table is empty.");
    }
    else {
      System.out.println("Table name: "+ TableName);
      System.out.println("Heapfile name: "+ heapfile);
      System.out.println("Number of attribute: "+numberOfAttr);
      System.out.println("Number of string attribute: "+ numberOfStrAttr);
      System.out.println("Number of records: "+numberOfRecord);
    }
  }

  public Tuple get_define_tuple(){
    Tuple tt = new Tuple();
    try {
      tt.setHdr(numberOfAttr, attrTypes, sizes);
    }
    catch (Exception e) {
      // status = FAIL;
      e.printStackTrace();
    }

    int sizett = tt.size();
    tt = new Tuple(sizett);
    try {
      tt.setHdr(numberOfAttr, attrTypes, sizes);
    }
    catch (Exception e) {
      // status = FAIL;
      e.printStackTrace();
    }
    return tt;
  }
  


  public Table(String tableName){
    TableName = tableName;
    heapfile = tableName+".in";
    numberOfAttr = 0;
    numberOfStrAttr = 0;
    numberOfRecord= 0;
  }
  public void loadRecordsFromFile(String filepath,int numrecords){
    // boolean status = OK;
    String line;
    String attrLine ="";
    ArrayList<String> dataList = new ArrayList<String>(); 
    //Read file
    //==============================================
    try {
      FileReader fileReader = new FileReader(filepath);
      BufferedReader bufferedReader = new BufferedReader(fileReader);
      // First line for attribute type
      attrLine = bufferedReader.readLine();
      // All other lines are data
      while((line = bufferedReader.readLine()) != null) {
        dataList.add(line);
      }
      bufferedReader.close();
    }
    catch(FileNotFoundException ex) {
      System.out.println("Unable to open file '" + filepath + "'");                
    }
    catch(IOException ex) {
      System.out.println("Error reading file '" + filepath + "'");                  
    }
    //==============================================
    // Define attribute types
    String[] attrList = attrLine.split(",");
    numberOfAttr = (short)attrList.length;
    attrTypes = new AttrType[numberOfAttr];
    for (int i=0;i< numberOfAttr;i++){
      switch (attrList[i])
      {
        case "attrString":
          attrTypes[i] = new AttrType (AttrType.attrString);
          numberOfStrAttr ++; // Number of String Attribute 
          break;
        case "attrInteger":
          attrTypes[i] = new AttrType (AttrType.attrInteger);
          break;
        case "attrReal":
          attrTypes[i] = new AttrType (AttrType.attrReal);
          break;
        case "attrSymbol":
          attrTypes[i] = new AttrType (AttrType.attrSymbol);
          break;
        default:  
          attrTypes[i] = new AttrType (AttrType.attrNull);
      }
    }
    //Define the size of string attr
    //Assume that the max size of string attr is 100
    if (numberOfStrAttr == 0){
      sizes = null;
    }
    else {
      sizes = new short[numberOfStrAttr];
      for (int i=0; i<numberOfStrAttr; i++){
        sizes[i] =100;
      }
    }
    // numrecords: limit number of records need to be read.
    // if numrecords == 0, read all records.
    if (numrecords == 0){
      numberOfRecord = dataList.size();
    }
    else {
      numberOfRecord = numrecords;
    }
    // System.out.println(numberOfRecord);

      //Define triple format 
    Tuple t = new Tuple();
    Heapfile f = null;
    try {
      t.setHdr((short) numberOfAttr ,attrTypes, sizes);
    }
    catch (Exception e) {
      System.err.println("*** error in Tuple.setHdr() ***");
      // status = FAIL;
      e.printStackTrace();
    }
    int size = t.size();
    // inserting the tuple into heapfile
    //RID             rid;
    RID  rid;
    f = null;
    try {
      f = new Heapfile(heapfile);
    }
    catch (Exception e) {
      System.err.println("*** error in Heapfile constructor ***");
      // status = FAIL;
      e.printStackTrace();
    }
      //Load records values from file to heapfile

    for (int j=0; j<numberOfRecord;j++){
      line = dataList.get(j);
      String[] attrValues = line.split(",");

      try {
        t = new Tuple(size);
        try {
          t.setHdr((short) attrList.length,attrTypes, sizes);
        }
        catch (Exception e) {
          System.err.println("*** error in Tuple.setHdr() ***");
          // status = FAIL;
          e.printStackTrace();
        }
        for (int i=0;i< attrList.length;i++){
          
          if (attrList[i].equals("attrString")){
            t.setStrFld(i+1, attrValues[i]);
          }
          else if (attrList[i].equals("attrInteger")){
            t.setIntFld(i+1, Integer.parseInt(attrValues[i]));
          }
          else if (attrList[i].equals("attrReal")){
            t.setFloFld(i+1, Float.parseFloat(attrValues[i]));
          }
        }
      }
      catch (Exception e) {
        System.err.println("*** error in Tuple.setStrFld() ***");
        // status = FAIL;
        e.printStackTrace();
      }      
        
      try {
        rid = f.insertRecord(t.returnTupleByteArray());
      }
      catch (Exception e) {
        System.err.println("*** error in Heapfile.insertRecord() ***");
        // status = FAIL;
        e.printStackTrace();
      }   
    }
  }
}

class JoinsDriver_2 implements GlobalConst {
  
  private boolean OK = true;
  private boolean FAIL = false;
  /** Constructor
   */
  public JoinsDriver_2() {
    
    //Configurate database
    String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.jointestdb"; 
    String logpath = "/tmp/"+System.getProperty("user.name")+".joinlog";


    //Clear database before insert new records
    String remove_cmd = "/bin/rm -rf ";
    String remove_logcmd = remove_cmd + logpath;
    String remove_dbcmd = remove_cmd + dbpath;
    String remove_joincmd = remove_cmd + dbpath;

    try {
      Runtime.getRuntime().exec(remove_logcmd);
      Runtime.getRuntime().exec(remove_dbcmd);
      Runtime.getRuntime().exec(remove_joincmd);
    }
    catch (IOException e) {
      System.err.println (""+e);
    }

    /*
    ExtendedSystemDefs extSysDef = 
      new ExtendedSystemDefs( "/tmp/minibase.jointestdb", "/tmp/joinlog",
			      1000,500,200,"Clock");
    */

    SystemDefs sysdef = new SystemDefs( dbpath, 1000, NUMBUF, "Clock" );
  }
  // public int[] get_Int_Tuple(Tuple t, short FldNum){
  //   int[] intergerArray = new int[FldNum]; 
  //   for(int i =0; i<FldNum; i++){
  //     intergerArray[i] = t.getIntFld(i);
  //   }
  //   return intergerArray;
  // }
  
  public boolean runTests() {
    
    Disclaimer();
    Query2();
    
    System.out.print ("Finished joins testing"+"\n");
   
    
    return true;
  }
  
  public void Query2() {
    boolean status = OK;

    //LOADING DATA
    Table Qtable = new Table("Q");
    Qtable.loadRecordsFromFile("../../queriesdata/Q.txt", 1000); //Change 0 to 5 to test with 5 records
    //Different part comparing with task1 a: adding one more attribute from projection of R table
    //--------------------------------------
    ////===============MANUAL============Since only column 1 and column 3 is needed for query
    FldSpec [] Qprojection = new FldSpec[2];
    Qprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
    // Qprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
    Qprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 3);
    // Qprojection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);
    ////===============MANUAL============
    FileScan qtable = null;
    try {
      qtable  = new FileScan(Qtable.getHeapFile(), Qtable.getAttrType(), Qtable.getStrAttrSize(), 
				  (short)Qtable.getnumberOfAttr(), (short)2, //MANUAL
				  Qprojection, null); //MANUAL
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }
    //--------------------------------------
    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for sailors");
      Runtime.getRuntime().exit(1);
    }
    TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
    Sort sort_names = null;

    AttrType [] Outtypes = {
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrInteger), 
    };
    try {
      sort_names = new Sort (Outtypes,(short)2, Qtable.getStrAttrSize(),
      (iterator.Iterator) qtable, 
      2, //Sort on the second attribute
      ascending, 
      4, 
      10);
    }
    catch (Exception e) {
      System.err.println ("*** Error preparing for sorting");
      System.err.println (""+e);
      Runtime.getRuntime().exit(1);
    }
    Tuple t = null;

    AttrType [] jtype = new AttrType[2];
    jtype[0] = new AttrType (AttrType.attrInteger);
    jtype[1] = new AttrType (AttrType.attrInteger);

    ArrayList<Tuple> listTuple = new ArrayList<>();
    try {
      while ((t = sort_names.get_next()) != null) {
        int size = t.size();
        Tuple tt = new Tuple(size);
        tt.tupleCopy(t);
        listTuple.add(tt);
      }
    }
      catch (Exception e) {
        System.err.println (""+e);
         e.printStackTrace();
         status = FAIL;
      }
    try{
      

      FldSpec [] perm_mat = new FldSpec[2];
      perm_mat[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
      perm_mat[1] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);


      for (int i=0; i< listTuple.size();i++) {
        Tuple outerTuple = new Tuple();
        outerTuple.setHdr((short) 2, jtype, null);
        outerTuple.tupleCopy(listTuple.get(i));
        for (int j=i+1;j<listTuple.size();j++){
          Tuple innerTuple = new Tuple();
          innerTuple.setHdr((short) 2, jtype, null);
          innerTuple.tupleCopy(listTuple.get(j));


          Tuple outTuple = new Tuple();
          outTuple.setHdr((short) 2, jtype, null);

          Projection.Join(outerTuple, jtype, 
          innerTuple, jtype, outTuple, perm_mat, 2);
          outTuple.print(jtype);
        }
        
      }
    }
          catch (Exception e) {
        System.err.println (""+e);
         e.printStackTrace();
         status = FAIL;
      }

      

    if (status != OK) {
      //bail out
      System.err.println ("*** Error in get next tuple ");
      Runtime.getRuntime().exit(1);
    }
  }


  
  private void Disclaimer() {
    System.out.print ("\n\nAny resemblance of persons in this database to"
         + " people living or dead\nis purely coincidental. The contents of "
         + "this database do not reflect\nthe views of the University,"
         + " the Computer  Sciences Department or the\n"
         + "developers...\n\n");
  }

}


public class Task2a
{
  public static void main(String argv[])
  {
    boolean sortstatus;
    //SystemDefs global = new SystemDefs("bingjiedb", 100, 70, null);
    //JavabaseDB.openDB("/tmp/nwangdb", 5000);

    JoinsDriver_2 jjoin = new JoinsDriver_2();

    sortstatus = jjoin.runTests();
    if (sortstatus != true) {
      System.out.println("Error ocurred during join tests");
    }
    else {
      System.out.println("join tests completed successfully");
    }
  }
}

