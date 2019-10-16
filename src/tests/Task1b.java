package tests;
//originally from : joins.C

import iterator.*;
import heap.*;
import global.*;
import index.*;
import java.io.*;
import java.util.*;

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

  
  public boolean runTests() {
    System.out.println("11");
    
    Disclaimer();
    Query2();
    
    System.out.print ("Finished joins testing"+"\n");
   
    
    return true;
  }

  private CondExpr[] Load_CondExpr(String[] elements) {
    CondExpr[] expr = new CondExpr[3];  
    expr[0] =  new CondExpr();

    expr[0].next  = null;
    // expr[0].op    = new AttrOperator(Integer.parseInt(elements[1]));
    expr[0].op    = new AttrOperator(AttrOperator.aopLT); ///1 is Less than as PDF convention
    // Type of operands in condition
    expr[0].type1 = new AttrType(AttrType.attrSymbol);
    expr[0].type2 = new AttrType(AttrType.attrSymbol);

    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),3);
    //Different part comparing with task1 a
    //--------------------------------------
    //expr[1] = null;
    expr[1] =  new CondExpr();

    expr[1].next  = null;
    // expr[0].op    = new AttrOperator(Integer.parseInt(elements[1]));
    expr[1].op    = new AttrOperator(AttrOperator.aopLE); //2 is "<=" as convention in PDF file, not same as minibase which is 4.
    // Type of operands in condition
    expr[1].type1 = new AttrType(AttrType.attrSymbol);
    expr[1].type2 = new AttrType(AttrType.attrSymbol);

    expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),3);
    expr[1].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),4);
  
    expr[2] = null;
    //--------------------------------------

    return expr;
  }
  
  public void Query2() {
    System.out.print("**********************Query2 strating *********************\n");
    boolean status = OK;

    // Sailors, Boats, Reserves Queries.
    System.out.print 
      ("Query: "
       + "  SELECT   R_1 S_1\n"
       + "  FROM     R S\n"
       + "  WHERE    R_3 1 S_3\n"
       + "  AND      R_4 2 S_4.\n");
    

    
    

    // Loading condition
    String query_file = "../../queriesdata/query_1a.txt";
    String [] Cond_elements = new String[3];
    String [] Proj_elements =  new String[3];
    try{
      FileReader fileReader = new FileReader(query_file);
      BufferedReader bufferedReader = new BufferedReader(fileReader);
      String line;
      //Project condition line
      line = bufferedReader.readLine();
      Proj_elements = line.split(" ");
      //Join elements 
      line = bufferedReader.readLine();
      //Condition elements
      line = bufferedReader.readLine();
      Cond_elements = line.split(" ");

    }
    catch(FileNotFoundException ex) {
      System.out.println("Unable to open file '" + query_file + "'");                
    }
    catch(IOException ex) {
        System.out.println("Error reading file '" + query_file + "'");                  
    }

    CondExpr [] outFilter  = Load_CondExpr(Cond_elements);
    
    Table Rtable = new Table("R");
    Table Stable = new Table("S");
    Rtable.loadRecordsFromFile("../../queriesdata/R.txt", 5); //Change 0 to 5 to test with 5 records
    Stable.loadRecordsFromFile("../../queriesdata/S.txt", 5); //Change 0 to 5 to test with 5 records
    //Different part comparing with task1 a: adding one more attribute from projection of R table
    //--------------------------------------
    ////===============MANUAL============Since only column 1 and column 3 is needed for query
    FldSpec [] Rprojection = new FldSpec[3];
    Rprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
    // Rprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
    Rprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 3);
    Rprojection[2] = new FldSpec(new RelSpec(RelSpec.outer), 4);
    ////===============MANUAL============
    FileScan am = null;
    try {
      am  = new FileScan(Rtable.getHeapFile(), Rtable.getAttrType(), Rtable.getStrAttrSize(), 
				  (short)Rtable.getnumberOfAttr(), (short)3, //MANUAL
				  Rprojection, null); //MANUAL
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


    FldSpec [] proj_list = new FldSpec[2];
    proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
    proj_list[1] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);

    NestedLoopsJoins nlj = null;
    try {
      nlj = new NestedLoopsJoins (Rtable.getAttrType(), Rtable.getnumberOfAttr(), Rtable.getStrAttrSize(),
				  Stable.getAttrType(), Stable.getnumberOfAttr(), Stable.getStrAttrSize(),
				  10,
				  am, Stable.getHeapFile(),
				  outFilter, null, proj_list, 2);
    }
    catch (Exception e) {
      System.err.println ("*** Error preparing for nested_loop_join");
      System.err.println (""+e);
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }

    Tuple t = null;
    AttrType [] jtype = new AttrType[2];
    jtype[0] = new AttrType (AttrType.attrInteger);
    jtype[1] = new AttrType (AttrType.attrInteger);

    try {
      while ((t = nlj.get_next()) != null) {
        // System.out.print("HERE");
        t.print(jtype);
        

        // qcheck1.Check(t);
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


public class Task1b
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

