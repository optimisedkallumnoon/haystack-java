package org.projecthaystack.ob;

import org.projecthaystack.client.HClient;
import org.projecthaystack.ob.fileDataEncryption.ProtectedConfigFile;

import javax.crypto.spec.SecretKeySpec;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.time.LocalDate;



public class Main {
    static int clientId;
    static final String toBase64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    static int periodCount = 0;
    static String password = "0pt1m1s3d1nt3gr4t10n5@";
    static byte[] salt = new String("12345678").getBytes();
    static int iterationCount = 40000;
    static int keyLength = 128;
    public static void main(String[] args) {
        try {
            SecretKeySpec key  = ProtectedConfigFile.createSecretKey(password.toCharArray(),salt, iterationCount, keyLength);
            FileReader reader = new FileReader("config.properties");
            Properties props = new Properties();
            props.load(reader);
            clientId = Integer.parseInt(props.getProperty("clientId"));
            String skysparkurl = props.getProperty("skysparkurl");
            String skysparkuser = props.getProperty("skysparkuser");
            String schema = props.getProperty("schema");
            skysparkuser = ProtectedConfigFile.decrypt(skysparkuser,key);
            String skysparkpass = props.getProperty("skysparkpass");
            skysparkpass = ProtectedConfigFile.decrypt(skysparkpass,key);
            String server = props.getProperty("server");
            String dbuser = props.getProperty("dbuser");
            dbuser = ProtectedConfigFile.decrypt(dbuser, key);
            String dbpass = props.getProperty("dbpass");
            dbpass = ProtectedConfigFile.decrypt(dbpass, key);
            String db = props.getProperty("db");
            String sparkTable = props.getProperty("sparkTable");
            String equipTable = props.getProperty("equipTable");
            String sensTable = props.getProperty("sensTable");

            HClient client = HClient.open(skysparkurl,skysparkuser,skysparkpass);
            if(args.length == 0) {
                importSparks(client,
                        server,
                        dbuser,
                        dbpass,
                        db,
                        sparkTable,
                        schema);
            }else if(args[0].equals("sparks")){
                importSparks(client,
                        server,
                        dbuser,
                        dbpass,
                        db,
                        sparkTable,
                        schema);
            }else if(args[0].equals("equip")){
                importEquip(client,
                        server,
                        dbuser,
                        dbpass,
                        db,
                        equipTable,
                        schema);
            }else if(args[0].equals("sens")){
                importSensor(client,
                        server,
                        dbuser,
                        dbpass,
                        db,
                        sensTable,
                        schema);
            }else if(args[0].equals("point")){
                importPoint(client,
                        server,
                        dbuser,
                        dbpass,
                        db,
                        sensTable,
                        schema);
            }

        } catch (FileNotFoundException ex) {
            System.out.println("File not found" + ex);
        } catch (IOException ex) {
            System.out.println("IOException" + ex);
        }catch (Exception ex) {
        ex.printStackTrace();
    }




    }
    public static void importPoint(HClient client,
                                    String server,
                                    String user,
                                    String password,
                                    String db,
                                    String table,
                                    String schema){

        ArrayList<String> sites = new Axon().getSiteId(client);
        int totalPoints = 0;
        for(String site : sites){
            ArrayList<String[]> point = new Axon().getPoints(client,site.split(" ")[0]);
            if(point.size()>0) {
                sensorToDb(point,server,user,password,db,table,schema,"point");
            }
            totalPoints += point.size();
        }
        System.out.println(totalPoints);

    }
    public static void importSensor(HClient client,
                                   String server,
                                   String user,
                                   String password,
                                   String db,
                                   String table,
                                   String schema){

        ArrayList<String[]> sens = new Axon().getSensors(client);
        sensorToDb(sens,server,user,password,db,table,schema,"sensor");
        System.out.println(sens.size());
    }
    public static void importEquip(HClient client,
                                   String server,
                                   String user,
                                   String password,
                                   String db,
                                   String table,
                                   String schema){

            ArrayList<String[]> equips = new Axon().getEquip(client);
            equipToDb(equips,server,user,password,db,table,schema);
            System.out.println(equips.size());
    }

    public static void importSparks(HClient client,
                                    String server,
                                    String user,
                                    String password,
                                    String db,
                                    String table,
                                    String schema){
        String date = lastSparkDate(server,user,password,schema,table,db);
        String endDate = LocalDate.now().toString();

            ArrayList<String> rules = new Axon().getRules(client);
            System.out.println(rules);
            System.out.println(rules.size());
        String connectionUrl =
                "jdbc:sqlserver://%s;"
                        + "database=%s;"
                        + "user=%s;"
                        + "password=%s;"
                        + "encrypt=false;"
                        + "trustServerCertificate=false;"
                        + "loginTimeout=30;";
        connectionUrl = String.format(connectionUrl, server,db,user,password);
        String queryInsert = " INSERT INTO [%s].[%s]\n" +
                " ([clientNo],[siteNo],[targetRef],[ruleRef],[date],[tz],[spark],[dur],[periods]\n";
        String queryValue = "VALUES(?,?,?,?,?,?,?,?,?";

        int mins = 0;
        int hours = 0;
        StringBuilder sb1 = new StringBuilder(queryInsert);
        StringBuilder sb2 = new StringBuilder(queryValue);
        for(int i = 0; i<24*4; i++){
            sb1.append(String.format(",[%02d:%02d]",hours,mins));
            sb2.append(",?");
            mins += 15;
            if (mins == 60){
                mins = 0;
                hours +=1;
            }

        }
        sb1.append(")");
        sb2.append(")");
        sb1.append(sb2);
        queryInsert = sb1.toString();
        queryInsert = String.format(queryInsert,schema,table);
        while (!date.equals(endDate)) {
            if (LocalDate.parse(date).isAfter(LocalDate.now())){
                break;
            }
            System.out.println(date);
            for (String rule : rules) {
                rule = rule.split("\"")[1];
                System.out.println(rule);

                ArrayList<String[]> dur = new Axon().getSparks(client,rule ,date);
                sparksToDb(dur, connectionUrl,queryInsert);



            }
            date = addOneDay(date);

        }
        System.out.printf("There are %d periods%n", periodCount);
    }
    public static Integer sparkTime(String t){
        int hours;
        int mins;
        int time = 0;
        if(t.contains("h")){
                  t = (t.substring(0,t.length()-1));
                  if (t.contains(".")){
                      String[] splt = t.split("\\.");
                      hours = Integer.parseInt(splt[0]);
                      mins = Math.round(Float.parseFloat("0."+splt[1])*60);
                  }else{
                      mins = 0;
                      hours = Integer.parseInt(t);
                  }
                  time = hours*60 + mins;
                  return time;
        }else if(t.contains("min")){
            mins = Integer.parseInt(t.substring(0,t.length()-3));
            return mins;
        }
        return time;
    }

    public static void equipToDb(ArrayList<String[]> equip,
                                 String server,
                                 String user,
                                 String password,
                                 String db,
                                 String table,
                                 String schema){
        String connectionUrl =
                "jdbc:sqlserver://%s;"
                        + "database=%s;"
                        + "user=%s;"
                        + "password=%s;"
                        + "encrypt=false;"
                        + "trustServerCertificate=false;"
                        + "loginTimeout=30;";
        connectionUrl = String.format(connectionUrl, server,db,user,password);
        String query = String.format("INSERT INTO [%s].[%s]" +
                        "([clientId],[skysparkId],[navName],[siteRef]",schema,table);

        String queryValue = " VALUES(?,?,?,?";
        List<String> headers = Arrays.asList(equip.get(0));
        Integer[] skipColumn = {headers.indexOf("id"),
                headers.indexOf("disMacro"),
                headers.indexOf("navName"),
                headers.indexOf("equip"),
                headers.indexOf("mod"),
                headers.indexOf("siteRef")};
        StringBuilder sb1 = new StringBuilder(query);
        StringBuilder sb2 = new StringBuilder(queryValue);
        for (int i=1;i<=headers.size()-skipColumn.length;i++){
            sb1.append(",infocol");
            sb1.append(i);
            sb2.append(",?");
        }
        sb1.append(")");
        sb2.append(");");
        sb1.append(sb2);
        query = sb1.toString();
        System.out.println(query);

        try (Connection con = DriverManager.getConnection(connectionUrl)){
            PreparedStatement preparedStatement = con.prepareStatement(query);
            equip.remove(0);
            for(String[] entry : equip) {
                preparedStatement.setInt(1, clientId);
                preparedStatement.setString(2, entry[skipColumn[0]].split("\"")[0]);
                preparedStatement.setString(3, entry[skipColumn[2]].replace("\"",""));
                if (entry[skipColumn[5]].contains(" ")) {
                    preparedStatement.setString(4, entry[skipColumn[5]].split(" ")[1]
                            .replace("\"", ""));
                }else {
                    preparedStatement.setString(4, entry[skipColumn[5]]
                            .replace("\"", ""));
                }
                List<Integer> skipColumnsList = Arrays.asList(skipColumn);
                int counter = 5;
                String value;
                for (int i = 0; i < headers.size(); i++) {
                    if (!skipColumnsList.contains(i)) {
                        if (entry[i].equals("")) {
                            value = "NULL";
                        } else {
                            value = entry[i];
                        }
                        preparedStatement.setString(counter, headers.get(i) + ":" + value);
                        counter++;
                    }
                }
                preparedStatement.execute();
            }
        } catch (SQLException ex) {
            Logger lgr = Logger.getLogger(Main.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        }
    }

    public static void sensorToDb(ArrayList<String[]> equip,
                                 String server,
                                 String user,
                                 String password,
                                 String db,
                                 String table,
                                 String schema,
                                  String type){
        String connectionUrl =
                "jdbc:sqlserver://%s;"
                        + "database=%s;"
                        + "user=%s;"
                        + "password=%s;"
                        + "encrypt=false;"
                        + "trustServerCertificate=false;"
                        + "loginTimeout=30;";
        connectionUrl = String.format(connectionUrl, server,db,user,password);
        String query = String.format("INSERT INTO [%s].[%s]" +
                "([clientId],[skysparkId],[equipRef],[navName],[type]",schema,table);

        String queryValue = " VALUES(?,?,?,?,?";
        List<String> headers = Arrays.asList(equip.get(0));
        Integer[] skipColumnReg = {headers.indexOf("id"),
                headers.indexOf("disMacro"),
                headers.indexOf("navName"),
                headers.indexOf("equipRef"),
                headers.indexOf("mod"),
                headers.indexOf("point"),
                headers.indexOf("siteRef"),
                headers.indexOf("axHistoryId"),
                headers.indexOf("axSlotPath"),
                headers.indexOf("axStatus"),
                headers.indexOf("axType"),
                headers.indexOf("connRef"),
                headers.indexOf("cur"),
                headers.indexOf("curStatus"),
                headers.indexOf("curVal"),
                headers.indexOf("enum"),
                headers.indexOf("haystackConnRef"),
                headers.indexOf("haystackCur"),
                headers.indexOf("haystackHis"),
                headers.indexOf("haystackPoint"),
                headers.indexOf("his"),
                headers.indexOf("hisAppendNA"),
                headers.indexOf("hisEnd"),
                headers.indexOf("hisErr"),
                headers.indexOf("hisFunc"),
                headers.indexOf("hisInterval"),
                headers.indexOf("hisMode"),
                headers.indexOf("hisOb"),
                headers.indexOf("hisSize"),
                headers.indexOf("hisStart"),
                headers.indexOf("hisStatus"),
                headers.indexOf("kind"),
                headers.indexOf("point"),
                headers.indexOf("sensor"),
                headers.indexOf("tz"),
                headers.indexOf("unit"),
                headers.indexOf("weatherCond"),
                headers.indexOf("weatherPoint"),
                headers.indexOf("weatherRef"),
                headers.indexOf("weatherSyncId"),
                headers.indexOf("zone"),
                headers.indexOf("outside"),
                headers.indexOf("sp"),
                headers.indexOf("humidity"),
                headers.indexOf("discharge"),
                headers.indexOf("fan"),
                headers.indexOf("flow"),
                headers.indexOf("flow"),
                headers.indexOf("pressure"),
                headers.indexOf("space")

        };



        try (Connection con = DriverManager.getConnection(connectionUrl)){
            equip.remove(0);



            for(String[] entry : equip) {
                StringBuilder sb1 = new StringBuilder(query);
                StringBuilder sb2 = new StringBuilder(queryValue);
                List<Integer> skipColumnsList = Arrays.asList(skipColumnReg);
                ArrayList<String> values = new ArrayList<>();
                int counter = 1;
                for (int i = 0; i < headers.size(); i++) {
                    if (!skipColumnsList.contains(i)) {
                        if (!entry[i].equals("")) {
                            sb1.append(",infocol");
                            sb1.append(counter);
                            sb2.append(",?");
                            values.add(headers.get(i) + ":" + entry[i]);
                            counter++;
                        }
                    }
                }
                sb1.append(")");
                sb2.append(");");
                sb1.append(sb2);
                System.out.println(values);
                String queryOut = sb1.toString();
                System.out.println(queryOut);
                PreparedStatement preparedStatement = con.prepareStatement(queryOut);

                preparedStatement.setInt(1, clientId);
                preparedStatement.setString(2, entry[skipColumnReg[0]].split("\"")[0]);
                preparedStatement.setString(3, entry[skipColumnReg[3]].split("\"")[0]);
                preparedStatement.setString(4, entry[skipColumnReg[2]].replace("\"",""));
                preparedStatement.setString(5, type);
                counter = 6;
                for(String value: values){
                    preparedStatement.setString(counter,value);
                    counter++;
                }


                preparedStatement.execute();
            }
        } catch (SQLException ex) {
            Logger lgr = Logger.getLogger(Main.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        }
    }
    public static String lastSparkDate(String server,
                                       String user,
                                       String password,
                                       String schema,
                                       String table,
                                       String db){
        String startDate;
        String connectionUrl =
                "jdbc:sqlserver://%s;"
                        + "database=%s;"
                        + "user=%s;"
                        + "password=%s;"
                        + "encrypt=false;"
                        + "trustServerCertificate=false;"
                        + "loginTimeout=140;";
        connectionUrl = String.format(connectionUrl, server,db,user,password);
        System.out.println(connectionUrl);
        String query = String.format("SELECT TOP (1)\n" +
                "      [date]\n" +
                "  FROM [%s].[%s] where clientNo = %s ORDER BY [date] desc ",schema,table,clientId);
        try (Connection con = DriverManager.getConnection(connectionUrl)){
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            if (rs.next()){
                startDate = addOneDay(rs.getString("date"));
            }else{
                startDate = LocalDate.now().plusDays(-30).toString();
            }


        } catch (SQLException ex) {
            Logger lgr = Logger.getLogger(Main.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
            startDate = "";
        }
        System.out.println(startDate);
        if (startDate.isEmpty()){
            startDate = LocalDate.now().plusDays(-30).toString();
        }
        return startDate;
    }
    public static String addOneDay(String date) {
        return LocalDate.parse(date).plusDays(1).toString();
    }
    public static void sparksToDb(ArrayList<String[]> sparks,
                                  String connectionUrl,
                                  String queryInsert){

        int duration;

        try (Connection con = DriverManager.getConnection(connectionUrl)){

                PreparedStatement preparedStmt = con.prepareStatement(queryInsert);
                for(String[] entry : sparks) {
                    if (entry[1].split("\"").length >1) {
                        String[] temp = entry[0].split("\"");
                        String targetRef = temp[0];
                        String ruleRef = entry[1].split("\"")[1];
                        String date = entry[2];
                        String tz = entry[3];
                        int sp = 1;
                        duration = sparkTime(entry[5]);
                        String decodedPeriods = periodsToTime(entry[6].replace("\"", ""));
                        ArrayList<Integer> bitPeriods = periodToBit(decodedPeriods);
                        preparedStmt.setInt(1, clientId);
                        preparedStmt.setString(2, temp[1].substring(0, 3));
                        preparedStmt.setString(3, targetRef);
                        preparedStmt.setString(4, ruleRef);
                        preparedStmt.setString(5, date);
                        preparedStmt.setString(6, tz);
                        preparedStmt.setInt(7, sp);
                        preparedStmt.setInt(8, duration);
                        preparedStmt.setString(9, decodedPeriods);
                        for (int i = 0; i < 24 * 4; i++) {
                            preparedStmt.setInt(i + 10, bitPeriods.get(i));
                        }
                        preparedStmt.execute();
                    }
                }
            } catch (SQLException ex) {
                Logger lgr = Logger.getLogger(Main.class.getName());
                lgr.log(Level.SEVERE, ex.getMessage(), ex);
            }
        }
        public static String periodsToTime(String encodedPeriods){
            StringBuilder sb = new StringBuilder();
            for(int i=0; i<encodedPeriods.length(); i+=4){
                periodCount++;
                int timeInMins = ((toBase64.indexOf(encodedPeriods.charAt(i)) << 6) + toBase64.indexOf(encodedPeriods.charAt(i + 1)));
                int durInMins = ((toBase64.indexOf(encodedPeriods.charAt(i + 2)) << 6) + toBase64.indexOf(encodedPeriods.charAt(i + 3)));
                int hours = Math.toIntExact(TimeUnit.HOURS.convert(timeInMins, TimeUnit.MINUTES));
                int minutes = timeInMins - (hours * 60);
                int closeTime = timeInMins + durInMins;
                int hoursClose = Math.toIntExact(TimeUnit.HOURS.convert(closeTime, TimeUnit.MINUTES));
                int minutesClose = closeTime - (hoursClose * 60);
                sb.append(String.format("%02d:%02d,%02d:%02d;", hours, minutes, hoursClose,minutesClose));
            }
            return sb.toString();
        }


        public static ArrayList<Integer> periodToBit(String decodedPeriods){
            ArrayList<Integer> periodBits = new ArrayList<>();
            String[] periods = decodedPeriods.split(";");
            int periodCounter = 0;
            boolean startEnd = false;
            int mins = 0;
            int hours = 0;
            for(int i = 0; i<24*4; i++){
                if (periodCounter>=periods.length){
                    periodBits.add(0);
                }else{
                    if (startEnd) {
                        if (periods[periodCounter].split(",")[1].equals(String.format("%02d:%02d",hours,mins))){
                            periodBits.add(0);
                            startEnd = false;
                            periodCounter++;
                        }else{
                            periodBits.add(1);
                        }
                    }else {
                        if (periods[periodCounter].split(",")[0].equals(String.format("%02d:%02d",hours,mins))){
                            periodBits.add(1);
                            startEnd = true;
                        }else{
                            periodBits.add(0);
                        }

                    }
                }
                mins += 15;
                if (mins == 60){
                    mins = 0;
                    hours +=1;
                }

            }
            return periodBits;
        }
    }

