package org.projecthaystack.ob;

import org.projecthaystack.client.HClient;

import java.sql.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.projecthaystack.ob.SqlDB.*;

public class Main {
    public static void main(String[] args) {

        try{
            HClient client = HClient.open("http://192.168.11.2:8282/api/morrisons","su","Optimised1234");

            ArrayList<String> sites =  new Axon().getSites(client);
//            insertSites(sites);
//
//            // Set the equip into the DB
//            TreeMap<Integer,String> dbSites = getSites();
//            for (Map.Entry<Integer,String> entry :dbSites.entrySet()){
//                ArrayList<String> equip1 = new Axon().getEquip(client,entry.getValue());
//                insertEquip(entry.getKey(),equip1);
//            }
//
//            //To get points for equip we need the site name, equip name. To store in DB we need equipID
//            // First get equip for OB
//            TreeMap<Integer,String> dbEquips = getEquips();
//
//            //Set the points points
//            for (Map.Entry<Integer,String> entry :dbEquips.entrySet()){
//                String  siteName = getSiteNameFromEquipId(entry.getKey());
//                ArrayList<String> points = new Axon().getPoints(client,siteName,entry.getValue());
//                insertPoints(entry.getKey(),points);
//            }

            TreeMap<Integer, String> points =  getSites();
            for (Map.Entry<Integer,String> entry: points.entrySet()){

            }


        }catch (Exception ex) {
            ex.printStackTrace();
        }

    }


    public static void sqlCon(TreeMap<String,String> his){

            String url = "jdbc:mysql://localhost:3306/test?useSSL=false";
            String user = "root";
            String password = "Optimised1234";
            String query = "SELECT * FROM table1";
             String queryInsert = " insert into table2 (name, value)"
                + " values (?, ?)";

            try (Connection con = DriverManager.getConnection(url, user, password);
                 Statement st = con.createStatement();
                 ResultSet rs = st.executeQuery(query)) {

                PreparedStatement preparedStmt = con.prepareStatement(queryInsert);
                for(Map.Entry<String,String> entry : his.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    preparedStmt.setString (1, key);
                    preparedStmt.setString (2, value);
                    preparedStmt.execute();
                }
                while (rs.next()) {
                    System.out.println(rs.getString(1)+ " " + rs.getString(2));
                }
            } catch (SQLException ex) {
                Logger lgr = Logger.getLogger(Main.class.getName());
                lgr.log(Level.SEVERE, ex.getMessage(), ex);
            }
        }
    }

