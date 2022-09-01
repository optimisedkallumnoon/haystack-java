package org.projecthaystack.ob;

import java.sql.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SqlDB {
    private static final String serverName = "localhost";
    private static final String databaseName = "skysparknew";
    private static final String user = "root";
    private static final String password = "Optimised1234";

    private static final String dbUrl = "jdbc:mysql://" + serverName + ":3306/" + databaseName + "?useSSL=false";//

    public static void insertSites(ArrayList<String> sites){
        String queryInsertSites = " insert into sites (site_name)" + " values (?)";
        try (Connection con = DriverManager.getConnection(dbUrl, user, password)) {
            PreparedStatement preparedStmt = con.prepareStatement(queryInsertSites);
            for (String site: sites) {
                preparedStmt.setString (1, site);
                preparedStmt.execute();
            }
        } catch (SQLIntegrityConstraintViolationException uex){

        } catch (SQLException ex)

        {
            Logger lgr = Logger.getLogger(Main.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        }
    }

    public static TreeMap<Integer,String> getSites(){
        String queryGetSites = "select * from sites";
        TreeMap<Integer,String> sites = new TreeMap<>();

        try (Connection con = DriverManager.getConnection(dbUrl, user, password)) {
            Statement stmt=con.createStatement();
            ResultSet rs=stmt.executeQuery(queryGetSites);
            while (rs.next()){
                sites.put(rs.getInt(rs.findColumn("site_id")),rs.getString(rs.findColumn("site_name")));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return sites;
    }

    public static void insertEquip(int siteId,ArrayList<String> equips){
        String queryInsertEquip = " insert into equips (site_id,equip_name)" + " values (?,?)";
        try (Connection con = DriverManager.getConnection(dbUrl, user, password)) {
            PreparedStatement preparedStmt = con.prepareStatement(queryInsertEquip);
            for (String equip: equips) {
                preparedStmt.setString (1, String.valueOf(siteId));
                preparedStmt.setString (2, equip);
                preparedStmt.execute();
            }
        } catch (SQLIntegrityConstraintViolationException uex){

        } catch (SQLException ex)

        {
            Logger lgr = Logger.getLogger(Main.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        }
    }



    public static TreeMap<Integer,String> getEquips(){
        String queryGetSites = "select * from equips" ;
        TreeMap<Integer,String> equips = new TreeMap<>();

        try (Connection con = DriverManager.getConnection(dbUrl, user, password)) {
            Statement stmt=con.createStatement();
            ResultSet rs=stmt.executeQuery(queryGetSites);
            while (rs.next()){
                equips.put(rs.getInt(rs.findColumn("equip_id")),rs.getString(rs.findColumn("equip_name")));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return equips;
    }

    public static String getSiteNameFromEquipId(int equipId){
        String siteName ="";
        String queryGetSiteId = "select * from site_equip where equip_Id = " + equipId;
        try (Connection con = DriverManager.getConnection(dbUrl, user, password)) {
            Statement stmt=con.createStatement();
            ResultSet rs=stmt.executeQuery(queryGetSiteId);
            while (rs.next()){
               siteName = rs.getString(rs.findColumn("site_name"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return siteName;
    }


    public static void insertPoints(int equipId,ArrayList<String> points){
        String queryInsertEquip = " insert into points (equip_id,point_name)" + " values (?,?)";
        try (Connection con = DriverManager.getConnection(dbUrl, user, password)) {
            PreparedStatement preparedStmt = con.prepareStatement(queryInsertEquip);
            for (String point: points) {
                preparedStmt.setString (1, String.valueOf(equipId));
                preparedStmt.setString (2, point);
                preparedStmt.execute();
            }
        } catch (SQLIntegrityConstraintViolationException uex){

        } catch (SQLException ex)

        {
            Logger lgr = Logger.getLogger(Main.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        }
    }

    public static TreeMap<Integer,String> getPoints(){
        String queryGetSites = "select * from points" ;
        TreeMap<Integer,String> points = new TreeMap<>();

        try (Connection con = DriverManager.getConnection(dbUrl, user, password)) {
            Statement stmt=con.createStatement();
            ResultSet rs=stmt.executeQuery(queryGetSites);
            while (rs.next()){
                points.put(rs.getInt(rs.findColumn("point_id")),rs.getString(rs.findColumn("point_name")));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return points;
    }



    public static void insertHistories(int pointId,ArrayList<String> histories){

    }


    public static void sqlCon(TreeMap<String,String> his){
        String queryInsertHis = " insert into table2 (name, value)" + " values (?, ?)";
        try (Connection con = DriverManager.getConnection(dbUrl, user, password)) {
            PreparedStatement preparedStmt = con.prepareStatement(queryInsertHis);
            for(Map.Entry<String,String> entry : his.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                preparedStmt.setString (1, key);
                preparedStmt.setString (2, value);
                preparedStmt.execute();
            }
        } catch (SQLException ex) {
            Logger lgr = Logger.getLogger(Main.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        }
    }
}
