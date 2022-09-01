package org.projecthaystack.ob;

import org.projecthaystack.HGrid;
import org.projecthaystack.client.HClient;

import java.util.*;

public class Axon {

    public ArrayList<String[]> getEquip(HClient client){
        String call = "readAll(equip)";
        try{
            HGrid grid = client.eval(call);
            ArrayList<String[]> equip = new ArrayList<>();
            Scanner scanner = new Scanner(grid.toString());
            String header = "";
            if (scanner.hasNext()) {
                scanner.nextLine();
            }
            if (scanner.hasNext()) {
                header = scanner.nextLine();
            }
            System.out.println(header);
            String[] tempRow;
            equip.add(header.split(","));
            while (scanner.hasNext()){
                tempRow = scanner.nextLine().split(",");
                equip.add(tempRow);
            }
            return equip;
        }catch(Exception e) {
            System.out.println(e);
            return new ArrayList<>();

        }
    }
    public ArrayList<String[]> getSensors(HClient client){
        String call = "readAll(sensor)";
        try{
            HGrid grid = client.eval(call);
            ArrayList<String[]> equip = new ArrayList<>();
            Scanner scanner = new Scanner(grid.toString());
            String header = "";
            if (scanner.hasNext()) {
                scanner.nextLine();
            }
            if (scanner.hasNext()) {
                header = scanner.nextLine();
            }
            System.out.println(header);
            String[] tempRow;
            equip.add(header.split(","));
            while (scanner.hasNext()){
                tempRow = scanner.nextLine().split(",");
                equip.add(tempRow);
            }
            return equip;
        }catch(Exception e) {
            System.out.println(e);
            return new ArrayList<>();

        }
    }


    public ArrayList<String> getRules(HClient client){
        String call = "readAll(rule and not disabled)";
        HGrid grid =  client.eval(call);
        return getColumn(grid,"id");
    }

    public ArrayList<String[]> getSparks(HClient client, String rule,String date){
        String call = "ruleSparks(ruleToTargets(readAll(dis==\""+rule+"\" and rule)[0]),\""+date+"\")";
        try{
            HGrid grid = client.eval(call);
            ArrayList<String[]> sparks = new ArrayList<>();
            Scanner scanner = new Scanner(grid.toString());
            String header = "";
            if (scanner.hasNext()) {
                scanner.nextLine();
            }
            if (scanner.hasNext()) {
                header = scanner.nextLine();
            }
            System.out.println(header);
            String[] tempRow;
            while (scanner.hasNext()){
                tempRow = scanner.nextLine().split(",");
                sparks.add(tempRow);
            }
            return sparks;
        }catch(Exception e) {
            System.out.println(e);
            return new ArrayList<>();

        }

    }

    public ArrayList<String> getSiteId (HClient client){
        String call = "readAll(site)";
        HGrid grid =  client.eval(call);
        return getColumn(grid,"id");
    }

    public ArrayList<String[]> getPoints(HClient client, String siteid) {
        String call = "readAll(point and siteRef == "+siteid+")";
        try{
            HGrid grid = client.eval(call);
            ArrayList<String[]> point = new ArrayList<>();
            Scanner scanner = new Scanner(grid.toString());
            String header = "";
            if (scanner.hasNext()) {
                scanner.nextLine();
            }
            if (scanner.hasNext()) {
                header = scanner.nextLine();
            }
            System.out.println(header);
            String[] tempRow;
            point.add(header.split(","));
            while (scanner.hasNext()){
                tempRow = scanner.nextLine().split(",");
                point.add(tempRow);
            }
            return point;
        }catch(Exception e) {
            System.out.println(e);
            return new ArrayList<>();

        }
    }

    private ArrayList<String> getColumn(HGrid grid,String col){
        Scanner scanner = new Scanner(grid.toString());

        String header = "";
        //First line is the version of the file and not required
        if (scanner.hasNext()) {
            scanner.nextLine();
        }
        //The second line is the header for the file
        if (scanner.hasNext()) {
            header = scanner.nextLine();
        }
        String[] headerList = header.split(",");
        ArrayList<String> headers = new ArrayList<>(Arrays.asList(headerList));
        int colPos = headers.indexOf(col);
        String line;
        ArrayList<String> cols = new ArrayList<>();
        while (scanner.hasNext() && colPos >= 0) {
            line = scanner.nextLine();
            String[] lineList = line.split(",");
            try {
                cols.add(lineList[colPos]);
            } catch (ArrayIndexOutOfBoundsException ignored) {
            }
        }
        return cols;
    }
    /*private TreeMap<String,String> getHisData(HGrid grid){
        Scanner scanner = new Scanner(grid.toString());

        String version = "";
        String header = "";
        //First line is the version of the file and not required
        if (scanner.hasNext()) {
            version = scanner.nextLine();
        }
        //The second line is the header for the file
        if (scanner.hasNext()) {
            header = scanner.nextLine();
        }
        String line = "";
        TreeMap<String,String> his = new TreeMap<>();
        while (scanner.hasNext()){
            line = scanner.nextLine();
            String lineList[] = line.split(",");
            String time = lineList[0];
            String val = lineList[1];
            his.put(time,val);
        }
        System.out.println(his.containsKey("2020-07-12T23:45:00+01:00 London"));
        return his;
    }*/
}
