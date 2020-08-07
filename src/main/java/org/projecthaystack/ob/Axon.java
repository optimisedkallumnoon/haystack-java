package org.projecthaystack.ob;

import org.projecthaystack.HGrid;
import org.projecthaystack.client.HClient;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

public class Axon {
    public ArrayList<String> getSites(HClient client) {
        HGrid grid = client.eval("site");
        return getColumn(grid,"dis");
    }

    public ArrayList<String> getEquip(HClient client,String siteName){
        HGrid grid = client.eval("readAll(siteRef->dis== "+ siteName + " and equip)");
        return getColumn(grid,"navName");
    }

    public ArrayList<String> getPoints(HClient client,String siteName,String equipName){
        HGrid grid = client.eval("readAll(siteRef->dis==" + siteName + " and equipRef->navName==" + equipName +")");
        return getColumn(grid,"navName");
    }

    public TreeMap<String,String> getHis(HClient client,String siteName,String equipName, String pointName,String period){
        String call = "readAll(siteRef->dis==" + siteName + " and equipRef->navName==" + equipName +" and navName=="+pointName+").hisRead(lastYear)";
        HGrid grid = client.eval("readAll(siteRef->dis==" + siteName + " and equipRef->navName==" + equipName +" and navName=="+pointName+").hisRead("+period+")");
        return getHisData(grid);
    }

    private ArrayList<String> getColumn(HGrid grid,String col){
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
        String headerList[] = header.split(",");
        ArrayList<String> headers = new ArrayList<>(Arrays.asList(headerList));
        headers.contains(col);
        int colPos = headers.indexOf(col);
        String line = "";
        ArrayList<String> cols = new ArrayList<>();
        while (scanner.hasNext() && colPos >= 0) {
            line = scanner.nextLine();
            String lineList[] = line.split(",");
            try {
                cols.add(lineList[colPos]);
            } catch (ArrayIndexOutOfBoundsException ex) {
            }
        }
        return cols;
    }
    private TreeMap<String,String> getHisData(HGrid grid){
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
    }
}
