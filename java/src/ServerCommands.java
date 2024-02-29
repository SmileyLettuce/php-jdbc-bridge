/*
 * Copyright (C) 2007 lenny@mondogrigio.cjb.net
 *
 * This file is part of PJBS (http://sourceforge.net/projects/pjbs)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Hashtable;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * This class is instantiated by the ServerThread class.
 * Contains the commands implementations.
 * @author lenny
 */
public class ServerCommands {

    private final ServerThread serverThread;
    private Connection conn = null;
    Hashtable<String, ResultSet> results = new Hashtable<String, ResultSet>();

    /** Creates a new instance of ServerCommands */
    public ServerCommands(ServerThread serverThread) {

        this.serverThread = serverThread;
    }

    /**
     * Connect to a JDBC data source.
     * @param dataObj JSONObject[]
     */
    public void connect(JSONObject dataObj) {

        String dsn = dataObj.getString("dsn");
        String user = dataObj.getString("user");
        String pass = dataObj.getString("pass");

        try {
            conn = DriverManager.getConnection(dsn, user, pass);
            Message msg = new Message("success", "connected to server");
            serverThread.write(msg.createJson().toString());

        } catch (SQLException e) {

            Message msg = new Message("error", Utils.formatSQLErrorMessage(e));
            serverThread.write(msg.createJson().toString());
        }
    }


    /**
     * Execute an SQL query.
     * @param dataObj JSONObject
     */
    public void execute(JSONObject dataObj) {

        try {

            String query = dataObj.getString("query");

            PreparedStatement st = conn.prepareStatement(query);
            st.setFetchSize(1);

            if(dataObj.has("params")){

                JSONArray paramsArr = dataObj.getJSONArray("params");
                if(!paramsArr.isEmpty()) {
                    for (int i = 0; i < paramsArr.length(); i++) {
                        String paramVal = paramsArr.getString(i);
                        st.setString(i+1, paramVal);
                    }
                }
            }

            if (st.execute()) {
                String resultId = Utils.makeUID();
                results.put(resultId, st.getResultSet());

                Message msg = new Message("success",resultId);
                serverThread.write(msg.createJson().toString());

            } else {
                Message msg = new Message("success", st.getUpdateCount() + " rows updated");
                serverThread.write(msg.createJson().toString());
            }

        } catch (SQLException e) {

            Message msg = new Message("error", Utils.formatSQLErrorMessage(e));
            serverThread.write(msg.createJson().toString());
        }
    }


    /**
     * Fetch results from a result_id ResultSet.
     * @param dataObj JSONObject
     */
    public void fetch_array(JSONObject dataObj) {

        try {

            String resultId = dataObj.getString("result_id");

            ResultSet rs = results.get(resultId);

            if (rs != null) {

                JSONArray dataArr = new JSONArray();

                while (rs.next()) {

                    ResultSetMetaData rsmd = rs.getMetaData();
                    JSONObject lineObj = new JSONObject();

                    for (int i = 1; i <= rsmd.getColumnCount(); i++) {

                        String colVal = rs.getString(i);
                        lineObj.put(rsmd.getColumnName(i), colVal == null ? JSONObject.NULL : colVal);
                    }

                    dataArr.put(lineObj);
                }

                Message msg = new Message("success","");
                msg.addData(dataArr);
                serverThread.write(msg.createJson().toString());

            }else{

                Message msg = new Message("error", "result set is null");
                serverThread.write(msg.createJson().toString());
            }

        }catch (SQLException e){
            Message msg = new Message("error", Utils.formatSQLErrorMessage(e));
            serverThread.write(msg.createJson().toString());
        }

        //we have reached the end of the object and we can remove it from memory
        this.free_result(dataObj);
    }


    /**
     * Fetch results from a result_id ResultSet.
     * @param dataObj JSONObject
     */
    public void fetch(JSONObject dataObj) {

        try {

            String resultId = dataObj.getString("result_id");

            ResultSet rs = results.get(resultId);

            if (rs != null) {

                JSONArray dataArr = new JSONArray();

                if (rs.next()) {

                    ResultSetMetaData rsmd = rs.getMetaData();
                    JSONObject lineObj = new JSONObject();

                    for (int i = 1; i <= rsmd.getColumnCount(); i++) {

                        String colVal = rs.getString(i);
                        lineObj.put(rsmd.getColumnName(i), colVal == null ? JSONObject.NULL : colVal);
                    }

                    dataArr.put(lineObj);

                    Message msg = new Message("success","");
                    msg.addData(dataArr);
                    serverThread.write(msg.createJson().toString());
                }

            }else{

                Message msg = new Message("error", "result set is null");
                serverThread.write(msg.createJson().toString());

                //we have reached the end of the object and we can remove it from memory
                this.free_result(dataObj);
            }

        }catch (SQLException e){
            Message msg = new Message("error", Utils.formatSQLErrorMessage(e));
            serverThread.write(msg.createJson().toString());
        }

    }


    /**
     * Free results from a result_id ResultSet.
     * @param dataObj JSONObject
     */
    public void free_result(JSONObject dataObj) {

        String resultId = dataObj.getString("result_id");
        if (resultId == null) {
            Message msg = new Message("error", "result_id not found");
            serverThread.write(msg.createJson().toString());
        }

        try {
            results.remove(resultId);
            Message msg = new Message("success",resultId + ": results were removed");
            serverThread.write(msg.createJson().toString());
        }
        catch (NullPointerException e){
            Message msg = new Message("error", e.getMessage());
            serverThread.write(msg.createJson().toString());
        }

    }


    /**
     * Release the JDBC connection.
     */
    public void close() {

        if (conn != null) {

            try {
                conn.close();

            } catch (SQLException e) {
                Message msg = new Message("error", Utils.formatSQLErrorMessage(e));
                serverThread.write(msg.createJson().toString());
            }
        }
    }

}
