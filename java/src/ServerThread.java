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

import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * Worker thread for the Server class.
 * Contains the dispatch table for the commands received
 * from the PHP backend.
 * @author lenny
 */
public class ServerThread extends Thread {

    private final Socket socket;
    private final BufferedReader in;
    private final PrintWriter out;
    private final ServerCommands serverCommands;

    public ServerThread(Socket socket) throws IOException {

        this.socket = socket;
        this.socket.setSoLinger(false, 0);

        this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.out = new PrintWriter(socket.getOutputStream(), false);
        this.serverCommands = new ServerCommands(this);
    }

    public void run() {

        try {

            String line;

            while ((line = in.readLine()) != null) {

                JSONObject mainObj= new JSONObject(line);
                String action = mainObj.getString("action");
                JSONObject dataObj = mainObj.getJSONObject("data");

                switch(action) {
                    case "connect":
                        serverCommands.connect(dataObj);
                        break;
                    case "execute":
                        serverCommands.execute(dataObj);
                        break;
                    case "fetch":
                        serverCommands.fetch(dataObj);
                        break;
                    case "fetch_array":
                        serverCommands.fetch_array(dataObj);
                        break;
                    case "free_result":
                        serverCommands.free_result(dataObj);
                        break;
                    default:
                        Message msg = new Message("error", "action was not provided");
                        this.write(msg.createJson().toString());
                        break;
                }

                out.flush();
            }

            serverCommands.close();
            socket.close();

        } catch (IOException e) {
            Message msg = new Message("error", e.getMessage());
            this.write(msg.createJson().toString());
        }
    }

    public void write(String s) {
        out.println(s);
    }

}
