import org.json.JSONArray;
import org.json.JSONObject;

public class Message {

    private final String status; //required
    private final String message; //required
    private JSONArray data = new JSONArray(); //optional


    public Message(String status, String message) {
        this.status = status;
        this.message = message;
    }

    public void addData(JSONArray data){
        this.data = data;
    }


    public String getStatus() {
        return status;
    }

    public String getMessage() {
        return message;
    }


    public JSONObject createJson(){

        JSONObject jsObj = new JSONObject();
        jsObj.put("status", this.status);
        jsObj.put("message", this.message);
        jsObj.put("data", this.data);
        return jsObj;
    }
}
