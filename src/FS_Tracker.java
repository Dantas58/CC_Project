import java.util.*;
import java.net.*;
import java.io.*;

public class FS_Tracker {
    
    private Map<String, Map<String, List<byte[]>>> fs_nodes;
    private int port;

    public FS_Tracker(){
        
        this.fs_nodes = new HashMap<>();
        this.port = 9090;
    }

    public void registerNode(Protocol infos){

        String node_address = infos.getNodeAddress();
        Map<String,List<byte[]>> files = infos.getFiles();

        fs_nodes.put(node_address, files);
        System.out.println("Node " + node_address + " Registered;");
    }

    public void updateNode(Protocol infos){

        String node_address = infos.getNodeAddress();
        Map<String,List<byte[]>> files = infos.getFiles();

        if(fs_nodes.containsKey(node_address)){

            fs_nodes.put(node_address, files);
            System.out.println("Node " + node_address + " Updated;");
        }
        else{
            System.out.println("Specified Node (" + node_address + ") does not exist;");
        }
    }


    //public Map<String, Map<String, List<byte[]>>> getNodes(Protocol infos){
    public Map<String, List<byte[]>> getNodes(Protocol infos){

        Map<String,List<byte[]>> files = infos.getFiles();
      
        String file_name = files.keySet().iterator().next();

        //Map<String, Map<String, List<byte[]>>> node_list = new HashMap<>();
        Map<String, List<byte[]>> node_list = new HashMap<>();

        for (String key : fs_nodes.keySet()) {

            if(fs_nodes.get(key).containsKey(file_name)){
                //Map<String, List<byte[]>> node_blocks = fs_nodes.get(key); // mudar o tipo da list
                List<byte[]> node_blocks = fs_nodes.get(key).get(file_name);
                //node_list.put(key, node_blocks);
                node_list.put(key, node_blocks);
            }
        }

        return node_list;
    }

    public byte[] sendNodes(Map<String, List<byte[]>> node_list) throws IOException{

        String address = InetAddress.getLocalHost().getHostAddress() + ":" + String.valueOf(port);
        Protocol packet = new Protocol("LIST", address, node_list);

        byte[] packet_ready = packet.packUp();
        return packet_ready;
    }

    public void start() throws IOException {
        
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("FS_Tracker is running. Listening on port " + port);

        while (true) {
            Socket clientSocket = serverSocket.accept();
            new ServerThread(clientSocket).start();
        }
    }
    

    class ServerThread extends Thread{

        private Socket socket;
        private ObjectInputStream in;
        private ObjectOutputStream out;

        public ServerThread(Socket socket){

            this.socket = socket;
        }

        @Override
        public void run(){

            try {

                in = new ObjectInputStream(socket.getInputStream());
                out = new ObjectOutputStream(socket.getOutputStream());
                out.flush();

                // tentar receber primeiro o tamanho para criar um byte[tamanho_do_packet];
                //int packet_size = (int) in.readObject();
                //byte[] received_packet = new byte[packet_size];
                //byte received_packet = (byte[]) in.readObject();

                while(true){

                    byte[] received_packet = (byte[]) in.readObject();
                    Protocol final_packet = Protocol.unpack(received_packet);

                    String command = final_packet.getCommand();

                    if(command.toUpperCase().equals("REGISTER")){

                        registerNode(final_packet);
                        out.flush();
                    }

                    else if(command.toUpperCase().equals("UPDATE")){

                        updateNode(final_packet);
                        out.flush();
                    }

                    else if(command.toUpperCase().equals("GET")){

                        Map<String, List<byte[]>> pakcet = getNodes(final_packet);
                        byte[] packet_ready = sendNodes(pakcet);
                        out.writeObject(packet_ready);
                        out.flush();
                    }

                    else if(command.toUpperCase().equals("EXIT")){

                        System.out.println("Terminating connection between FS Tracker and Node ( " + final_packet.getNodeAddress() + " );");
                        in.close();
                        out.close();
                        socket.close();
                    }
            
                }

            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }        
        }
    }
    
    public static void main(String [] args) throws IOException{ // porque IOException?!
        
        FS_Tracker fs_Tracker = new FS_Tracker();
        fs_Tracker.start();
    }

}



