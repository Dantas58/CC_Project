import java.util.*;
import java.util.stream.Collectors;
import java.net.*;
import java.io.*;

public class FS_Tracker {

    private Map<String, Map<String, List<Integer>>> fs_nodes;
    private int port;
    private String local_address;

    public FS_Tracker() {

        this.fs_nodes = new HashMap<>();
        this.port = 9090;
        try {
            this.local_address = InetAddress.getByName(getLocalAddress()).getHostName();
        } catch (SocketException | UnknownHostException e) {
            e.printStackTrace();
        };
    }

    private void registerNode(Track_Packet infos) {

        String node_address = infos.getNodeAddress();
        Map<String, List<Integer>> files = infos.getFiles();

        fs_nodes.put(node_address, files);
        System.out.println("Node " + node_address + " Registered;");
    }

    private void updateNode(Track_Packet infos) {

        String node_address = infos.getNodeAddress();
        Map<String, List<Integer>> files = infos.getFiles();

        if (fs_nodes.containsKey(node_address)) {

            fs_nodes.put(node_address, files);

        } else {
        }
    }

    private Map<String, List<Integer>> getNodes(Track_Packet infos) {

        Map<String, List<Integer>> files = infos.getFiles();
        String file_name = files.keySet().iterator().next();

        return fs_nodes.entrySet().stream()
                                  .filter(entry -> entry.getValue().containsKey(file_name))
                                  .collect(Collectors.toMap(
                                                            Map.Entry::getKey,
                                                            entry -> entry.getValue().get(file_name)));
    }

    private byte[] sendNodes(Map<String, List<Integer>> node_list) throws IOException {

        String address = this.local_address;
        Track_Packet packet = new Track_Packet("LIST", address, node_list);

        byte[] packet_ready = packet.packUp();
        return packet_ready;
    }

    private void start() throws IOException {

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("FS_Tracker is running. Listening on port " + port);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                new ServerThread(clientSocket).start();
            }
        }
    }

    private String getLocalAddress() throws SocketException {
        Enumeration<NetworkInterface> iterNetwork = NetworkInterface.getNetworkInterfaces();
        NetworkInterface network;
        InetAddress address;

        while (iterNetwork.hasMoreElements()) {
            network = iterNetwork.nextElement();

            if (!network.isUp())
                continue;

            if (network.isLoopback())
                continue;

            Enumeration<InetAddress> iterAddress = network.getInetAddresses();

            while (iterAddress.hasMoreElements()) {
                address = iterAddress.nextElement();

                if (address.isAnyLocalAddress())
                    continue;

                if (address.isLoopbackAddress())
                    continue;

                if (address.isMulticastAddress())
                    continue;

                return address.getHostName();
            }
        }

        throw new SocketException("No suitable network interface found");
    }


    class ServerThread extends Thread {

        private Socket socket;
        private ObjectInputStream in;
        private ObjectOutputStream out;

        public ServerThread(Socket socket) {

            this.socket = socket;
        }

        @Override
        public void run() {

            try {

                in = new ObjectInputStream(socket.getInputStream());
                out = new ObjectOutputStream(socket.getOutputStream());
                out.flush();

                while (true) {

                    byte[] received_packet = (byte[]) in.readObject();
                    Track_Packet final_packet = Track_Packet.unpack(received_packet);

                    String command = final_packet.getCommand();

                    if (command.toUpperCase().equals("REGISTER")) {

                        registerNode(final_packet);
                        out.flush();
                    }

                    else if (command.toUpperCase().equals("UPDATE")) {

                        updateNode(final_packet);
                        for (String key : final_packet.getFiles().keySet()) {
                        }
                        out.flush();
                    }

                    else if (command.toUpperCase().equals("GET")) {

                        Map<String, List<Integer>> packet = getNodes(final_packet);
                        byte[] packet_ready = sendNodes(packet);
                        out.writeObject(packet_ready);
                        out.flush();
                    }

                    else if (command.toUpperCase().equals("EXIT")) {

                        System.out
                                .println("Terminating connection with Node ( " + final_packet.getNodeAddress() + " );");
                        fs_nodes.remove(final_packet.getNodeAddress());
                        in.close();
                        out.close();
                        socket.close();
                        break;
                    }

                }

            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException { // porque IOException?!
        FS_Tracker fs_Tracker = new FS_Tracker();
        fs_Tracker.start();
    }
}
