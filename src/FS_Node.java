import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.CRC32;
import java.io.*;

public class FS_Node {

    private static final int BLOCK_SIZE = 100;
    private final String directory;
    private final String server_address;
    private final int server_port;
    private String node_address;

    private Map<String, List<Integer>> files;

    private ObjectInputStream in;
    private ObjectOutputStream out;

    private Socket tcp_socket;
    private DatagramSocket udp_socket;

    private volatile boolean running = true;

    public FS_Node(int server_port, String server_address, String directory, String node_address) {

        this.server_address = server_address;
        this.server_port = server_port;
        this.directory = directory;
        this.node_address = node_address;
        // tcp_socket = new Socket(server_address, server_port);
        // this.node_address = InetAddress.getLocalHost().getHostAddress(); // + ":" +
        // String.valueOf(TCP_Port);
    }

    public class FileBlock {

        private Integer Id;
        private byte[] data;

        public FileBlock(Integer blockId, byte[] data) {
            this.Id = blockId;
            this.data = data;
        }

        public Integer getBlockId() {
            return Id;
        }

        public byte[] getData() {
            return data;
        }
    }

    private FileBlock findBlock(List<FileBlock> blocks, int id) {
        return blocks.stream()
            .filter(block -> block.getBlockId().equals(id))
            .findFirst()
            .orElse(null);
    }

    public String getAddress() throws IOException {

        return this.node_address;
    }

    private void register() throws IOException {

        String address = getAddress();

        this.files = readFilesToMap(directory);
        Track_Packet packet = new Track_Packet("REGISTER", address, this.files);
        byte[] packet_ready = packet.packUp();
        out.writeObject(packet_ready);
        out.flush();

        System.out.println("Node has been Registered in FS Tracker;\n");
    }

    private void update() throws IOException {

        String address = getAddress();
        this.files = readFilesToMap(directory);

        Track_Packet packet = new Track_Packet("UPDATE", address, this.files);

        byte[] packet_ready = packet.packUp();
        out.writeObject(packet_ready);
        out.flush();
    }

    private void updateBlocks() throws IOException{

        String address = getAddress();
        
        Track_Packet packet = new Track_Packet("UPDATE", address, this.files);

        byte[] packet_ready = packet.packUp();
        out.writeObject(packet_ready);
        out.flush();

    }

    private void get(String file_name) throws IOException {

        String address = getAddress();

        Map<String, List<Integer>> files_just_name = new HashMap<>();
        files_just_name.put(file_name, new ArrayList<Integer>());
        Track_Packet packet = new Track_Packet("GET", address, files_just_name);

        byte[] packet_ready = packet.packUp();
        out.writeObject(packet_ready);
        out.flush();
    }

    private void exit() throws IOException {

        String address = getAddress();
        running = false;

        // Use a placeholder as there will be no need to send actual file information
        Map<String, List<Integer>> placeHolder = new HashMap<>();
        placeHolder.put("placeHolder", new ArrayList<>());

        Track_Packet packet = new Track_Packet("EXIT", address, placeHolder);
        byte[] packetReady = packet.packUp();
        
        out.writeObject(packetReady);
        out.flush();

        tcp_socket.close();

        if (udp_socket != null && !udp_socket.isClosed()) {
            udp_socket.close();
        }

        in.close();
        out.close();
    }
    private void send(String address, String file_name, int block_id, int total_blocks, boolean is_request){
        try {
            DatagramSocket socket = new DatagramSocket();
            InetAddress address_final = InetAddress.getByName(address);
            Transfer_Packet packet;

            if(is_request){
                // It's a request == no need for block data to be sent + checksum -1 to represent a request 
                packet = new Transfer_Packet(file_name, block_id, new byte[0], total_blocks, -1);
            }
            else{
                System.err.println("Sending block " + block_id + " of file " + file_name);
                RandomAccessFile raf = new RandomAccessFile(new File(directory, file_name), "r");
                raf.seek(block_id * BLOCK_SIZE);
                byte[] block_data = new byte[BLOCK_SIZE];
                int bytesRead = raf.read(block_data);
                if (bytesRead < BLOCK_SIZE) {
                    block_data = Arrays.copyOf(block_data, bytesRead);
                }
                raf.close();
                long checksum = calcChecksum(block_data);
                packet = new Transfer_Packet(file_name, block_id, block_data, total_blocks, checksum);
            }

            byte[] packet_ready = packet.packUpTransfer();

            DatagramPacket packet_final = new DatagramPacket(packet_ready, packet_ready.length, address_final, 9090);

            socket.send(packet_final);
            socket.close();

        } catch (Exception e) {
            System.err.println("Error sending UDP packet: " + e.getMessage());
        }
    }

    private void saveBlock(Transfer_Packet packet) throws IOException { 
        String file_name = packet.getFileName();
        byte[] block_data = packet.getBlockData();
        int block_id = packet.getBlockId();

        if (this.files.containsKey(file_name) && this.files.get(file_name).contains(block_id)) {
            System.err.println("Block " + block_id + " of file " + file_name + " already exists");
            return;
        }

        RandomAccessFile raf = new RandomAccessFile(new File(directory, file_name), "rw");
        raf.seek(block_id * BLOCK_SIZE);
        raf.write(block_data);
        raf.close();

        if (!this.files.containsKey(file_name)) {
            this.files.put(file_name, new ArrayList<>());
        }
        this.files.get(file_name).add(block_id);
        updateBlocks();
    }


    public Map<String, List<Integer>> readFilesToMap(String directoryPath) {
        File directory = new File(directoryPath);
        Map<String, List<Integer>> fileBlocksMap = new HashMap<>();

        File[] files = directory.listFiles();

        if (files != null) {
            for (File file : files) {
                if (file.isFile()) {
                    String fileName = file.getName();

                    try (FileInputStream fileInputStream = new FileInputStream(file)) {
                        int blockId = 0;
                        int bufferSize = 100;
                        byte[] buffer = new byte[bufferSize];

                        List<Integer> blockIds = new ArrayList<>();

                        while (fileInputStream.read(buffer) != -1) {
                            blockIds.add(blockId);
                            blockId++;
                        }

                        fileBlocksMap.put(fileName, blockIds);

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        return fileBlocksMap;
    }

    private long calcChecksum(byte[] data) {
        CRC32 crc32 = new CRC32();
        crc32.update(data);
        return crc32.getValue();
    }

    private void handlePacket(DatagramPacket packet) throws IOException {

        Transfer_Packet received_packet = Transfer_Packet.unpackTransfer(packet.getData());

        String address = packet.getAddress().getHostAddress();
        String file_name = received_packet.getFileName();
        int block_id = received_packet.getBlockId();
        int total_blocks = received_packet.getTotalBlocks();

        if (received_packet.getChecksum() == -1) {
            // This is a request packet
            send(address, file_name, block_id, total_blocks, false);
        } else if (received_packet.getChecksum() == calcChecksum(received_packet.getBlockData())) {
            // This is a data packet and the checksum is correct
            saveBlock(received_packet);
        } else {
            // This is a data packet but the checksum is incorrect
            System.err.println("Checksum mismatch for block " + received_packet.getBlockId());
            send(address, file_name, block_id, total_blocks, true);
        }
    }

    private void setupPeer() {

        try {
            udp_socket = new DatagramSocket(9090);
            System.out.println("FS_Transfer Protocol: Listening on Port 9090;");
            new Thread(() -> {
                while (running) {
                    byte[] buffer = new byte[2048];
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    try {
                        udp_socket.receive(packet);
                        new Thread(() -> {
                            try {
                                handlePacket(packet);
                            } catch (IOException e) {
                                System.err.println("Error handling received packet: " + e.getMessage());
                                e.printStackTrace();
                            }
                        }).start();
                    } catch (IOException e) {
                        if(running)
                            System.err.println("Error receiving UDP packet: " + e.getMessage());
                    }
                }
            }).start();
            
        } catch (SocketException e) {
            System.err.println("Error opening UDP Socket: " + e.getMessage());
        }
    }

    private void setupTrackerConnection() throws IOException, ClassNotFoundException {

        tcp_socket = new Socket(server_address, server_port);
        out = new ObjectOutputStream(tcp_socket.getOutputStream());
        in = new ObjectInputStream(tcp_socket.getInputStream());

        System.out.println(
                "FS Track Protocol connection established with server " + server_address + " on port " + server_port);

        register();
    }

    private void commandHandler() throws IOException, ClassNotFoundException {

        try (Scanner scanner = new Scanner(System.in)) {
            while (running) {

                System.out.println("Waiting for a command:");
                String command = (scanner.nextLine()).toUpperCase();

                switch (command) {

                    case "UPDATE":{

                        update();
                        break;
                    }
                    
                    case "GET": {

                        System.out.println("Choose file to locate:");
                        String file_name = scanner.nextLine();
                        get(file_name);

                        byte[] received_packet = (byte[]) in.readObject();
                        Track_Packet final_packet = Track_Packet.unpack(received_packet);

                        if (final_packet.getFiles().isEmpty())
                            System.out.println("Specified file could not be found in any registered Node;");

                        else {
                            for (String key : final_packet.getFiles().keySet()) {
                                System.out.println(key + ": " + final_packet.getFiles().get(key) + " blocks");
                            }
                        }

                        break;
                    }

                    case "TRANSFER": {
                        System.out.println("Choose file to transfer: ");
                        String file_name = scanner.nextLine();

                        get(file_name);

                        byte[] received_packet = (byte[]) in.readObject();
                        Track_Packet final_packet = Track_Packet.unpack(received_packet);

                        if (final_packet.getFiles().isEmpty())
                            System.out.println("Specified file could not be found in any registered Node;");
                        else {
                            int total_ids = final_packet.getFiles().values().stream()
                                .mapToInt(List::size)
                                .max()
                                .orElse(0);  // default value if no maximum is found

                            for (int id = 0; id < total_ids; id++) {
                                if(this.files.containsKey(file_name) && this.files.get(file_name).contains(id))
                                    continue;
                                for (Map.Entry<String, List<Integer>> entry : final_packet.getFiles().entrySet()) {
                                    if (entry.getValue().contains(id)) {
                                        String address = entry.getKey();
                                        try {
                                            send(address, file_name, id, total_ids, true);
                                        } catch (Exception e) {
                                            System.err.println("Error sending block " + id + " of file " + file_name + ": " + e.getMessage());
                                        }
                                        break;
                                    }
                                }
                            }
                        }

                        break;
                    }

                    case "EXIT": {

                        exit();
                        return;
                    }

                    default:

                        System.out.println("Error: command is not valid;");
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException {

        if (args.length < 4) {
            System.out.println("Not enough arguments");
            return;
        }

        String directory = args[0];
        String server_address = args[1];
        int server_port = Integer.parseInt(args[2]);
        String node_address = args[3];

        FS_Node node = new FS_Node(server_port, server_address, directory,node_address);

        node.setupTrackerConnection();
        node.setupPeer();
        node.commandHandler();
    }
}
