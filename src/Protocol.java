import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class Protocol {
    private String command;
    private String nodeAddress;
    private Map<String, Integer> files;

    public Protocol(String command, String nodeAddress, Map<String, Integer> files) {
        this.command = command;
        this.nodeAddress = nodeAddress;
        this.files = files;
    }

    public String getCommand(){
        return command;
    }

    public String getNodeAddress(){
        return nodeAddress;
    }

    public Map<String, Integer> getFiles(){
        return files;
    }

    public void setCommand(String command){
        this.command = command;
    }

    public void setNodeAddress(String nodeAddress){
        this.nodeAddress = nodeAddress;
    }

    public void setFiles(Map<String, Integer> files){
        this.files = files;
    }



    public byte[] packUp() throws IOException{

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        byte[] commandBytes = command.getBytes();
        int commandSize = commandBytes.length;  
        byteArrayOutputStream.write(ByteBuffer.allocate(4).putInt(commandSize).array());
        byteArrayOutputStream.write(commandBytes);

        byte[] nodeAddressBytes = nodeAddress.getBytes();
        int nodeAddressSize = nodeAddressBytes.length;
        byteArrayOutputStream.write(ByteBuffer.allocate(4).putInt(nodeAddressSize).array());
        byteArrayOutputStream.write(nodeAddressBytes);

        int fileCount = files.size();
        byteArrayOutputStream.write(ByteBuffer.allocate(4).putInt(fileCount).array());

        for (Map.Entry<String, Integer> entry : files.entrySet()) {
            String fileName = entry.getKey();
            int blockCount = entry.getValue();

            byte[] fileNameBytes = fileName.getBytes();
            int fileNameSize = fileNameBytes.length;
            byteArrayOutputStream.write(ByteBuffer.allocate(4).putInt(fileNameSize).array());
            byteArrayOutputStream.write(fileNameBytes);

            byteArrayOutputStream.write(ByteBuffer.allocate(4).putInt(blockCount).array());
        }

        return byteArrayOutputStream.toByteArray();
    }

    public static Protocol unpack(byte[] serializedData) throws IOException{

        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(serializedData);
        DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);

        int commandSize = dataInputStream.readInt();
        byte[] commandBytes = new byte[commandSize];
        dataInputStream.readFully(commandBytes);
        String command = new String(commandBytes);

        int nodeAddressSize = dataInputStream.readInt();
        byte[] nodeAddressBytes = new byte[nodeAddressSize];
        dataInputStream.readFully(nodeAddressBytes);
        String nodeAddress = new String(nodeAddressBytes);

        int fileCount = dataInputStream.readInt();

        Map<String, Integer> files = new HashMap<>();

        for (int i = 0; i < fileCount; i++) {
            int fileNameSize = dataInputStream.readInt();
            byte[] fileNameBytes = new byte[fileNameSize];
            dataInputStream.readFully(fileNameBytes);
            String fileName = new String(fileNameBytes);
    
            int blockCount = dataInputStream.readInt();
    
            files.put(fileName, blockCount);
        }

        Protocol unpacked_data = new Protocol(command, nodeAddress, files);
        return unpacked_data;
    }

}

    /* 
    public static void main(String[] args) throws IOException {
        // Serialize a Protocol object
        Protocol originalProtocol = new Protocol("REGISTER", "192.168.1.1:12345", Map.of("file1.txt", List.of("File 1 Data".getBytes())));
        byte[] serializedPacket = originalProtocol.packUp();

        // Deserialize the packet
        Protocol deserializedProtocol = Protocol.unpack(serializedPacket);

        // Access the deserialized data
        System.out.println("Deserialized Command: " + deserializedProtocol.getCommand());
        System.out.println("Deserialized Node Address: " + deserializedProtocol.getNodeAddress());
        
        for (Map.Entry<String, List<byte[]>> entry : deserializedProtocol.files.entrySet()){
            String fileName = entry.getKey();
            System.out.println(fileName);
        }
    }
}
*/
