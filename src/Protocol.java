import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class Protocol {
    private String command;
    private String nodeAddress;
    private Map<String, List<byte[]>> files;

    public Protocol(String command, String nodeAddress, Map<String, List<byte[]>> files) {
        this.command = command;
        this.nodeAddress = nodeAddress;
        this.files = files;
    }

    public String getCommand() {
        return command;
    }

    public String getNodeAddress() {
        return nodeAddress;
    }

    public Map<String, List<byte[]>> getFiles() {
        return files;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public void setNodeAddress(String nodeAddress) {
        this.nodeAddress = nodeAddress;
    }

    public void setFiles(Map<String, List<byte[]>> files) {
        this.files = files;
    }



    public byte[] packUp() throws IOException {

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        // Add command size and command
        byte[] commandBytes = command.getBytes();
        int commandSize = commandBytes.length;
        byteArrayOutputStream.write(ByteBuffer.allocate(4).putInt(commandSize).array());
        byteArrayOutputStream.write(commandBytes);

        // Add node address size and node address
        byte[] nodeAddressBytes = nodeAddress.getBytes();
        int nodeAddressSize = nodeAddressBytes.length;
        byteArrayOutputStream.write(ByteBuffer.allocate(4).putInt(nodeAddressSize).array());
        byteArrayOutputStream.write(nodeAddressBytes);

        // Add file count
        int fileCount = files.size();
        byteArrayOutputStream.write(ByteBuffer.allocate(4).putInt(fileCount).array());

        // Add file names, sizes, and data
        for (Map.Entry<String, List<byte[]>> entry : files.entrySet()) {
            String fileName = entry.getKey();
            List<byte[]> fileData = entry.getValue();

            // Add file name size and file name
            byte[] fileNameBytes = fileName.getBytes();
            int fileNameSize = fileNameBytes.length;
            byteArrayOutputStream.write(ByteBuffer.allocate(4).putInt(fileNameSize).array());
            byteArrayOutputStream.write(fileNameBytes);

            // Add the number of file blocks
            int blockCount = fileData.size();
            byteArrayOutputStream.write(ByteBuffer.allocate(4).putInt(blockCount).array());

            // Add each file block's size and data
            for (byte[] data : fileData) {
                int blockSize = data.length;
                byteArrayOutputStream.write(ByteBuffer.allocate(4).putInt(blockSize).array());
                byteArrayOutputStream.write(data);
            }
        }

        // Convert the byte list to a byte array
        return byteArrayOutputStream.toByteArray();
    }

    public static Protocol unpack(byte[] serializedData) throws IOException {

        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(serializedData);
        DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);

        // Read the command
        int commandSize = dataInputStream.readInt();
        byte[] commandBytes = new byte[commandSize];
        dataInputStream.readFully(commandBytes);
        String command = new String(commandBytes);

        // Read the node address
        int nodeAddressSize = dataInputStream.readInt();
        byte[] nodeAddressBytes = new byte[nodeAddressSize];
        dataInputStream.readFully(nodeAddressBytes);
        String nodeAddress = new String(nodeAddressBytes);

        // Read the file count
        int fileCount = dataInputStream.readInt();

        // Create a map to store file data
        Map<String, List<byte[]>> files = new HashMap<>();

        // Read file names, sizes, and data
        for (int i = 0; i < fileCount; i++) {
            int fileNameSize = dataInputStream.readInt();
            byte[] fileNameBytes = new byte[fileNameSize];
            dataInputStream.readFully(fileNameBytes);
            String fileName = new String(fileNameBytes);

            int blockCount = dataInputStream.readInt();

            List<byte[]> fileData = new ArrayList<>();
            for (int j = 0; j < blockCount; j++) {
                int blockSize = dataInputStream.readInt();
                byte[] blockData = new byte[blockSize];
                dataInputStream.readFully(blockData);
                fileData.add(blockData);
            }

            files.put(fileName, fileData);
        }

        // Create and return your data structure with the deserialized data
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
