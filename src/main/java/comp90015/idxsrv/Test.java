package comp90015.idxsrv;

import comp90015.idxsrv.peer.Peer;
import comp90015.idxsrv.textgui.PeerGUI;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;


public class Test {
    public static void main(String[] args) throws IOException {

        String secret = "sharer123"; // the secret used by this peer to share files
        String serverSecret = "server123"; // the secret used by the index server, to gain access
        InetAddress idxSrvAddress = InetAddress.getByName("172.26.128.236"); // default hostname of the index server

        int idxSrvPort = 3200; // the port of the index server
        int port = 27100; // the port this peer uses for other peers to connect to
        int timeout = 1000; // the default socket timeout in milliseconds for idle sockets

        String welcome = "Welcome to the default IdxSrv implementation for COMP90015 SM2 2022.";
        String dir = null;
        try {
            dir = new File(System.getProperty("user.dir")).getCanonicalPath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println(dir);

        PeerGUI textGUI = null;
        try {
            textGUI = new PeerGUI(idxSrvAddress,
                    idxSrvPort,
                    serverSecret,
                    secret);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Peer[] peer = new Peer[65000];

//        for (int i = 0; i<10000; i++){
//            try {
//                peer[i] = new Peer(port+i,dir,timeout,textGUI);
//            } catch (IOException e) {
//                System.out.println("port " + (port + i) + " are already used");
//            }
//        }
        for (int i = 0; i < 1800; i++) {

            peer[i] = new Peer(port+i,dir,timeout,textGUI);
            System.out.println(i);

        }
        for (int i = 0; i < 1800 ; i++) {
            peer[i].shareFileWithIdxServer(new File(dir +"/idxsrv.md"),idxSrvAddress,idxSrvPort,serverSecret,secret);
        }

    }
}
