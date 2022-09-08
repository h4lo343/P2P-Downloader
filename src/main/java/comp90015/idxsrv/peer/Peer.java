package comp90015.idxsrv.peer;


import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLOutput;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingDeque;

import comp90015.idxsrv.filemgr.BlockUnavailableException;
import comp90015.idxsrv.filemgr.FileDescr;
import comp90015.idxsrv.filemgr.FileMgr;
import comp90015.idxsrv.message.*;
import comp90015.idxsrv.server.IOThread;
import comp90015.idxsrv.server.IndexElement;
import comp90015.idxsrv.textgui.ISharerGUI;

/**
 * Skeleton Peer class to be completed for Project 1.
 *
 * @author aaron
 */
public class Peer implements IPeer {

    private IOThread ioThread;

    // used to store data(especially the absolute path for shared file )
    // because peers do not share absolute path on idxsrv, it could help
    // peer find location of a file when sending it
    private LinkedBlockingDeque<Socket> incomingConnections;

    private ISharerGUI tgui;

    private String basedir;

    private int timeout;

    private int port;

    private HashMap<String, ShareRequest> sharedFile;

    public Peer(int port, String basedir, int socketTimeout, ISharerGUI tgui) throws IOException {

        this.tgui = tgui;
        this.port = port;
        this.timeout = socketTimeout;
        this.basedir = new File(basedir).getCanonicalPath();
        this.incomingConnections = new LinkedBlockingDeque<Socket>();
        ioThread = new IOThread(port, incomingConnections, socketTimeout, tgui);
        ioThread.start();

        sharedFile = new HashMap<String, ShareRequest>();

        // create a monitor thread, continually take socket in IO thread and process
        // the download request
        Thread sending = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Socket clientPeerSocket = incomingConnections.take();
                        sendingFile(clientPeerSocket);
                    } catch (InterruptedException | JsonSerializationException | IOException | NoSuchAlgorithmException | BlockUnavailableException e) {
                        e.printStackTrace();
                    }
                }
            }
        });

        sending.start();
    }

    public void shutdown() throws InterruptedException, IOException {
        ioThread.shutdown();
        ioThread.interrupt();
        ioThread.join();

    }

    /*
     * Students are to implement the interface below.
     */
    @Override
    public void shareFileWithIdxServer(File file, InetAddress idxAddress, int idxPort, String idxSecret,
                                       String shareSecret) {
        // Thread for sharing file
        Thread Share = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    // get socket with idxsrv and relevant IO streams
                    Socket socket = connectToIdxSrv(idxAddress, idxPort);
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                    BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));

                    tgui.logInfo("share file with server in progress");
                    System.out.println("share file with server in progress");

                    // create file descr and send share request to idxsrv
                    RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
                    FileDescr descr = new FileDescr(randomAccessFile);
                    writeMsg(bufferedWriter, new ShareRequest(descr, file.getName(), shareSecret, idxPort));

                    tgui.logInfo("share request for file <" + file.getName() + "> has sent to idxserver: " + idxAddress + "/" + idxPort);
                    System.out.println("share request for file <" + file.getName() + "> has sent to idxserver: " + idxAddress + "/" + idxPort);

                    // receive share reply from idxsrv
                    ShareReply shareReply = (ShareReply) readMsg(bufferedReader);
                    if (shareReply.numSharers > 0) {
                        // put the information about shared files on a hashmap, use its absolute path as key
                        // so that the program could find that file when sending it using the path
                        sharedFile.put(file.getPath(), new ShareRequest(descr, file.getName(), idxSecret, idxPort));

                        // if reply shows share is successful, update the gui
                        tgui.addShareRecord(file.getPath(), new ShareRecord(new FileMgr(file.getPath()), shareReply.numSharers, "ready", idxAddress, idxPort, idxSecret, shareSecret));
                        System.out.println("<" + file.getName() + "> has been shared to idxserver: " + idxAddress + "/" + idxPort);
                        tgui.logInfo("<" + file.getName() + "> has been shared to idxserver: " + idxAddress + "/" + idxPort);
                    } else {
                        System.out.println("share of <" + file.getName() + "> failed");
                        tgui.logInfo("share of <" + file.getName() + "> failed");
                    }

                    socket.close();
                    bufferedReader.close();
                    bufferedWriter.close();
                    randomAccessFile.close();


                } catch (IOException e) {
                    e.printStackTrace();
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                } catch (JsonSerializationException e) {
                    e.printStackTrace();
                }
            }
        });

        Share.start();

    }

    @Override
    public void searchIdxServer(String[] keywords,
                                int maxhits,
                                InetAddress idxAddress,
                                int idxPort,
                                String idxSecret) {

        // Thread for searching
        Thread Search = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    // connect to idxsrv
                    Socket socket = connectToIdxSrv(idxAddress, idxPort);
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                    BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));

                    // clear result in gui for the last searching
                    tgui.clearSearchHits();

                    tgui.logInfo("search file in progress");
                    System.out.println("search file in progress");

                    // send search request and receive search reply
                    writeMsg(bufferedWriter, new SearchRequest(maxhits, keywords));
                    SearchReply searchReply = (SearchReply) readMsg(bufferedReader);


                    if (searchReply.hits.length == 0) {
                        System.out.println("there is no file named: <" + Arrays.toString(keywords) + "> in idxserver: " + idxAddress + "/" + idxPort);
                        tgui.logInfo("there is no file named: <" + Arrays.toString(keywords) + "> in idxserver: " + idxAddress + "/" + idxPort);
                    }
                    // show the overall hit number
                    else {
                        System.out.println("totally find: " + searchReply.hits.length + " files on idxserver: " + idxAddress + "/" + idxPort);
                        tgui.logInfo("totally find: " + searchReply.hits.length + " files on idxserver: " + idxAddress + "/" + idxPort);


                        // traverse searching results and update them on gui
                        for (int i = 0; i < searchReply.hits.length; i++) {
                            tgui.addSearchHit(searchReply.hits[i].filename, new SearchRecord(searchReply.hits[i].fileDescr, searchReply.seedCounts[i], idxAddress, idxPort, idxSecret, searchReply.hits[i].secret));
                            System.out.println("get file: " + searchReply.hits[i].filename + " from peer: " + searchReply.hits[i].ip + "/" + searchReply.hits[i].port);
                            tgui.logInfo("get file: " + searchReply.hits[i].filename + " from peer: " + searchReply.hits[i].ip + "/" + searchReply.hits[i].port);
                        }
                    }

                    socket.close();
                    bufferedReader.close();
                    bufferedWriter.close();

                } catch (IOException | JsonSerializationException e) {
                    e.printStackTrace();

                }
            }
        });

        Search.start();

    }

    @Override
    public boolean dropShareWithIdxServer(String relativePathname, ShareRecord shareRecord) {

        try {
            // connect to idxsrv and create relevant IO steams
            Socket socket = connectToIdxSrv(shareRecord.idxSrvAddress, shareRecord.idxSrvPort);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));

            // send drop request and receive drop reply
            File temp = new File(relativePathname);
            writeMsg(bufferedWriter, new DropShareRequest(temp.getName(), shareRecord.fileMgr.getFileDescr().getFileMd5(), shareRecord.sharerSecret, shareRecord.idxSrvPort));
            DropShareReply dropReply = (DropShareReply) readMsg(bufferedReader);

            // if drop is successful, remove information stored
            // in local hashmap for shared file and update tgui
            if (dropReply.success == true) {
                sharedFile.remove(relativePathname);
                System.out.println("<" + temp.getName() + ">" + " has been droped from server: " + shareRecord.idxSrvAddress + "/" + shareRecord.idxSrvPort);
                tgui.logInfo("<" + temp.getName() + ">" + " has been droped from server: " + shareRecord.idxSrvAddress + "/" + shareRecord.idxSrvPort);

                socket.close();
                bufferedReader.close();
                bufferedWriter.close();
                return true;
            }

            // print failed message
            else {
                System.out.println("<" + temp.getName() + ">" + " drop failed");
                tgui.logInfo("<" + temp.getName() + ">" + " drop failed");

                socket.close();
                bufferedReader.close();
                bufferedWriter.close();
                return false;
            }
        } catch (IOException | JsonSerializationException e) {
            e.printStackTrace();
        }

        return false;
    }

    @Override
    public void downloadFromPeers(String relativePathname, SearchRecord searchRecord) {

        // Thread for downloading file
        Thread download = new Thread(new Runnable() {
            @Override
            public void run() {
                String fileMD5 = searchRecord.fileDescr.getFileMd5();
                int blockNum = searchRecord.fileDescr.getNumBlocks();
                long numSharers = searchRecord.numSharers;

                // the variable is used to stored
                // position for currently downloaded block
                int currentBlock = 0;

                try {
                    // first we need to connect to idxsrv
                    // to get information about peers who shared the file
                    Socket socket = connectToIdxSrv(searchRecord.idxSrvAddress, searchRecord.idxSrvPort);
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                    BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));

                    writeMsg(bufferedWriter, new LookupRequest(relativePathname, searchRecord.fileDescr.getFileMd5()));
                    System.out.println("have connected to idxserver: " + searchRecord.idxSrvAddress + "/" + searchRecord.idxSrvPort + " to look for information for target file: <" + relativePathname + ">");
                    tgui.logInfo("have connected to idxserver: " + searchRecord.idxSrvAddress + "/" + searchRecord.idxSrvPort + " to look for information for target file: <" + relativePathname + ">");

                    // receive and store lookup result in local
                    LookupReply lookupReply = (LookupReply) readMsg(bufferedReader);

                    socket.close();
                    bufferedReader.close();
                    bufferedWriter.close();

                    // update gui, hint whether there is peer sharing the file now
                    if (lookupReply.hits.length > 0) {
                        System.out.println("obtained peer list, " + "totally find: (" + lookupReply.hits.length + ") peer(s) sharing this file");
                        tgui.logInfo("obtained peer list, " + "totally find: (" + lookupReply.hits.length + ") peer(s) sharing this file");
                    } else {
                        System.out.println("no peer is sharing this file: <" + relativePathname + ">");
                        tgui.logInfo("no peer is sharing this file: <" + relativePathname + ">");

                        return;
                    }

                    // start to connect to the peer
                    // sequentially to download file
                    for (long i = 0; i < numSharers; i++) {
                        String ip = lookupReply.hits[(int) i].ip;

                        System.out.println("try to connect to peer: " + i + ", " + ip + "/" + port + " to download file: <" + relativePathname + ">");
                        tgui.logInfo("try to connect to peer: " + i + ", " + ip + "/" + port + " to download file: <" + relativePathname + ">");

                        try {
                            // connect to that peer and create IO stream
                            Socket serverSocket = new Socket(ip, port);
                            InputStream inputStream = serverSocket.getInputStream();
                            OutputStream outputStream = serverSocket.getOutputStream();
                            BufferedReader bufferedReaderDownload = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
                            BufferedWriter bufferedWriterDownload = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));

                            // send download request to that peer
                            writeMsg(bufferedWriterDownload, new DownloadRequest(relativePathname, fileMD5));
                            System.out.println("connected to the peer, send download request");
                            tgui.logInfo("connected to the peer,  send download request");

                            // receive download request and to see
                            // whether that peer found the file
                            DownloadReply downloadReply = (DownloadReply) readMsg(bufferedReaderDownload);

                            // start to download file if that peer found the file
                            if (downloadReply.success) {
                                System.out.println("find file: <" + relativePathname + "> on peer: " + ip + "/" + port + ", start to download");
                                tgui.logInfo("find file: <" + relativePathname + "> on peer: " + ip + "/" + port + ", start to download");

                                // create folder for file downloaded
                                new File("download").mkdirs();

                                // create fileMgr to write data in that empty file
                                FileMgr downloadedFile = new FileMgr("download/" + relativePathname, searchRecord.fileDescr);

                                Boolean Unfinished = true;
                                while (Unfinished) {
                                    try {
                                        //request for block and receive block sequentially
                                        for (int j = currentBlock; j < blockNum; j++) {

                                            // record position of block that has just been downloaded
                                            // used in downloaded recovery
                                            currentBlock = j;

                                            writeMsg(bufferedWriterDownload, new BlockRequest(relativePathname, fileMD5, j));

                                            // receive data and convert byte array into byte array data
                                            String rawData = ((BlockReply) readMsg(bufferedReaderDownload)).bytes;
                                            byte[] data = Base64.getDecoder().decode(rawData);

                                            // write byte array data into target file
                                            downloadedFile.writeBlock(j, data);

                                            // record position of block which has just been downloaded
                                            // could be used in download recovery
                                            currentBlock = j;

                                            System.out.println("finished downloading block: " + j + ", remaining block number: " + (blockNum - (j + 1)));
                                            tgui.logInfo("finished downloading block: " + j + ", remaining block number: " + (blockNum - (j + 1)));

                                            // close while loop if all block has been downloaded
                                            if (currentBlock == blockNum - 1) {
                                                Unfinished = false;
                                            }
                                        }
                                    } catch (Exception e) {

                                        // show message if there is no peering sharing the file
                                        if (i == lookupReply.hits.length - 1) {
                                            System.out.println("download failed, there is no more peer sharing this file ");
                                            tgui.logInfo("download failed, there is no more peer sharing this file ");
                                            return;
                                        } else {

                                            // try to connect to the next peer
                                            // to keep download from the broken block
                                            serverSocket.close();
                                            bufferedReaderDownload.close();
                                            bufferedWriterDownload.close();

                                            // connect to the next pair
                                            ip = lookupReply.hits[(int) (++i)].ip;
                                            serverSocket = new Socket(ip, port);

                                            System.out.println("connected to the next peer, start to continue downloading");
                                            tgui.logInfo("connected to the next peer, start to continue downloading");

                                            // replace the old IO steams with new ones
                                            inputStream = serverSocket.getInputStream();
                                            outputStream = serverSocket.getOutputStream();
                                            bufferedReaderDownload = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
                                            bufferedWriterDownload = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
                                        }
                                    }
                                }
                                // download all blocks, receive goodbye message from peer
                                String goodbye = ((Goodbye) readMsg(bufferedReaderDownload)).message;
                                System.out.println(goodbye);
                                tgui.logInfo(goodbye);

                                downloadedFile.closeFile();
                                serverSocket.close();
                                bufferedReaderDownload.close();
                                bufferedWriterDownload.close();
                            } else {

                                // show message for not finding target file on
                                // one peer and try to find on the next one
                                System.out.println("there is no file: <" + relativePathname + "> on peer: " + ip + "/" + port + ", try to download from next peer");
                                tgui.logInfo("there is no file: <" + relativePathname + "> on peer: " + ip + "/" + port + ", try to download from next peer");
                                continue;
                            }

                        } catch (IOException | NoSuchAlgorithmException e) {
                        }
                    }
                    socket.close();
                    bufferedReader.close();
                    bufferedWriter.close();
                } catch (IOException | JsonSerializationException e) {
                    e.printStackTrace();
                }
            }
        });
        download.start();
    }

    private void sendingFile(Socket clientPeerSocket) throws IOException, JsonSerializationException, NoSuchAlgorithmException, BlockUnavailableException {

        // Thread for sending file
        Thread sending = new Thread(new Runnable() {
            @Override
            public void run() {
                try {

                    // connect to requesting peer and create IO streams
                    InputStream inputStream = clientPeerSocket.getInputStream();
                    OutputStream outputStream = clientPeerSocket.getOutputStream();
                    BufferedReader bufferedReaderDownload = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
                    BufferedWriter bufferedWriterDownload = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));

                    // receive request, get the file information
                    DownloadRequest downloadRequest = (DownloadRequest) readMsg(bufferedReaderDownload);
                    System.out.println("received downloading request for file: <" + downloadRequest.filename + "> from peer: " + clientPeerSocket.getInetAddress().getHostAddress());
                    tgui.logInfo("received downloading request for file: <" + downloadRequest.filename + "> from peer: " + clientPeerSocket.getInetAddress().getHostAddress());

                    // try to search that file in local shared
                    // hash map and send success reply if found
                    boolean searched = false;
                    String path = "";
                    for (String i : sharedFile.keySet()) {
                        if (sharedFile.get(i).fileDescr.getFileMd5().equals(downloadRequest.fileMD5)) {
                            writeMsg(bufferedWriterDownload, new DownloadReply(true));
                            searched = true;
                            path = i;
                            break;
                        }
                    }

                    // send fail reply
                    if (searched == false) {
                        writeMsg(bufferedWriterDownload, new DownloadReply(false));
                        return;
                    }

                    // start to send file
                    FileMgr sentFile = new FileMgr(path);
                    int count = 0;

                    //receive block request and send block reply
                    while (true) {

                        // receive block request
                        BlockRequest blockRequest = (BlockRequest) readMsg(bufferedReaderDownload);
                        System.out.println("receive request for block: " + blockRequest.blockIdx);
                        tgui.logInfo("receive request for block: " + blockRequest.blockIdx);

                        // send block reply
                        writeMsg(bufferedWriterDownload, new BlockReply(new File(path).getName(), sharedFile.get(path).fileDescr.getFileMd5(), blockRequest.blockIdx, Base64.getEncoder().encodeToString(sentFile.readBlock(blockRequest.blockIdx))));
                        count++;
                        System.out.println("data for block: " + blockRequest.blockIdx + " has been sent, remaining block: " + (sharedFile.get(path).fileDescr.getNumBlocks() - count));
                        tgui.logInfo("data for block: " + blockRequest.blockIdx + " has been sent, remaining block: " + (sharedFile.get(path).fileDescr.getNumBlocks() - count));

                        // stop while loop if all blocks have been send
                        if (blockRequest.blockIdx == sharedFile.get(path).fileDescr.getNumBlocks() - 1) {
                            break;
                        }
                    }

                    // write goodbye message
                    writeMsg(bufferedWriterDownload, new Goodbye("data for file: <" + new File(path).getName() + "> has all been sent, file would be downloaded in a folder named \"download\" --- peer: " + clientPeerSocket.getLocalAddress().getHostAddress()));

                    System.out.println("finished sending all data for target file: " + new File(path).getName());
                    tgui.logInfo("finished sending all data for target file: " + new File(path).getName());

                    sentFile.closeFile();
                    clientPeerSocket.close();
                    bufferedReaderDownload.close();
                    bufferedWriterDownload.close();

                } catch (IOException | JsonSerializationException | NoSuchAlgorithmException | BlockUnavailableException e) {
                    e.printStackTrace();
                }
            }
        });

        sending.start();

    }

    // this method is used by peer to connect
    // to idxsrv. It returns a socket with connection
    // to idxsrv
    private Socket connectToIdxSrv(InetAddress idxAddress, int idxPort) throws IOException, JsonSerializationException {
        System.out.println("-----------------------------------------------------------");
        tgui.logInfo("-----------------------------------------------------------");
        System.out.println("try to connect to idxserver: " + idxAddress + "/" + idxPort);
        tgui.logInfo("try to connect to idxserver: " + idxAddress + "/" + idxPort);

        // build connection to idxsrv
        Socket socket = new Socket(idxAddress, idxPort);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

        //receive welcome from idxsrv
        WelcomeMsg welcome = (WelcomeMsg) readMsg(bufferedReader);
        System.out.println(idxAddress + "/" + idxPort + " reply: " + welcome.msg);
        tgui.logInfo(idxAddress + "/" + idxPort + " reply: " + welcome.msg);

        System.out.println("try to authenticate to idxserver: " + idxAddress + "/" + idxPort);
        tgui.logInfo("try to authenticate to idxserver: " + idxAddress + "/" + idxPort);

        // provide secret key to idxsrv
        writeMsg(bufferedWriter, new AuthenticateRequest("server123"));

        // receive reply of authentication result
        AuthenticateReply authReply = (AuthenticateReply) readMsg(bufferedReader);
        if (authReply.success.equals(true)) {
            System.out.println("authentication to " + idxAddress + "/" + idxPort + " is successful");
            tgui.logInfo("authentication to " + idxAddress + "/" + idxPort + " is successful");
            System.out.println("-----------------------------------------------------------");
            tgui.logInfo("-----------------------------------------------------------");

            return socket;
        } else {
            System.out.println("authentication to " + idxAddress + "/" + idxPort + " failed");
            tgui.logInfo("authentication to " + idxAddress + "/" + idxPort + " failed");
            System.out.println("-----------------------------------------------------------");
            tgui.logInfo("-----------------------------------------------------------");

            return null;
        }
    }

    // used to write Message
    private void writeMsg(BufferedWriter bufferedWriter, Message msg) throws IOException {
        bufferedWriter.write(msg.toString());
        bufferedWriter.newLine();
        bufferedWriter.flush();
    }

    // used to read Message
    private Message readMsg(BufferedReader bufferedReader) throws IOException, JsonSerializationException {
        String jsonStr = bufferedReader.readLine();
        if (jsonStr != null) {
            Message msg = (Message) MessageFactory.deserialize(jsonStr);
            return msg;
        } else {
            throw new IOException();
        }
    }
}
