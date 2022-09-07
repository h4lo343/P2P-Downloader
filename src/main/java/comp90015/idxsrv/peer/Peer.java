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
 * @author aaron
 *
 */
public class Peer implements IPeer {

	private IOThread ioThread;

	private LinkedBlockingDeque<Socket> incomingConnections;

	private ISharerGUI tgui;

	private String basedir;

	private int timeout;

	private int port;

	private Socket socket;

	private BufferedReader bufferedReader;

	private BufferedWriter bufferedWriter;

	private HashMap<String, ShareRequest> sharedFile;

	public Peer(int port, String basedir, int socketTimeout, ISharerGUI tgui) throws IOException {
		this.tgui=tgui;
		this.port=port;
		this.timeout=socketTimeout;
		this.basedir=new File(basedir).getCanonicalPath();
		this.incomingConnections = new LinkedBlockingDeque<Socket>();
		ioThread = new IOThread(port,incomingConnections,socketTimeout,tgui);
		ioThread.start();

		sharedFile = new HashMap<String, ShareRequest>();

		Thread sending = new Thread(new Runnable() {
			@Override
			public void run() {
				while(true) {
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

	public void shutdownConnection() throws IOException {
		bufferedWriter.close();
		bufferedReader.close();
		socket.close();
	}
	/*
	 * Students are to implement the interface below.
	 */
	@Override
	public void shareFileWithIdxServer(File file, InetAddress idxAddress, int idxPort, String idxSecret,
									   String shareSecret) {
		try {
			connectToIdxSrv(idxAddress, idxPort);

			tgui.logInfo("share file with server in progress");
			System.out.println("share file with server in progress");
			RandomAccessFile randomAccessFile = new RandomAccessFile(file,"r");
			FileDescr descr = new FileDescr(randomAccessFile);
			writeMsg(bufferedWriter, new ShareRequest(descr, file.getName(), shareSecret, idxPort));
			tgui.logInfo("share request for file <"+file.getName()+"> has sent to idxserver: "+idxAddress+"/"+idxPort);
			System.out.println("share request for file <"+file.getName()+"> has sent to idxserver: "+idxAddress+"/"+idxPort);
			ShareReply shareReply = (ShareReply) readMsg(bufferedReader);
			if(shareReply.numSharers>0) {
				sharedFile.put(file.getPath(), new ShareRequest(descr, file.getName(), idxSecret, idxPort));
				tgui.addShareRecord(file.getPath(),new ShareRecord(new FileMgr(file.getPath()), shareReply.numSharers, "ready", idxAddress, idxPort, idxSecret, shareSecret ));
				System.out.println("<"+file.getName()+"> has been shared to idxserver: "+idxAddress+"/"+idxPort);
				tgui.logInfo("<"+file.getName()+"> has been shared to idxserver: "+idxAddress+"/"+idxPort);
			}
			else {
				System.out.println("share of <"+file.getName()+"> failed");
				tgui.logInfo("share of <"+file.getName()+"> failed");
			}
			shutdownConnection();

		} catch (IOException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (JsonSerializationException e) {
			e.printStackTrace();
		}

	}



	@Override
	public void searchIdxServer(String[] keywords,
								int maxhits,
								InetAddress idxAddress,
								int idxPort,
								String idxSecret) {
		try {
			connectToIdxSrv(idxAddress, idxPort);

			tgui.logInfo("search file in progress");
			System.out.println("search file in progress");

			writeMsg(bufferedWriter, new SearchRequest(maxhits, keywords));

			SearchReply searchReply = (SearchReply) readMsg(bufferedReader);
			if(searchReply.hits.length == 0) {
				System.out.println("there is no file named: <"+ Arrays.toString(keywords) +"> in idxserver: " +idxAddress+"/"+idxPort);
				tgui.logInfo("there is no file named: <"+ Arrays.toString(keywords) +"> in idxserver: " +idxAddress+"/"+idxPort);
			}
			else {
				System.out.println("totally find: "+ searchReply.hits.length+" files on idxserver: "+idxAddress+"/"+idxPort);
				tgui.logInfo("totally find: "+ searchReply.hits.length+" files on idxserver: "+idxAddress+"/"+idxPort);



				for(int i=0; i<searchReply.hits.length;i++) {
//					searchResult.put(searchReply.hits[i].fileDescr.getFileMd5(), searchReply.hits[i]);
					tgui.addSearchHit(searchReply.hits[i].filename, new SearchRecord(searchReply.hits[i].fileDescr, searchReply.seedCounts[i], idxAddress, idxPort, idxSecret, searchReply.hits[i].secret ));
					System.out.println("get file: "+searchReply.hits[i].filename+" from peer: "+searchReply.hits[i].ip+"/"+searchReply.hits[i].port);
					tgui.logInfo("get file: "+searchReply.hits[i].filename+" from peer: "+searchReply.hits[i].ip+"/"+searchReply.hits[i].port);
				}
			}

			shutdownConnection();

		} catch (IOException | JsonSerializationException e) {
			e.printStackTrace();

		}
	}

	@Override
	public boolean dropShareWithIdxServer(String relativePathname, ShareRecord shareRecord) {

		try {
			connectToIdxSrv(shareRecord.idxSrvAddress, shareRecord.idxSrvPort);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JsonSerializationException e) {
			e.printStackTrace();
		}

		File temp = new File(relativePathname);
		try {
			writeMsg(bufferedWriter, new DropShareRequest(temp.getName(), shareRecord.fileMgr.getFileDescr().getFileMd5(), shareRecord.sharerSecret, shareRecord.idxSrvPort));
			DropShareReply dropReply = (DropShareReply) readMsg(bufferedReader);
			if(dropReply.success==true) {
				sharedFile.remove(relativePathname);
				System.out.println("<"+temp.getName()+">"+" has been droped from server: " +shareRecord.idxSrvAddress+"/"+shareRecord.idxSrvPort);
				tgui.logInfo("<"+temp.getName()+">"+" has been droped from server: " +shareRecord.idxSrvAddress+"/"+shareRecord.idxSrvPort);

				shutdownConnection();
				return true;
			}
			else {
				System.out.println("<"+temp.getName()+">"+" drop failed");
				tgui.logInfo("<"+temp.getName()+">"+" drop failed");

				return false;
			}
		} catch (IOException | JsonSerializationException e) {
			e.printStackTrace();
		}
		try {
			shutdownConnection();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return false;
	}

	@Override
	public void downloadFromPeers(String relativePathname, SearchRecord searchRecord) {
			String fileMD5 = searchRecord.fileDescr.getFileMd5();
			int blockNum = searchRecord.fileDescr.getNumBlocks();
			long fileLength = searchRecord.fileDescr.getFileLength();
			long blockLength = searchRecord.fileDescr.getBlockLength();
			long numSharers = searchRecord.numSharers;

		try {
			connectToIdxSrv(searchRecord.idxSrvAddress, searchRecord.idxSrvPort);
			writeMsg(bufferedWriter, new LookupRequest( relativePathname, searchRecord.fileDescr.getFileMd5()));
			System.out.println("have connected to idxserver: "+ searchRecord.idxSrvAddress + "/" + searchRecord.idxSrvPort + " to look for information for target file: <"+relativePathname+">");
			tgui.logInfo("have connected to idxserver: "+ searchRecord.idxSrvAddress + "/" + searchRecord.idxSrvPort + " to look for information for target file: <"+relativePathname+">");

			LookupReply lookupReply = (LookupReply) readMsg(bufferedReader);

			shutdownConnection();

			if(lookupReply.hits.length>=0) {
				System.out.println("obtained peer list, "+"totally find: ("+lookupReply.hits.length+") peer(s) sharing this file");
				tgui.logInfo("obtained peer list, "+"totally find: ("+lookupReply.hits.length+") peer(s) sharing this file");
			}
			else {
				System.out.println("no peer is sharing this file: <" + relativePathname + ">");
				tgui.logInfo("no peer is sharing this file: <" + relativePathname + ">");

				return;
			}

			for (long i = 0; i<numSharers;i++) {
				String ip = lookupReply.hits[(int)i].ip;

				System.out.println("try to connect to peer: "+ i + ", " + ip + "/" + port + " to download file: <" + relativePathname + ">");
				tgui.logInfo("try to connect to peer: "+ i + ", " + ip + "/" + port + " to download file: <" + relativePathname + ">");

				try {
					Socket serverSocket = new Socket(ip, port);
					InputStream inputStream = serverSocket.getInputStream();
					OutputStream outputStream = serverSocket.getOutputStream();
					BufferedReader bufferedReaderDownload = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
					BufferedWriter bufferedWriterDownload = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));

					writeMsg(bufferedWriterDownload, new DownloadRequest(relativePathname, fileMD5));
					System.out.println("connected to the peer, send download request");
					tgui.logInfo("connected to the peer,  send download request");

					DownloadReply downloadReply = (DownloadReply) readMsg(bufferedReaderDownload);
					if(downloadReply.success) {
						System.out.println("find file: <" + relativePathname + "> on peer: "+ ip + "/" + port+ ", start to download");
						tgui.logInfo("find file: <" + relativePathname + "> on peer: "+ ip + "/" + port+ ", start to download");

						/*
						 * Start to download file
					     */

						// create folder for file downloaded
						new File("download").mkdirs();

						// create fileMgr to write data in that empty file
						FileMgr downloadedFile = new FileMgr("download/"+relativePathname, searchRecord.fileDescr);

						//request for block and receive block
						for (int j = 0; j< blockNum; j++) {
							writeMsg(bufferedWriterDownload, new BlockRequest(relativePathname, fileMD5, j));

							// convert byte array into byte array data
							String rawData = ((BlockReply) readMsg(bufferedReaderDownload)).bytes;
							byte [] data = Base64.getDecoder().decode(rawData);

							// write byte array data into target file
							downloadedFile.writeBlock(j, data);

							System.out.println("finished download block: "+j+", remaining block number: "+ (blockNum - (j+1)));
							tgui.logInfo("finished download block: "+j+", remaining block number: "+ (blockNum - (j+1)));
						}
							String goodbye = ((Goodbye)readMsg(bufferedReaderDownload)).message;
							System.out.println(goodbye);
							tgui.logInfo(goodbye);

							downloadedFile.closeFile();
							serverSocket.close();
							bufferedReaderDownload.close();
							bufferedWriterDownload.close();
					}
					else {
						System.out.println("there is no file: <"+ relativePathname + "> on peer: "+ ip + "/" + port+", try to download from next peer");
						tgui.logInfo("there is no file: <"+ relativePathname + "> on peer: "+ ip + "/" + port+", try to download from next peer");
						continue;
					}

				} catch (IOException e) {
					e.printStackTrace();
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
			}
			shutdownConnection();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JsonSerializationException e) {
			e.printStackTrace();
		}
	}

	private void sendingFile(Socket clientPeerSocket) throws IOException, JsonSerializationException, NoSuchAlgorithmException, BlockUnavailableException {

		InputStream inputStream = clientPeerSocket.getInputStream();
		OutputStream outputStream = clientPeerSocket.getOutputStream();
		BufferedReader bufferedReaderDownload = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
		BufferedWriter bufferedWriterDownload = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));

		DownloadRequest downloadRequest = (DownloadRequest) readMsg(bufferedReaderDownload);
		System.out.println("received downloading request for file: <"+downloadRequest.filename+"> from peer: "+clientPeerSocket.getInetAddress().getHostAddress());
		tgui.logInfo("received downloading request for file: <"+downloadRequest.filename+"> from peer: "+clientPeerSocket.getInetAddress().getHostAddress());

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

		if(searched == false) {
			writeMsg(bufferedWriterDownload, new DownloadReply(false));
			return;
		}

		// start to send file
		FileMgr sentFile = new FileMgr(path);
		int count = 0;

		//receive block request and send block reply
		while (true) {
			BlockRequest blockRequest = (BlockRequest) readMsg(bufferedReaderDownload);
			System.out.println("receive request for block:" + blockRequest.blockIdx);
			tgui.logInfo("receive request for block:" + blockRequest.blockIdx);

			writeMsg(bufferedWriterDownload, new BlockReply(new File(path).getName(), sharedFile.get(path).fileDescr.getFileMd5(), blockRequest.blockIdx, Base64.getEncoder().encodeToString(sentFile.readBlock(blockRequest.blockIdx))));
			count++;
			System.out.println("data for block: "+ blockRequest.blockIdx + " has been sent, remaining block: "+ (sharedFile.get(path).fileDescr.getNumBlocks() - count) );
			tgui.logInfo("data for block: "+ blockRequest.blockIdx + " has been sent, remaining block: "+ (sharedFile.get(path).fileDescr.getNumBlocks() - count));

			if (blockRequest.blockIdx == sharedFile.get(path).fileDescr.getNumBlocks() - 1) {
				break;
			}
		}

		writeMsg(bufferedWriterDownload, new Goodbye("data for file: <"+ new File(path).getName() + "> has all been sent, file would be downloaded in a folder named \"download\" --- peer: "+ clientPeerSocket.getLocalAddress().getHostAddress()));

		System.out.println("finished sending all data for target file: "+new File(path).getName());
		tgui.logInfo("finished sending all data for target file: "+new File(path).getName());

		sentFile.closeFile();
		clientPeerSocket.close();
		bufferedReader.close();
		bufferedWriter.close();
	}

	private void connectToIdxSrv (InetAddress idxAddress, int idxPort) throws IOException, JsonSerializationException {
		System.out.println("-----------------------------------------------------------");
		tgui.logInfo("-----------------------------------------------------------");
		System.out.println("try to connect to idxserver: "+ idxAddress+"/"+idxPort);
		tgui.logInfo("try to connect to idxserver: "+ idxAddress+"/"+idxPort);

		// build connection to idxsrv
		this.socket = new Socket(idxAddress, idxPort);
		this.bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
		this.bufferedWriter = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));

		//receive welcome from idxsrv
		WelcomeMsg welcome = (WelcomeMsg) readMsg(bufferedReader);
		System.out.println(idxAddress+"/"+idxPort+" reply: "+welcome.msg);
		tgui.logInfo(idxAddress+"/"+idxPort+" reply: "+welcome.msg);

		System.out.println("try to authenticate to idxserver: "+ idxAddress+"/"+idxPort);
		tgui.logInfo("try to authenticate to idxserver: "+ idxAddress+"/"+idxPort);

		// provide secret key to idxsrv
		writeMsg(bufferedWriter, new AuthenticateRequest("server123"));

		// receive reply of authentication result
		AuthenticateReply authReply = (AuthenticateReply) readMsg(bufferedReader);
		if(authReply.success.equals(true)) {
			System.out.println("authentication to "+idxAddress+"/"+idxPort+" is successful");
			tgui.logInfo("authentication to "+idxAddress+"/"+idxPort+" is successful");
			System.out.println("-----------------------------------------------------------");
			tgui.logInfo("-----------------------------------------------------------");
		}
		else {
			System.out.println("authentication to "+idxAddress+"/"+idxPort+" failed");
			tgui.logInfo("authentication to "+idxAddress+"/"+idxPort+" failed");
			System.out.println("-----------------------------------------------------------");
			tgui.logInfo("-----------------------------------------------------------");
		}
	}

	private void writeMsg(BufferedWriter bufferedWriter,Message msg) throws IOException {
		bufferedWriter.write(msg.toString());
		bufferedWriter.newLine();
		bufferedWriter.flush();
	}

	private Message readMsg(BufferedReader bufferedReader) throws IOException, JsonSerializationException {
		String jsonStr = bufferedReader.readLine();
		if(jsonStr!=null) {
			Message msg = (Message) MessageFactory.deserialize(jsonStr);
			return msg;
		} else {
			throw new IOException();
		}
	}
}
