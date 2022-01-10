package appl;

import core.Message;

import java.util.*;

public class SingleUser {

    private PubSubClient user;
    private String userName;

    String primaryAddress = "localhost";
    int primaryPort = 8080;
    String backupAddress = "localhost";
    int backupPort = 8081;

    public String getResource() {
        return resource;
    }

    public void setResource(String resource) {
        this.resource = resource;
    }

    private String resource;

    public PubSubClient getUser() {
        return user;
    }

    public void setUser(PubSubClient user) {
        this.user = user;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public static void main(String[] args){
        new SingleUser();
    }

    public SingleUser(){
        Scanner reader = new Scanner(System.in);

        //Credenciais do Broker----------------------------------

        //Credenciais do usuário---------------------------------
        System.out.print("Insira o nome do usuário:");
        String userName = reader.next();
        setUserName(userName);

        System.out.print("Insira o número da porta do usuário:");
        int userPort = reader.nextInt();

        System.out.print("Insira o endereço do usuário:");
        String userAddress = reader.next();

        PubSubClient user = new PubSubClient(userAddress, userPort);
        setUser(user);
        user.subscribe(primaryAddress, primaryPort);
        user.subscribe(backupAddress, backupPort);
        StartTP(user, userName, primaryPort, primaryAddress);
    }

    private void StartTP(PubSubClient user, String userName, int brokerPort, String brokerAdd){
        String[] resorces = {"x"};
        Random seed = new Random();

        for(int i = 0; i < 15; i++){
            String oneResorse = resorces[seed.nextInt(resorces.length)];
            setResource(oneResorse);
            Thread sendOneMessage = new ThreadWrapper(user, userName+":acquire:"+oneResorse, brokerAdd, brokerPort);
            sendOneMessage.start();

            try{
                sendOneMessage.join();
            }catch (Exception e) {
                e.printStackTrace();
            }
            List<Message> logUser = user.getLogMessages();

            treatLog(logUser);

            logUser = user.getLogMessages();
            Iterator<Message> it = logUser.iterator();

            System.out.print("Log user itens: ");
            while (it.hasNext()){
                Message aux = it.next();
                System.out.print(aux.getContent() + aux.getLogId() + " | ");
            }
            System.out.println();
        }

        user.unsubscribe(brokerAdd, brokerPort);
        user.stopPubSubClient();
    }

    private void treatLog(List<Message> logUser){
        List<String> acquires = new ArrayList<String>();
        List<String> releases = new ArrayList<String>();

        Iterator<Message> it = logUser.iterator();
        while(it.hasNext()){
            Message log = it.next();
            String content = log.getContent();
            if (content.contains(":acquire:")){
                acquires.add(content);
            }
            if (content.contains(":release:")){
                releases.add(content);
            }
        }

        while (acquires.size() != releases.size() ){

            String firstClient = acquires.get(releases.size());

            boolean hasRelease = false;

            while(!hasRelease){
                int randomInterval = 1000;
                if(firstClient.contains(getUserName())){
                    try {
                        Thread access = new ThreadWrapper(getUser(), getUserName() + ":use:" + getResource(), primaryAddress, primaryPort);
                        access.start();
                        try {
                            access.join();
                        } catch (Exception ignored) {}

                        Thread.currentThread().sleep(randomInterval);

                        access = new ThreadWrapper(getUser(), getUserName().concat(":release:" + getResource()), primaryAddress, primaryPort);
                        access.start();
                        hasRelease = true;
                        try {
                            access.join();
                        } catch (Exception ignored) {}
                    }catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    if (!acquires.isEmpty()){
                        acquires.remove(0);
                    }
                }
                else {
                    try {
                        Thread.currentThread().sleep(randomInterval);
                        hasRelease = true;
                        releases.clear();
                        logUser = getUser().getLogMessages();
                        it = logUser.iterator();
                        while(it.hasNext()){
                            Message log = it.next();
                            String content = log.getContent();
                            if (content.contains(":release:")){
                                releases.add(content);
                            }
                        }

                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    class ThreadWrapper extends Thread {
        PubSubClient client;
        String message;
        String host;
        int port;

        public ThreadWrapper(PubSubClient client, String message, String host, int port){
            this.client = client;
            this.message = message;
            this.host = host;
            this.port = port;
        }

        public void run(){
            client.publish(message, host, port);
        }
    }
}