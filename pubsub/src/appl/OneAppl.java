package appl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import core.Message;

public class OneAppl {
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        new OneAppl(true);
    }

    public OneAppl(boolean flag) {
        String primaryAdress = "localhost";
        int primaryPort = 8080;
        String backupAdress = "localhost";
        int backupPort = 8081;

        PubSubClient clientA = new PubSubClient("localhost", 8082);
        PubSubClient clientB = new PubSubClient("localhost", 8083);
        PubSubClient clientC = new PubSubClient("localhost", 8084);

        clientA.subscribe(primaryAdress, primaryPort);
        clientB.subscribe(primaryAdress, primaryPort);
        clientC.subscribe(primaryAdress, primaryPort);

        clientA.subscribe(backupAdress, backupPort);
        clientB.subscribe(backupAdress, backupPort);
        clientC.subscribe(backupAdress, backupPort);

        Thread accessOne = new requestAcquire(clientA, "ClientA", "-acquire-", "X", primaryAdress, primaryPort);
        Thread accessTwo = new requestAcquire(clientB, "ClientB", "-acquire-", "X", primaryAdress, primaryPort);
        Thread accessThree = new requestAcquire(clientC, "ClientC", "-acquire-", "X", primaryAdress, primaryPort);

        int seconds = (int) (Math.random() * (10000 - 1000)) + 1000;
        System.out.println("Starting in " + seconds / 1000 + " seconds...\n");
        try {
            Thread.currentThread().sleep(seconds);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        accessOne.start();
        accessTwo.start();
        accessThree.start();

        try {
            accessOne.join();
            accessTwo.join();
            accessThree.join();
        } catch (Exception ignored) {
        }
        clientA.unsubscribe(primaryAdress, primaryPort);
        clientB.unsubscribe(primaryAdress, primaryPort);
        clientC.unsubscribe(primaryAdress, primaryPort);

        clientA.stopPubSubClient();
        clientB.stopPubSubClient();
        clientC.stopPubSubClient();
    }

    class requestAcquire extends Thread {
        PubSubClient client;
        String clientName;
        String action;
        String resource;
        String hostBroker;
        int portBroker;

        public requestAcquire(PubSubClient client, String clientName, String action, String resource, String hostBroker, int portBroker) {
            this.client = client;
            this.clientName = clientName;
            this.action = action;
            this.resource = resource;
            this.hostBroker = hostBroker;
            this.portBroker = portBroker;
        }

        public void run() {
            Thread access = new ThreadWrapper(client, clientName.concat(action).concat(resource), hostBroker, portBroker);
            access.start();

            try {
                access.join();
            } catch (Exception ignored) {}


            List<Message> logs = client.getLogMessages();

            //Vetor que guarda todos os aquires do log
            List<String> acquires = new ArrayList<String>();

            //Varredura do log e capturando todas as mensagens que tiverem a mensagem acquire
            Iterator<Message> it = logs.iterator();
            while(it.hasNext()){
                Message log = it.next();
                String content = log.getContent();
                if (content.contains("-acquire-")){
                    acquires.add(content);
                }
            }

            System.out.print("\nORDEM DE CHEGADA MANTIDA PELO BROKER: " + acquires + " \n");

            while (!acquires.isEmpty()){

                String firstClient = acquires.get(0);
                //recurso liberado
                boolean hasRelease = false;

                //Enquanto o recurso não for liberado
                while(!hasRelease){
                    int randomInterval = getRandomInteger(1000, 10000);
                    //Pega o primeiro cliente da lista de qcquire, contem o Meu nome?
                    // Se o cliente é o mesmo que está tratando o log
                    if(firstClient.contains(clientName)){
                        try {
                            access = new ThreadWrapper(client, "useX", hostBroker, 8080);
                            access.start();
                            try {
                                access.join();
                            } catch (Exception ignored) {}

                            System.out.println("___________________________");
                            System.out.println(firstClient.split("-")[0] + " pegou o recurso X");

                            System.out.println("Aguardando " + randomInterval/1000 + " segundos...\n");
                            Thread.currentThread().sleep(randomInterval);

                            access = new ThreadWrapper(client, clientName.concat(":release:X"), hostBroker, 8080);
                            access.start();
                            hasRelease = true;
                            try {
                                access.join();
                            } catch (Exception ignored) {}
                        }catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    try{
                        Thread.currentThread().sleep(randomInterval);
                        hasRelease = true;
                    }catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                if (!acquires.isEmpty()){
                    acquires.remove(0);
                }
            }
        }

        public int getRandomInteger(int minimum, int maximum){
            return ((int) (Math.random()*(maximum - minimum))) + minimum;
        }
    }

    class ThreadWrapper extends Thread{
        PubSubClient c;
        String msg;
        String host;
        int port;

        public ThreadWrapper(PubSubClient c, String msg, String host, int port){
            this.c = c;
            this.msg = msg;
            this.host = host;
            this.port = port;
        }

        public void run(){
            c.publish(msg, host, port);
        }
    }
}
