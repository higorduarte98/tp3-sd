package core;


import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;


//the useful socket consumer
public class PubSubConsumer<S extends Socket> extends GenericConsumer<S> {

    private int uniqueLogId;
    private SortedSet<Message> log;
    private Set<String> subscribers;
    private boolean isPrimary;
    private String secondaryServer;
    private int secondaryPort;

    public PubSubConsumer(GenericResource<S> re, boolean isPrimary, String secondaryServer, int secondaryPort) {
        super(re);
        uniqueLogId = 1;
        log = new TreeSet<Message>(new MessageComparator());
        subscribers = new TreeSet<String>();

        this.isPrimary = isPrimary;
        this.secondaryServer = secondaryServer;
        this.secondaryPort = secondaryPort;
    }


    @Override
    protected void doSomething(S str) {
        try {
            // TODO Auto-generated method stub
            ObjectInputStream in = new ObjectInputStream(str.getInputStream());

            Message msg = (Message) in.readObject();

            Message response = null;

            List<Message> logUser = getMessages();
            Iterator<Message> it = logUser.iterator();

            System.out.print("Log user itens: ");
            while (it.hasNext()){
                Message aux = it.next();
                System.out.print(aux.getContent() + aux.getLogId() + " | ");
            }

            System.out.println();

            if(msg.getType().startsWith("changeToPrimary")){
                System.out.println("ENTROU AQUI");
                //response = commands.get(msg.getType()).execute(msg, log, subscribers, isPrimary, secondaryServer, secondaryPort);
                this.isPrimary = true;
                this.secondaryPort = 0;
                this.secondaryServer = "";
            }
            else if (!isPrimary && !msg.getType().startsWith("sync")) {

                //Client client = new Client(secondaryServer, secondaryPort);
                //response = client.sendReceive(msg);

                response = new MessageImpl();
                response.setType("backup");
                response.setContent(secondaryServer + ":" + secondaryPort);

            } else {
                if (!msg.getType().equals("notify") && !msg.getType().startsWith("sync"))
                    msg.setLogId(uniqueLogId);

                response = commands.get(msg.getType()).execute(msg, log, subscribers, isPrimary, secondaryServer, secondaryPort);

                if (!msg.getType().equals("notify")) //&& !msg.getType().startsWith("sync"))
                    uniqueLogId = msg.getLogId();
            }

            ObjectOutputStream out = new ObjectOutputStream(str.getOutputStream());
            out.writeObject(response);
            out.flush();
            out.close();
            in.close();

            str.close();

        } catch (Exception e) {
            try {
                str.close();
            } catch (IOException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
        }

    }

    public List<Message> getMessages() {
        CopyOnWriteArrayList<Message> logCopy = new CopyOnWriteArrayList<Message>();
        logCopy.addAll(log);

        return logCopy;
    }

}
