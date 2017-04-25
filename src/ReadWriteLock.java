


/**
 *
 * @author Laimonas Beniušis
 * Semaphore
 * 
 */
public class ReadWriteLock {
    private long readers = 0;
    private long writers = 0;
    private long writeReq = 0;
    
    public synchronized void lockRead() throws InterruptedException{
        while(writers + writeReq > 0){
            wait();
        }
        readers++;
    }
    
    public synchronized void unlockRead(){
        readers--;
        notifyAll();
    }
    
    public synchronized void lockWrite() throws InterruptedException{
        writeReq++;
        while(readers + writers > 0){
            wait();
        }
        writeReq--;
        writers++;
    }
    public synchronized void unlockWrite(){
        writers--;
        notifyAll();
    }
    public boolean isBeingWritten(){
        return writers > 0;
    }
    public boolean isBeingRead(){
        return readers > 0;
    }
}
