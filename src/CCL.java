/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.imageio.ImageIO;

/**
 *
 * @author Lemmin
 */
public class CCL {
    
    
    public interface iTableFunction{
        public void onIteration(Object[][] array, int x, int y);
        public void onNewArray(Object[][] array, int x, int y);
    }
    public static void tableFunction(Object[][] array, iTableFunction function){
        for(int i=0;i<array.length;i++){
            for(int j=0; j<array[i].length; j++){
                function.onIteration(array,i,j);
            }
            function.onNewArray(array, i, i);
        } 
    }
    
    
    public static Integer[][] parsePicture(String path,boolean monochrome) throws IOException{
        BufferedImage image = ImageIO.read(new File(path));
        int h = image.getHeight();
        int w = image.getWidth();
        Integer[][] pixels = new Integer[h][w];

        for (int x = 0; x < h ; x++) {
            for (int y = 0; y < w; y++) {
                if(!monochrome){
                    pixels[x][y] = (Integer) (image.getRGB(y, x)); 
                }else{
                    pixels[x][y] = (Integer) (image.getRGB(y, x) == 0xFFFFFFFF ? 0 : 1);

                }
                
            }
        }
        return pixels;
    }  
    public static BufferedImage toImage(MiniShared shared){
        final BufferedImage image = new BufferedImage(shared.length(),shared.width(),BufferedImage.TYPE_3BYTE_BGR);
        iTableFunction colorMe = new iTableFunction() {
            @Override
            public void onIteration(Object[][] array, int x, int y) {
                    MiniComponent comp = (MiniComponent) array[x][y];
                    int val = Math.abs(comp.label.hashCode() % 10000);
                    
                    int rgb;
                    int red = val*13 % 255;
                    int blu = val*17 % 255;
                    int green = val *23 % 255;
                    rgb = new Color(red,green,blu).getRGB();               
                    image.setRGB(comp.location.x, comp.location.y, rgb);
            }

            @Override
            public void onNewArray(Object[][] array, int x, int y) {
            }
        };
        tableFunction(shared.comp,colorMe);
        return image;
    }
   

    public static class MiniShared{
        public MiniComponent[][] comp;
        public MiniShared(MiniComponent[][] array){
            this.comp = array;
        }
        protected MiniComponent get(int y, int x){
            if(y<this.width() && x<this.length()){
                return this.comp[y][x];
            }else{
                return null;
            }
        }
        public final int width(){
            return comp.length;
        }
        public final int length(){
            return comp[0].length;
        }
        protected MiniComponent get(Pos pos){
            if(pos == null){
                return null;
            }
            return get(pos.y,pos.x);
        }
    }
    public static class MiniComponent{
        public Pos location;
        public String label;
        public Pos down;
        public int id;
        public MiniComponent(int Y, int X, int id){
            this.location = new Pos(Y,X);
            this.id = id;
            this.label = null;
        }
        @Override
        public int hashCode(){
            return location.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final MiniComponent other = (MiniComponent) obj;
            return Objects.equals(this.location, other.location);
        }
        @Override
        public String toString(){
            String hasDown = "+";
            if(down == null){
                hasDown = "-";
            }
                    
            return location.toString()+hasDown;
        }
    }
    public static class Pos{
        public int y,x;
        public Pos(int Y, int X){
            this.y = Y;
            this.x = X;
        }
        
        @Override
        public String toString(){
            return y+" "+x;
        }
        @Override
        public int hashCode(){
            return new Long(y*shared.width() + x).hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final Pos other = (Pos) obj;
            if (this.y != other.y) {
                return false;
            }
            return this.x == other.x;
        }
    }
    
    
    public static class RosenfeldPfaltz {
    public static AtomicInteger unionRequests = new AtomicInteger(0);
    public static SimpleComponent[][] fromPixelArraySimple(Integer[][] pixels){
        int width = pixels.length;
        int length = pixels[0].length;
        SimpleComponent[][] array = new SimpleComponent[width][length];
        for(int i=0;i<width;i++){
            for(int j=0; j<length; j++){
                SimpleComponent comp = new SimpleComponent(i,j,pixels[i][j]);
                array[i][j] = comp; 
            }
        } 
        return array;
    }
    public static HashMap<Integer,Integer> lookUpCache = new HashMap<>();
    public static UnionFind uf = new UnionFind();
    public static AtomicInteger currentLabel = new AtomicInteger(1);
    public static ExecutorService exe;
    public static ExecutorService unionService;// = Executors.newSingleThreadExecutor();
    public static void unionRequest(final int min, final int max){
        if(!uf.map.containsKey(min)){
            uf.add(min);
        } 
        if(!uf.map.containsKey(max)){
            uf.add(max);  
        }
        Runnable run = new Runnable(){
            @Override
            public void run() {
                uf.union(min, max);
                unionRequests.decrementAndGet();
            }
        };
        unionRequests.incrementAndGet();
        unionService.submit(run);
    }
    public static void lookUpCache(final SimpleComponent comp){
        final int id = comp.intLabel;
        if(lookUpCache.containsKey(id)){
            comp.label = lookUpCache.get(id)+"";
        }else{
            if(!uf.map.containsKey(id)){
                comp.label = id+"";
            }else{
                if(unionRequests.get()>0){
                    Runnable run = new Runnable() {
                        @Override
                        public void run() {
                            int find = (int) uf.find(id);
                            lookUpCache.put(id, find);
                            comp.label = find+"";
                        }
                    };
                    unionService.submit(run);
                }else{
                    int find = (int) uf.find(id);
                    lookUpCache.put(id, find);
                    comp.label = find+""; 
                }
                
                
            }
        }
        
    }
    public static class SimpleShared extends MiniShared{
        public SimpleShared(SimpleComponent[][] array){
            super(array);
        }
        @Override
        protected SimpleComponent get(int y, int x){
            if(y<this.width() && x<this.length()){
                return (SimpleComponent) this.comp[y][x];
            }else{
                return null;
            }
        }
        @Override
        protected SimpleComponent get(Pos pos){
            if(pos == null){
                return null;
            }
            return get(pos.y,pos.x);
        }
    }
    public static class SimpleComponent extends MiniComponent{
        public int intLabel;
        public SimpleComponent(int Y, int X, int id) {
            super(Y, X, id);
            intLabel = 0;
        }
        
        
    }
    public static class RowMarker extends RowRemarker implements Callable{
        public LinkedBlockingDeque<SimpleComponent> row;
        public RowMarker topRow;

        
        public RowMarker(SimpleShared shared,int rowIndex,RowMarker dependency){
            super(shared,rowIndex);
            this.topRow = dependency;
            this.row = new LinkedBlockingDeque<>(shared.length()+1);
        }
        
        @Override
        public Object call() throws Exception {
//            Log.print("Started "+rowIndex);
            int size = shared.length();
            if(rowIndex == 0){//I AM FIRST
                SimpleComponent temp = shared.get(0, 0);
                temp.intLabel = currentLabel.get();
                row.putLast(temp);
                for(int i=1;i<size;i++){
                    SimpleComponent get = shared.get(rowIndex, i);
                    SimpleComponent left = shared.get(rowIndex, i-1);
                    if(left.id == get.id){
                        get.intLabel = left.intLabel;
                    }else{
                        get.intLabel = currentLabel.incrementAndGet();
                    }
                    row.putLast(get);
                }
            }else{
                SimpleComponent get = shared.get(rowIndex, 0);
                SimpleComponent top = topRow.row.takeFirst();
                if(get.id == top.id){
                    get.intLabel = top.intLabel;
                }else{
                    get.intLabel = currentLabel.incrementAndGet();
                }
                row.putLast(get);
                SimpleComponent left;
                for(int i=1;i<size;i++){
//                    Log.print(i);
                    get = shared.get(rowIndex, i);
                    left = shared.get(rowIndex, i-1);
                    top = topRow.row.takeFirst();
                    if(top.id != get.id && left.id != get.id){
                        get.intLabel = currentLabel.incrementAndGet();
                    }else if(top.id == get.id && left.id != get.id){
                        get.intLabel = top.intLabel;
                    }else if(top.id != get.id && left.id == get.id){
                        get.intLabel = left.intLabel;
                    }else if(top.id == get.id && left.id == get.id){
                        if(left.intLabel!= top.intLabel){ // add relation
                            int min,max;
                         
                            if(left.intLabel>top.intLabel){
                                max = left.intLabel;
                                min = top.intLabel;
                            }else{
                                min = left.intLabel;
                                max = top.intLabel;
                            }
                            get.intLabel = min;
                            unionRequest(min,max);
                            
                        }else {
                            get.intLabel = top.intLabel;
                        }
                    }else{
                        get.intLabel = currentLabel.incrementAndGet();
                    }
                    row.putLast(get);
                }
            }
//            Log.print(this.row.toString());
//            Log.print("Finished "+rowIndex);
            latch.countDown();
            return null;
        }
        
    }
    
    public static class RowRemarker implements Callable{
        public int rowIndex;
        public SimpleShared shared;
        public CountDownLatch latch;
        public RowRemarker(SimpleShared shared,int rowIndex){
            this.rowIndex = rowIndex;
            this.shared = shared;
        }
        @Override
        public Object call() throws Exception {
            int size = shared.length();
            for(int i=0; i<size; i++){
                
                SimpleComponent get = shared.get(rowIndex, i);
                lookUpCache(get);
            }
            latch.countDown();
            return null;
        }
        
    }
    
    public static void strategy(SimpleShared shared, int threadCount) throws InterruptedException, Exception{
//        iTableFunction printIntLabel = new iTableFunction() {
//            String line = "";
//            int maxWidth = 3;
//            @Override
//            public void onIteration(Object[][] array, int x, int y) {
//                SimpleComponent name = (SimpleComponent)array[x][y];
//                String app= name.intLabel+"";
//                while(app.length()<maxWidth){
//                    app+=" ";
//                }
//                line += app;               
//            }
//
//            @Override
//            public void onNewArray(Object[][] array, int x, int y) {
//                Log.print(line);
//                line = "";
//            }
//        };
        exe = Executors.newFixedThreadPool(threadCount);
        unionService = Executors.newSingleThreadExecutor();
        int width = shared.width();
        ArrayList<RowRemarker> workers = new ArrayList<>(width);
        RowMarker marker = new RowMarker(shared,0,null);
        workers.add(marker);
        for(int i=1; i<width; i++){
            RowMarker next = new RowMarker(shared,i,marker);
            marker = next;
            workers.add(next);
        }
        CountDownLatch latch = new CountDownLatch(width);

        for(RowRemarker cal:workers){
            cal.latch = latch;
            exe.submit(cal);
        }
        latch.await();
        CountDownLatch latch2 = new CountDownLatch(width);
        for(int i=0; i<width; i++){
            RowRemarker remarker = new RowRemarker(shared,i);
            remarker.latch = latch2;
            exe.submit(remarker);
        }
        latch2.await();
        exe.shutdown();
        unionService.shutdown();
        unionService.awaitTermination(1, TimeUnit.DAYS);
//        for(UFNode node:nodes.values()){
//            Log.print(node);
//        }
//                tableFunction(shared.comp,printIntLabel);

    }
}
    
    
    public static MiniShared shared;
    public static ArrayList<String> Log = new ArrayList<>();
    public static ExecutorService pool;
    public static final int CORE_COUNT = Runtime.getRuntime().availableProcessors();
    public static int THREAD_COUNT = CORE_COUNT;
    
    
    public static long make(Integer[][] pixelArray, boolean makeImage, String picPath) throws Exception{
        shared = new RosenfeldPfaltz.SimpleShared(RosenfeldPfaltz.fromPixelArraySimple(pixelArray));
        long time = System.currentTimeMillis();
        RosenfeldPfaltz.strategy((RosenfeldPfaltz.SimpleShared) shared, THREAD_COUNT);
        time = System.currentTimeMillis() - time;
        if(makeImage)
            ImageIO.write(toImage(shared), "png", new File(picPath));
        return time;
    }
    public static long make(Integer[][] pixelArray) throws Exception{
        return make(pixelArray,false,null);
    }
    public static void main(String[] args) throws Exception{
        String logPath = "log.txt";
        if(args.length<3){
            Log.add("Bad arguments");
            Log.add("Usage: "+"(string)\"picture path\" (boolean)\"make image\" (integer)\"Thread Count\" (string)\"log path\"[optional] (integer)tryUntilThreadCount[optional]");            
        }else{
            Log.add("Core count "+CORE_COUNT);
            String pic = args[0];
            Boolean makeImage = Boolean.parseBoolean(args[1]);
            int potentialThreadCount = Integer.parseInt(args[2]);
            if(potentialThreadCount>0){
                THREAD_COUNT = potentialThreadCount;
            }
            boolean forLoop = false;
            int maxThreadCount = THREAD_COUNT;

            if(args.length>3)
                logPath = args[3];
            if(args.length>4){
                forLoop = true;
                maxThreadCount = Integer.parseInt(args[4]);
            }
                
            Integer[][] parsePicture = parsePicture(pic, false);
            int i=maxThreadCount;
            if(forLoop){
               i = 1; 
            }
            for(; i<maxThreadCount+1; i++){
                
                THREAD_COUNT = i;
                Log.add("Thread count:"+THREAD_COUNT);
                Log.add("Time:"+make(parsePicture,makeImage,"res.png"));
                Log.add("");
            }
            
        }
        PrintStream stream = new PrintStream(new BufferedOutputStream(new FileOutputStream(new File(logPath))));
        for(String line:Log){
            System.out.println(line);
            stream.println(line);
        }
        stream.close();
        System.exit(0);
    }
    
    

}
