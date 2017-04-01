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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
    
    public static void start(Collection<? extends Callable> list) throws InterruptedException, Exception{
        join();
        
        pool = Executors.newFixedThreadPool(THREAD_COUNT);
        for(Callable c:list){
            pool.submit(c);
        }
        
        
    }
    public static void join() throws InterruptedException{
        if(pool!=null){
            pool.shutdown();
            pool.awaitTermination(1, TimeUnit.DAYS);
        }
    }

    
    
    public static class OptimizedAPI {
        public static String getUnusedLabel(){ 
            String valueOf = String.valueOf(Character.toChars(charInt++));
            return valueOf;
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

        public static MiniComponent[][] transpose(final MiniComponent[][] array) throws InterruptedException{    
            final int size = array[0].length;
            final MiniComponent[][] result = new MiniComponent[size][array.length];
            if(threadedTranspose){
                final CountDownLatch latch = new CountDownLatch(array.length);
                for(int i=0;i<array.length; i++){
                    final int index = i;
                    Runnable run = new Runnable() {
                        @Override
                        public void run() {
                            for(int j=0; j<size; j++){
                                result[j][index] = array[index][j];
                            }
                            latch.countDown();
                        }
                    };
                    forTranspose.submit(run);
                }
                latch.await();
            }else{
                for(int i=0;i<array.length; i++){
                    for(int j=0; j<size; j++){
                        result[j][i] = array[i][j];
                    }
                }
            }
        return result;
        
    }
        
        public static <T> boolean hasSameElement(Collection<T> col1, Collection<T> col2){
                if(col1.isEmpty()||col2.isEmpty()){
                    return false;
                }
                if(col1.size()<col2.size()){
                    for(T el:col1){
                        if(col2.contains(el)){
                            return true;
                        }
                    }
                }else{
                    for(T el:col2){
                        if(col1.contains(el)){
                            return true;
                        }
                    }
                }

                return false;
            }
 
        public static MiniComponent[][] fromPixelArrayMini(Integer[][] pixels){
            int width = pixels.length;
            int length = pixels[0].length;
            MiniComponent[][] array = new MiniComponent[width][length];
            for(int i=0;i<width;i++){
                for(int j=0; j<length; j++){
                    MiniComponent comp = new MiniComponent(i,j,pixels[i][j]);
                    array[i][j] = comp; 
                }
            } 
            return array;
        }

        public static class CompSet{
            public HashSet<Pos> topPos;
            public HashSet<MiniComponent> bottom;
            public HashSet<MiniComponent> collected;
            public boolean added = false;
            public HashSet<Pos> getConnectedBottomPos(){
                HashSet<Pos> pos = new HashSet<>();
                for(MiniComponent component:bottom){
                    if(component.down != null){
                        pos.add(component.down);
                    }
                }
                return pos;
            }
            public CompSet(){
                this.topPos = new HashSet<>();
                this.bottom = new HashSet<>();
                this.collected = new HashSet<>();
            }
            @Override
            public String toString(){
                String res = "" ;
                res += "top:\n" + sortByPos(topPos)+"\n";
                res += "collected:\n"+ sortByComponent(collected)+"\n";
                res += "bottom:\n"+sortByPos(getConnectedBottomPos());
                return res;
            }
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

        public static abstract class DependableWorker extends MiniShared implements Callable{
            public CountDownLatch latch;
            public ArrayList<CountDownLatch> dependencies;
            public DependableWorker(MiniComponent[][] array) {
                super(array);
                this.dependencies = new ArrayList<>();
                this.latch = new CountDownLatch(1);
            }

            public void waitForDependencies() throws InterruptedException{
                for(CountDownLatch l:dependencies){
                    l.await();
                }
            }
            public abstract void logic();
            @Override
            public final Object call() throws Exception {
                waitForDependencies();
                logic();
                latch.countDown();
                return null;
            }


    }
        public static class UltimateWorker extends DependableWorker{
            private final int workLine;
            public ArrayDeque<HashSet<MiniComponent>> iterated;
            public ArrayList<CompSet> compSet;
            public UltimateWorker(MiniComponent[][] array, int workLine) {
                super(array);
                this.workLine = workLine;
                this.compSet = new ArrayList<>();
                this.iterated = new ArrayDeque<>();

            }

            @Override
            public void logic() {
                int index = 0;
                this.iterated.add(new HashSet<MiniComponent>());
                MiniComponent prev;
                MiniComponent next = this.comp[workLine][index];
                MiniComponent down = this.get(workLine+1, index);
                if(down!=null && down.id == next.id){
                    next.down = down.location;
                }
                index++;
                while(index<this.length()){
                    this.iterated.getLast().add(next);
                    prev = next;
                    next = this.get(workLine, index);
                    if(prev.id != next.id){
                        this.iterated.add(new HashSet<MiniComponent>());
                    }
                    down = this.get(workLine+1, index);
                    if(down!=null && down.id == next.id){
                        next.down = down.location;
                    }
                    index++;
                }
                this.iterated.getLast().add(next);
                for(HashSet<MiniComponent> set:iterated){
                    CompSet cset = new CompSet();
                    cset.bottom.addAll(set);
                    for(MiniComponent component:set){
                        cset.topPos.add(component.location);
                    }
                    cset.collected.addAll(set);
                    compSet.add(cset);
                }
                this.iterated.clear();
            }

        } 
        public static class WorkerMerger extends DependableWorker{

            public UltimateWorker top,bot;

            public WorkerMerger(MiniComponent[][] array,UltimateWorker topWork,UltimateWorker botWork) {
                super(array);
                top = topWork;
                bot = botWork;
            }

            
            @Override
            public void logic(){
                ArrayList<CompSet> topSet = top.compSet;  
                ArrayList<CompSet> botSet = bot.compSet;

                ArrayDeque<CompSet> mergingSets = new ArrayDeque<>();
                ArrayDeque<CompSet> nonMergingSets = new ArrayDeque<>();
                int i =-1;
                int topLimit = topSet.size()-1;
                int botLimit = botSet.size()-1;       
                while(i<topLimit){
                    i++;
                    CompSet set = topSet.get(i);
                    HashSet<Pos> connectedBottomPos = set.getConnectedBottomPos();

                    ArrayDeque<CompSet> matchedSets = new ArrayDeque<>();
                    int j = -1;
                    while(j<botLimit){
                        j++;
                        CompSet otherSet = botSet.get(j);
                        if(hasSameElement(connectedBottomPos,otherSet.topPos)){
                            matchedSets.add(otherSet);
                            otherSet.added = true;
                        }else if (i == topLimit && !otherSet.added){//new set to add
                            nonMergingSets.add(otherSet);
                        }
                    }

                    if(!matchedSets.isEmpty()){
                        set.bottom.clear();
                        for(CompSet matched:matchedSets){
                            set.bottom.addAll(matched.bottom);
                            set.collected.addAll(matched.collected);
                        }
                        mergingSets.add(set);
                    }else{
                        nonMergingSets.add(set);
                    }  
                }
                topSet.clear();
                topSet.addAll(nonMergingSets);

                //merge sets
                while(!mergingSets.isEmpty()){
                    CompSet set = mergingSets.pollFirst();
                    Iterator<CompSet> iterator = mergingSets.iterator();
                    while(iterator.hasNext()){
                        CompSet other = iterator.next();
                        if(hasSameElement(set.bottom,other.bottom)){
                            set.topPos.addAll(other.topPos); 
                            set.bottom.addAll(other.bottom);
                            set.collected.addAll(other.collected);
                            iterator.remove();
                        }
                    }
                    topSet.add(set);

                }
            }


            public String id(){
                return top.workLine+" "+bot.workLine;
            }
            @Override
            public String toString(){
                String res = id();

                return res;
            }

        }

        public static BufferedImage optimizedStrategy(MiniShared shared, boolean didTranspose, boolean returnImage) throws InterruptedException, Exception{
            long transposeOverhead = System.currentTimeMillis();
            if(didTranspose){
                shared.comp = transpose(shared.comp);
            }
            transposeOverhead = System.currentTimeMillis() - transposeOverhead;
            Log.add("Image dimensions:" +shared.length()+" X "+shared.width());
            UltimateWorker firstWorker;
            ArrayList<UltimateWorker> workers = new ArrayList<>();
            ArrayList<WorkerMerger> mergers = new ArrayList<>();
            HashMap<Integer,DependableWorker> dependables = new HashMap<>();
            for(int i = 0; i<shared.width(); i++){
                UltimateWorker worker = new UltimateWorker(shared.comp,i);
                workers.add(worker);
                dependables.put(i, worker);
            }
            firstWorker = workers.get(0);
            int increment = 2;
            int index = 0;
            do{
                int offset = increment/2;
                int count = 0;
                for(int i=0; i+offset<shared.width(); i+= increment){
                    int top = i;
                    int bot = i + offset;
    //                Log.println(top+" "+(bot));
                    WorkerMerger merger = new WorkerMerger(shared.comp,workers.get(top),workers.get(bot));
                    merger.dependencies.add(dependables.remove(bot).latch);
                    merger.dependencies.add(dependables.remove(top).latch);
                    dependables.put(top, merger);
                    mergers.add(merger);
                    count++;
                }
                increment*=2;
                index++;
                Log.add("Dependency level "+index +"\t parallelization: "+count);
            }while(increment/2<shared.width());
            
            ArrayList<DependableWorker> all = new ArrayList<>();
            all.addAll(workers);
            all.addAll(mergers);
            Log.add("Total workers: "+all.size());
            //Log.add("Left:"+dependables.size());  //remains last merger
            long time = System.currentTimeMillis();
            start(all);
            join();
            time = System.currentTimeMillis() - time;
            ArrayList<MiniComponent> comps = new ArrayList<>(shared.width()*shared.length());
            for(CompSet comp:firstWorker.compSet){
                String label = getUnusedLabel();
                for(MiniComponent component:comp.collected){
                    component.label = label;
                }
                comps.addAll(comp.collected);
            }
            long transposeOverhead2 = System.currentTimeMillis();
            if(didTranspose){
                shared.comp = transpose(shared.comp);
            }
            transposeOverhead2 = System.currentTimeMillis() - transposeOverhead2;
            Log.add("\n"+time);
            Log.add("transpose overhead "+ transposeOverhead +" "+transposeOverhead2);
            Log.add("Total:"+(time+ transposeOverhead + transposeOverhead2));

            if(returnImage){
                BufferedImage image = new BufferedImage(shared.length(),shared.width(),BufferedImage.TYPE_3BYTE_BGR);
                for(MiniComponent comp:comps){
                    try{
                    int val = comp.label.hashCode();
                    int red = val*70 % 255;
                    int blu = val*50 %255;
                    int green = val *60 % 255;
                    int rgb = new Color(red,green,blu).getRGB();
                        image.setRGB(comp.location.x, comp.location.y, rgb);
                    }catch (Exception e){
                        Log.add(comp.location+" "+e.getMessage());
                    }
                }
                return image;
            }else{
                return null;
            }
        }
    
        public static ArrayList sortByComponent(Collection<MiniComponent> list){
            ArrayList l = new ArrayList(list);
            Collections.sort(l, new Comparator() {
                @Override
                public int compare(Object o1, Object o2) {
                    MiniComponent c1,c2;
                    c1 = (MiniComponent)o1;
                    c2 = (MiniComponent)o2;

                    return c1.hashCode() - c2.hashCode();
                }
            });
            return l;
        }
        public static ArrayList sortByPos(Collection<OptimizedAPI.Pos> list){  
        ArrayList l = new ArrayList(list);
        Collections.sort(l, new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
                Pos c1,c2;
                c1 = (Pos)o1;
                c2 = (Pos)o2;
                
                return c1.hashCode() - c2.hashCode();
            }
        });
        return l;
    }
    
    }
    

    
    public static ArrayList<String> Log = new ArrayList<>();
    public static ExecutorService pool;
    public static ExecutorService forTranspose;
    public static OptimizedAPI.MiniShared shared;
    public static int charInt = 33;
    public static final int CORE_COUNT = Runtime.getRuntime().availableProcessors();
    public static int THREAD_COUNT = CORE_COUNT;
    public static boolean threadedTranspose = false;
    
    
    public static void make(Integer[][] pixelArray, boolean transpose,boolean makeImage, String picPath) throws Exception{
        Log.add("");
        shared = new OptimizedAPI.MiniShared(OptimizedAPI.fromPixelArrayMini(pixelArray));
        BufferedImage image = OptimizedAPI.optimizedStrategy(shared, transpose, makeImage);
        if(makeImage)
            ImageIO.write(image, "png", new File(picPath));
    }
    public static void make(Integer[][] pixelArray, boolean transpose) throws Exception{
        Log.add("");
        shared = new OptimizedAPI.MiniShared(OptimizedAPI.fromPixelArrayMini(pixelArray));
        OptimizedAPI.optimizedStrategy(shared, transpose, false);
    }
    public static void main(String[] args) throws Exception{
        String logPath = "log.txt";
        if(args.length<3){
            Log.add("Bad arguments");
            Log.add("Usage: "+"(string)\"picture path\" (boolean)\"make image\" (integer)\"Thread Count\" (string)\"log path\"[optional]");            
        }else{
            Log.add("Core count "+CORE_COUNT);
            String pic = args[0];
            Boolean makeImage = Boolean.parseBoolean(args[1]);
            int potentialThreadCount = Integer.parseInt(args[2]);
            if(potentialThreadCount>0){
                THREAD_COUNT = potentialThreadCount;
            }
            if(args.length>3)
                logPath = args[3];
            Log.add("Thread count:"+THREAD_COUNT);
            if(threadedTranspose)
                forTranspose = Executors.newFixedThreadPool(THREAD_COUNT);
            
            Integer[][] parsePicture = parsePicture(pic, false);
            make(parsePicture,false,makeImage,"res.png");
        }
        PrintStream stream = new PrintStream(new BufferedOutputStream(new FileOutputStream(new File(logPath))));
        for(String line:Log){
            System.out.println(line);
            stream.println(line);
        }
        if(threadedTranspose)
            forTranspose.shutdown();
        stream.close();
        System.exit(0);
    }
    
    

}
