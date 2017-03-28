/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
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
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import javax.imageio.ImageIO;

/**
 *
 * @author Lemmin
 */
public class CCL {

    
    public static final int THREAD_COUNT = 20;
    public static ExecutorService pool;
    public static HashSet<String> set = new HashSet<>();
    public static int length,width;
    public static int charInt = 33;
    public static final int CORE_COUNT = Runtime.getRuntime().availableProcessors();
    public static MiniComponent[][] transpose(final MiniComponent[][] array) throws InterruptedException{
        
        ExecutorService service = Executors.newFixedThreadPool(CORE_COUNT);
        final int size = array[0].length;
        final MiniComponent[][] result = new MiniComponent[size][array.length];
        for(int i=0;i<array.length; i++){
            final int index = i;
            Runnable run = new Runnable() {
                @Override
                public void run() {
                    for(int j=0; j<size; j++){
                        result[j][index] = array[index][j];
                    }
                }
            };
            service.submit(run);
        }
        service.shutdown();
        service.awaitTermination(1, TimeUnit.DAYS);
        
        return result;
        
    }
    public static String getUnusedLabel(){
        
        charInt++;
        String valueOf = String.valueOf(Character.toChars(charInt));
        return valueOf;
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
            return new Long(y*width + x).hashCode();
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
    
    public static void start(Collection<? extends Callable> list) throws InterruptedException{
        if(pool !=null){
            pool.shutdownNow();
            pool.awaitTermination(1, TimeUnit.DAYS); 
        }
        
        pool = Executors.newFixedThreadPool(THREAD_COUNT);
        for(Callable c:list){
            pool.submit(c);
        }
    }
    
    public static void join(Collection<? extends Callable> list) throws InterruptedException{
        if(pool!=null){
            pool.shutdown();
            pool.awaitTermination(1, TimeUnit.DAYS);
        }
    }
    

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception{
        
        if(args.length<2){
            System.out.println("Failed arguments");
            System.exit(1);
        }
        System.out.println("Core count "+CORE_COUNT);
        String pic = args[0];
        String res = args[1];
        Integer[][] parsePicture = parsePicture(pic,false);
        length = parsePicture[0].length;
        width = parsePicture.length;
//        oldStrat(parsePicture);
        BufferedImage image = CCL.OptimizedAPI.optimizedStrategy(parsePicture);
        ImageIO.write(image, "png", new File(res));
    }
    public static iTableFunction print = new iTableFunction() {
            boolean firstPrint = true;
            @Override
            public void onIteration(Object[][] array, int x, int y) {
                if(firstPrint){
                    for(int i=0; i<array[0].length; i++){
                        System.out.print(i%10+" ");
                    }
                    System.out.println();
                    firstPrint = false;
                }
                System.out.print(array[x][y] +" ");
            }

            @Override
            public void onNewArray(Object[][] array, int x, int y) {
                System.out.println(" :"+x);
            }
        };
    public static iTableFunction printLabel = new iTableFunction() {
            boolean firstPrint = true;
            String line = "";
            @Override
            public void onIteration(Object[][] array, int x, int y) {
                if(firstPrint){
                    String numbers = "";
                    for(int i = 0; i<array[0].length; i++){
                        numbers += i%10;
                    }
                    System.out.println(numbers);
                    firstPrint = false;
                }
                MiniComponent comp = (MiniComponent)array[x][y];
                String printme = " ";
                if(comp != null && comp.label != null){
                    printme = comp.label;
                }
                line+= printme;
            }
            @Override
            public void onNewArray(Object[][] array, int x, int y) {
                System.out.println(line+" :"+x);
                line = "";
            }
        };
    public static iTableFunction printX = new iTableFunction() {
            @Override
            public void onIteration(Object[][] array, int x, int y) {
                System.out.print(x +" ");
            }

            @Override
            public void onNewArray(Object[][] array, int x, int y) {
                System.out.println();
            }
        };
    public static iTableFunction printY = new iTableFunction() {
            @Override
            public void onIteration(Object[][] array, int x, int y) {
                System.out.print(y +" ");
            }

            @Override
            public void onNewArray(Object[][] array, int x, int y) {
                System.out.println();
            }
        };
    
    
    
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
    public static ArrayList sortByPos(Collection<Pos> list){  
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
    
    public static class OptimizedAPI {
    
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
        public HashSet<MiniComponent> recentlyAdded;
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
            this.recentlyAdded = new HashSet<>();
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
        public final int length,width;
        public MiniShared(MiniComponent[][] array){
            this.comp = array;
            this.width = array.length;
            this.length = array[0].length;
        }
        protected MiniComponent get(int y, int x){
            if(y<this.width && x<this.length){
                return this.comp[y][x];
            }else{
                return null;
            }
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
            this.iterated.addLast(new HashSet<MiniComponent>());
            MiniComponent prev;
            MiniComponent next = this.comp[workLine][index];
            MiniComponent down = this.get(workLine+1, index);
            if(down!=null && down.id == next.id){
                next.down = down.location;
            }
            index++;
            while(index<this.length){
                this.iterated.getLast().add(next);
                prev = next;
                next = this.get(workLine, index);
                if(prev.id != next.id){
                    this.iterated.addLast(new HashSet<MiniComponent>());
                }
                down = this.get(workLine+1, index);
                if(down!=null && down.id == next.id){
                    next.down = down.location;
                }
                index++;
            }
            for(HashSet<MiniComponent> set:iterated){
                CompSet cset = new CompSet();
                cset.bottom.addAll(set);
                for(MiniComponent co:set){
                    cset.topPos.add(co.location);
                }
                cset.collected.addAll(set);
                compSet.add(cset);
            }
        }
        
    } 
    public static class WorkerMerger extends DependableWorker{
        
//        public WorkerMerger depTop,depBot;
        public UltimateWorker top,bot;

        public WorkerMerger(MiniComponent[][] array,UltimateWorker topWork,UltimateWorker botWork) {
            super(array);
            top = topWork;
            bot = botWork;
        }
        
        public <T> boolean hasSameElement(Collection<T> col1, Collection<T> col2){
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
        
        
        @Override
        public void logic(){
            ArrayList<CompSet> topSet = top.compSet;  
            ArrayList<CompSet> botSet = bot.compSet;
            int i =-1;
            int topLimit = topSet.size()-1;
            int botLimit = botSet.size()-1;
            boolean endOuter = false;
            boolean endInner = false;
            while(i<topLimit && !endOuter){
                i++;
                CompSet set = topSet.get(i);
                HashSet<Pos> connectedBottomPos = set.getConnectedBottomPos();
                
                ArrayList<CompSet> matchedSets = new ArrayList<>();
                int j = -1;
                while(j<botLimit && !endInner){
                    j++;
                    CompSet otherSet = botSet.get(j);
                    if(hasSameElement(connectedBottomPos,otherSet.topPos)){
                        matchedSets.add(otherSet);
                        otherSet.added = true;
                    }
                }
                set.bottom.clear();
                if(!matchedSets.isEmpty()){
                    set.recentlyAdded.clear();
                    for(CompSet matched:matchedSets){
                        set.bottom.addAll(matched.bottom);
                        set.collected.addAll(matched.collected);
                    }
                }   
            }
            //find new set
            for(CompSet bots:botSet){              
                if(!bots.added){
                    topSet.add(bots);
                }
            }
            //merge sets
            ArrayList<CompSet> newSets = new ArrayList<>();
            while(!topSet.isEmpty()){
                CompSet set = topSet.remove(0);
                Iterator<CompSet> iterator = topSet.iterator();
                while(iterator.hasNext()){
                    CompSet other = iterator.next();
                    if(hasSameElement(set.collected,other.collected)){
                        set.topPos.addAll(other.topPos); 
                        set.bottom.addAll(other.bottom);
                        set.collected.addAll(other.collected);
                        iterator.remove();
                    }
                }
                newSets.add(set);
                
            }
            topSet.clear();
            topSet.addAll(newSets);
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
    
    public static BufferedImage optimizedStrategy(Integer[][] pixels) throws InterruptedException{
        MiniComponent[][] fromPixels = fromPixelArrayMini(pixels);
        
        boolean didTranspose = false;
        long transposeOverhead = System.currentTimeMillis();
        
        if(fromPixels.length > fromPixels[0].length){
            fromPixels = transpose(fromPixels);
            didTranspose = true;
        }
        transposeOverhead = System.currentTimeMillis() - transposeOverhead;
        
        MiniShared shared = new MiniShared(fromPixels);
        UltimateWorker firstWorker;
        ArrayList<UltimateWorker> workers = new ArrayList<>();
        ArrayList<WorkerMerger> mergers = new ArrayList<>();
        HashMap<Integer,DependableWorker> dependables = new HashMap<>();
        for(int i = 0; i<shared.width; i++){
            UltimateWorker worker = new UltimateWorker(shared.comp,i);
            workers.add(worker);
            dependables.put(i, worker);
        }
        firstWorker = workers.get(0);
        int increment = 2;
        
        do{
            int offset = increment/2;
            int count = 0;
            for(int i=0; i+offset<shared.width; i+= increment){
                int top = i;
                int bot = i + offset;
                System.out.println(top+" "+(bot));
                WorkerMerger merger = new WorkerMerger(shared.comp,workers.get(top),workers.get(bot));
                merger.dependencies.add(dependables.remove(bot).latch);
                merger.dependencies.add(dependables.remove(top).latch);
                dependables.put(top, merger);
                mergers.add(merger);
                count++;
            }
            increment*=2;
            System.out.println("#### "+increment +" parallelization: "+count);
        }while(increment/2<shared.width);
        
        ArrayList<DependableWorker> all = new ArrayList<>();
        all.addAll(workers);
        all.addAll(mergers);
        long time = System.currentTimeMillis();
        start(all);
        join(all);
        time = System.currentTimeMillis() - time;
        ArrayList<MiniComponent> comps = new ArrayList<>(shared.width*shared.length);
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
//        tableFunction(shared.comp,printLabel);
        System.out.println("\n"+time);
        System.out.println("transpose overhead "+ transposeOverhead +" "+transposeOverhead2);
        System.out.println("Total:"+(time+ transposeOverhead + transposeOverhead2));
        BufferedImage image = new BufferedImage(shared.length,shared.width,BufferedImage.TYPE_3BYTE_BGR);
        for(MiniComponent comp:comps){
            int val = comp.label.hashCode();
            int red = val*70 % 255;
            int blu = val*50 %255;
            int green = val *60 % 255;
            int rgb = new Color(red,green,blu).getRGB();
            try{
                image.setRGB(comp.location.x, comp.location.y, rgb);
            }catch (Exception e){
                System.out.println(comp.location);
            }
        }
        return image;
        
        
    }
}
}
