package bufmgr;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class LRUK extends Replacer {

        
        
        
    int frames[];    
   //history(p) denotes the history control block of page p; itcontains the times of the K most recent references to page p, discounting correlated references: 
  //history(p,l) denotes the time of last reference, history(p,2) the time of the second to the last reference...
  //last(p) denotes the time of the most recent reference to page p.
    Map<Integer, LinkedList> history = new LinkedHistMap<Integer, LinkedList>();
    Map<Integer, Long> last = new LinkedLongHashMap<Integer, Long>();
	int correlated_reference_period = 0;
	int lastRef;
	int nframes;


	  /**
   	   * This pushes the given frame to the end of the list.
           * @param frameNo the frame number
           */
	
        private void update(int frameNo) {
              
                long t = System.currentTimeMillis();
                long correl_period_of_refd_page = 0;
  		//update history information of a page
                if (t - last.get(frameNo) >= correlated_reference_period) {
                        List historyDetailsList = history.get(frameNo);
                        long histFirstValueOfPage = (Long) historyDetailsList.get(1);
                        // new uncorrelated reference
                        correl_period_of_refd_page = last.get(frameNo)
                                        - histFirstValueOfPage;

                        LinkedList historyDetailsOfFrame = history.get(frameNo);
                        // change 2 to k=1
                        // pulls the history(p,i) forward in time by the value of the correl_period_of_refd_page
                        for (int i = 2; i < lastRef; i++) {
                                historyDetailsOfFrame.add(i,
                                                (Long) historyDetailsOfFrame.get(i - 1)
                                                                + correl_period_of_refd_page);
                        }
                        historyDetailsOfFrame.add(1, t);
                        history.put(frameNo, historyDetailsOfFrame);
                        //update the last to the current time
                        last.put(frameNo, t);
                } else {
                        // corrolated reference
                        LinkedList historyDetailsOfFrame = history.get(frameNo);
                        if (historyDetailsOfFrame == null)
                                historyDetailsOfFrame = new LinkedList();
                        historyDetailsOfFrame.add(t);
                        history.put(frameNo, historyDetailsOfFrame);
                        last.put(frameNo, t);
                }
        }

		  /**
	   * Calling super class the same method
	   * Initializing the frames[] with number of buffer allocated
	   * by buffer manager
	   * set number of frame used to zero
	   *
	   * @param	mgr	a BufMgr object
	   * @see	BufMgr
	   * @see	Replacer
	   */
	public void setBufferManager(BufMgr mgr) {
                super.setBufferManager(mgr);
                frames = new int[mgr.getNumBuffers()];
                nframes = 0;
        }


	 /**
	   * Class constructor
	   * Initializing frames[] pinter = null.
	   */
	
        protected LRUK(BufMgr javamgr, int _lastRef) {
                super(javamgr);
                frames = null;
		lastRef = _lastRef;
        }




        /**
         * calll super class the same method pin the page in the given frame number
         * move the page to the end of list
         *
         * @param frameNo
         *            the frame number to pin
         * @exception InvalidFrameNumberException
         */
        public void pin(int frameNo) throws InvalidFrameNumberException {
                super.pin(frameNo);

                update(frameNo);
        }



        public int pick_victim() throws BufferPoolExceededException {

                int numBuffers = mgr.getNumBuffers();
                int frame = 0;
                int victim = -1;

                if (nframes < numBuffers) {

                        frame = nframes++;
                        frames[frame] = frame;
                        state_bit[frame].state = Pinned;
                        (mgr.frameTable())[frame].pin();
                        return frame;
                } else {
                        long t = System.currentTimeMillis();
                        long min = t;
                        for (int i = 0; i < numBuffers; ++i) {
                                frame = frames[i];

                                long lastOfPage = (long) last.get(frame);
                                List historyOfReference = (List) history.get(frame);

                                // Finding K backward distance for the frame
                                // Last Reference is the K value
                                // CHECK K -1
                                // We check if it's eligible for replacement 
                                long lastRefOfPage = (Long) historyOfReference.get(lastRef);
                                if (t - lastOfPage >= correlated_reference_period
                                                && lastRefOfPage <= min
                                                && state_bit[frame].state != Pinned) {

                                        victim = frame;
                                        min = lastRefOfPage;
                                }

                        }
                }
                // If victim is not -1 then we found an empty slot and update the state_bits and frame in buffer manager respectively
                if (victim != -1) {
                        state_bit[victim].state = Pinned;
                        (mgr.frameTable())[victim].pin();
                        return victim;
                }

                throw new BufferPoolExceededException(null,
                                "bufmgr.BufferPoolExceededException");
        }
	
	public long HIST(int _frameNo,int refID) {
		return (Long) history.get(_frameNo).get(refID);
	}

	public long last(int _frameNo) {
		return last.get(_frameNo);
	}
	public long LAST(int _frameNo) {
		return last.get(_frameNo);
	}
	public int[] getFrames() {return frames;}

	public String name() {
			return "LRUK";
	}

	     /**
	   * print out the information of frame usage
	   */  
	 public void info()
	 {
	    super.info();

	    System.out.print( "LRUK REPLACEMENT");
	    
	    for (int i = 0; i < nframes; i++) {
		if (i % 5 == 0)
	System.out.println( );
	System.out.print( "\t" + frames[i]);
		
	    }
	    System.out.println();
	 }

}

class LinkedLongHashMap<Integer, Long> extends LinkedHashMap<Integer, Long> {
        protected long defaultValue = 0;

        @Override
        public Long get(Object k) {
                return (Long) (containsKey(k) ? super.get(k) : defaultValue);
        }
}

class LinkedHistMap<Integer, LinkedList> extends
                LinkedHashMap<Integer, LinkedList> {

        protected List list = new java.util.LinkedList();

        public LinkedHistMap() {
                for (int i = 0; i < 10; i++)
                        list.add((long) 0);
        }

        @Override
        public LinkedList get(Object k) {
                return (LinkedList) (containsKey(k) ? super.get(k) : list);
        }
}
