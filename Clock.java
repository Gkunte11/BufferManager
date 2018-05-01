package bufmgr;

import global.GlobalConst;
import global.PageId;

import java.util.*;


public class Clock implements GlobalConst {

	protected List<FrameDesc> frametab;
	protected int current;
	 
	public Clock() {

		this.frametab = null;
		this.current = 0;
	}



    public FrameDesc pickVictim(BufMgr bm) {

    		//frametab stores the various states in a structure.
        this.frametab = new ArrayList<FrameDesc>(bm.bufmap.values());

        //we iterate through twice of the buffer pool because, we give the frame a second chance,
        //by changing the reference bit to 0 in the first iteration.
        for (int i = 0; i < (frametab.size() * 2); i++) {
        	//for clock replacement policy we need to check if the page is valid.
        //If the current frame's state is invalid , the frame will be chosen.
            if (frametab.get(current).valid != true) {
                return frametab.get(current);
            }
           
            else {
            		//if the frame is valid , check if the pin count is zero.
                if (frametab.get(current).pin_count == 0) {
                    //if the pincount is zero, check it's reference bit.
                		//if the referece bit is set to 1, change it to 0 and proceed.
                    if (frametab.get(current).refbit) {
                        frametab.get(current).refbit = false;
                    } else {
                    	//if the reference bit is zero, choose the frame as the victim frame.
                        return frametab.get(current);
                    }
                }
            }
            
            //if the iteration goes beyond the buffer size, start from the begining.
         	current = (current+1) % frametab.size();
            
        }

       // return null if no victim frame found.
        return null;
    }
}