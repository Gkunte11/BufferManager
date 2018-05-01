package bufmgr;

import global.GlobalConst;
import global.Minibase;
import global.Page;
import global.PageId;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * <h3>Minibase Buffer Manager</h3>
 * The buffer manager manages an array of main memory pages.  The array is
 * called the buffer pool, each page is called a frame.  
 * It provides the following services:
 * <ol>
 * <li>Pinning and unpinning disk pages to/from frames
 * <li>Allocating and deallocating runs of disk pages and coordinating this with
 * the buffer pool
 * <li>Flushing pages from the buffer pool
 * <li>Getting relevant data
 * </ol>
 * The buffer manager is used by access methods, heap files, and
 * relational operators.
 */
public class BufMgr implements GlobalConst {

  /**
   * Constructs a buffer manager by initializing member data.  
   * 
   * @param numframes number of frames in the buffer pool
   */
	
	 ArrayList<FrameDesc> bufferpool;
	HashMap<PageId, FrameDesc> bufmap;
	Clock clock;
	
  public BufMgr(int numframes) {
	  //Initializing the buffer pool as an array of frame objects.
	  bufferpool = new ArrayList<FrameDesc>();
	  
	  //Adding frames descriptors to frames of buffer pool
	  for (int i = 0; i < numframes; ++i) {
		  bufferpool.add(new FrameDesc());
	  }

	  //Mapping of disk page number to the frame descriptor
	  bufmap = new HashMap<PageId, FrameDesc>();
	  
	  //Creating clock object, where the clock class contains the implemented clock replacement policy.
	  clock = new Clock();
	  
  } 

  /**
   * The result of this call is that disk page number pageno should reside in
   * a frame in the buffer pool and have an additional pin assigned to it, 
   * and mempage should refer to the contents of that frame. <br><br>
   * 
   * If disk page pageno is already in the buffer pool, this simply increments 
   * the pin count.  Otherwise, this<br> 
   * <pre>
   * 	uses the replacement policy to select a frame to replace
   * 	writes the frame's contents to disk if valid and dirty
   * 	if (contents == PIN_DISKIO)
   * 		read disk page pageno into chosen frame
   * 	else (contents == PIN_MEMCPY)
   * 		copy mempage into chosen frame
   * 	[omitted from the above is maintenance of the frame table and hash map]
   * </pre>		
   * @param pageno identifies the page to pin
   * @param mempage An output parameter referring to the chosen frame.  If
   * contents==PIN_MEMCPY it is also an input parameter which is copied into
   * the chosen frame, see the contents parameter. 
   * @param contents Describes how the contents of the frame are determined.<br>  
   * If PIN_DISKIO, read the page from disk into the frame.<br>  
   * If PIN_MEMCPY, copy mempage into the frame.<br>  
   * If PIN_NOOP, copy nothing into the frame - the frame contents are irrelevant.<br>
   * Note: In the cases of PIN_MEMCPY and PIN_NOOP, disk I/O is avoided.
   * @throws IllegalArgumentException if PIN_MEMCPY and the page is pinned.
   * @throws IllegalStateException if all pages are pinned (i.e. pool is full)
   */
  public void pinPage(PageId pageno, Page mempage, int contents) {

	  // Gets the frame information from the buffer pool
	  FrameDesc frameinfo = bufmap.get(pageno);
		  
	  //check if the frame containing the page is present in the buffer pool, if it is present increment the pin count
	  if(frameinfo != null) {
          frameinfo.pin_count++;

      } else {
    	  	  // selecting the current frame from buffer pool
		  FrameDesc victimFrm;
		  if (!bufferpool.isEmpty()) {
			  victimFrm = bufferpool.get(bufferpool.size()-1);
			  bufferpool.remove(victimFrm);
		  } else {
			  // If the buffer pool is full, use the clock replacement policy to 
			  //select a victim frame to be removed from the buffer pool
			  victimFrm = clock.pickVictim(this);
			  //if all pages are pinned , throw IllegalStateException.
			  if (victimFrm == null) {
				  throw new IllegalStateException("All pages are pinned (pool is full)!");
			  } else {
				  // If the page is modified , check if its dirty and flush it to disk
				  if (victimFrm.dirty) {
					  flushPage(pageno, victimFrm);
				  }
			  }

			  //clear the mapping of the chosen victim page and the frame descriptor
			  bufmap.remove(pageno);

			  //Resetting all the states associated to the frame.
			  victimFrm.pin_count = 0;
			  victimFrm.valid = false;
			  victimFrm.dirty = false;
			  victimFrm.refbit = false;
		  }

		  	 
		     //if the new page contents are present in the disk, read it from the disk and 
		     //copy the page from the disk to the chosen frame.
			  if(contents == PIN_DISKIO) {  
		          Minibase.DiskManager.read_page(pageno, victimFrm);
		    
		          victimFrm.pin_count ++;
		          victimFrm.valid = true;
		          victimFrm.dirty = false;
				  victimFrm.pageno = new PageId();
		          victimFrm.pageno.copyPageId(pageno);
		          victimFrm.refbit = true;
		          
		          bufmap.put(victimFrm.pageno, victimFrm);
		          mempage.setData(victimFrm.getData());
			  }
			//if the new page contents are present in the memory, copy the page from
			//the memory to the chosen frame.
			  else if (contents == PIN_MEMCPY) {
				
				  
				  victimFrm.pin_count++;
		          victimFrm.valid = true;
		          victimFrm.dirty = false;
				  victimFrm.pageno = new PageId();
		          victimFrm.pageno.copyPageId(pageno);
		          victimFrm.refbit = true;
	              

		          bufmap.put(victimFrm.pageno, victimFrm);
		          victimFrm.setPage(mempage);
			  }
			  //If contents are PIN_NOOP, copy nothing into the frame - the frame contents are irrelevant
			  else if(contents == PIN_NOOP) {
				
                  victimFrm.pin_count++;
                  victimFrm.valid = true;
                  victimFrm.dirty = false;
                  victimFrm.pageno = new PageId(pageno.pid);
                  victimFrm.refbit = true;

                  bufmap.put(pageno, victimFrm);
			  }
			  }
		  }
	  
 // } 
  
  /**
   * Unpins a disk page from the buffer pool, decreasing its pin count.
   * 
   * @param pageno identifies the page to unpin
   * @param dirty UNPIN_DIRTY if the page was modified, UNPIN_CLEAN otherwise
   * @throws IllegalArgumentException if the page is not in the buffer pool
   *  or not pinned
   */
  public void unpinPage(PageId pageno, boolean dirty) {

	  	  // Gets the frame information from the buffer pool
		  FrameDesc frameinfo = bufmap.get(pageno);
		  // throw IllegalArgumentException if the frame is not present in the buffer pool 
		  // or if the page is not pinned.
		  if(frameinfo == null || frameinfo.pin_count == 0)
			{
			  throw new IllegalArgumentException("Page is not in the buffer pool or not pinned!"+ "P###"
					  + pageno.toString() + ":" + pageno.pid);
			}
			else
			{
			// if the page is present in the buffer pool, check if it is dirty and decrement the pincount
			//if the pin count is zero after decrementing, set the reference bit to 1
				if(dirty)
					frameinfo.dirty = UNPIN_DIRTY; 
				else
					frameinfo.dirty = UNPIN_CLEAN; 
				frameinfo.pin_count--;
				
				if (frameinfo.pin_count == 0)
			      {
			        frameinfo.refbit = true;
			      }
			}
  } 
  
  /**
   * Allocates a run of new disk pages and pins the first one in the buffer pool.
   * The pin will be made using PIN_MEMCPY.  Watch out for disk page leaks.
   * 
   * @param firstpg input and output: holds the contents of the first allocated page
   * and refers to the frame where it resides
   * @param run_size input: number of pages to allocate
   * @return page id of the first allocated page
   * @throws IllegalArgumentException if firstpg is already pinned
   * @throws IllegalStateException if all pages are pinned (i.e. pool exceeded)
   */
  public PageId newPage(Page firstpg, int run_size) {
	  //allocates a new disk page
	  PageId pageno = new PageId();
	  // Gets the frame information from the buffer pool
	  FrameDesc frameinfo = bufmap.get(pageno);
	  
	  //check if all the pages in the buffer pool is pinned or not.
	  if(getNumUnpinned() == 0) {
		  throw new IllegalStateException("all pages are pinned");
	  }
	  
	  //throw IllegalArgumentException by checking if the page is present in the 
	  //buffer pool and if the page is already pinnned.
	  else if (frameinfo != null && frameinfo.pin_count > 0) {
		  throw new IllegalArgumentException("firstpg is already pinned");
	  }
	  //allocate the page in the disk and pin the page in the buffer pool
	  else {
		  pageno.pid = Minibase.DiskManager.allocate_page(run_size).pid;
		  pinPage(pageno, firstpg, PIN_MEMCPY);
	  }
	  
	 
	  return pageno;

  } 

  /**
   * Deallocates a single page from disk, freeing it from the pool if needed.
   * 
   * @param pageno identifies the page to remove
   * @throws IllegalArgumentException if the page is pinned
   */
  public void freePage(PageId pageno) {
	  	//obtain frame information of the page
		FrameDesc frameinfo = bufmap.get(pageno);
		//throw IllegalArgumentException by checking if the page is present in the 
		  //buffer pool and if the page is already pinnned.
		if(frameinfo != null && frameinfo.pin_count > 0) {
			throw new IllegalArgumentException("The page is pinned!");
		}
		// if the page is present in the buffer pool, free it from the buffer pool
		//and deallocate the page from the disk 
		else {
			if(bufmap.containsKey(pageno.pid)) {
				bufmap.remove(pageno);
			}
			Minibase.DiskManager.deallocate_page(pageno);
			
		}

  } 

  /**
   * Write all valid and dirty frames to disk.
   * Note flushing involves only writing, not unpinning or freeing
   * or the like.
   * 
   */
  public void flushAllFrames() {
	
	  //We need to write all valid and dirty frames to the disk
	  for (Iterator map = bufmap.entrySet().iterator(); map.hasNext(); ) {
		
		  Map.Entry it = (Map.Entry) map.next();
		  PageId key = (PageId) it.getKey();
		  FrameDesc value = (FrameDesc) it.getValue();
		  map.remove();
		  if (value.valid && value.dirty) {
			  flushPage(key, value);
		  }
		}
  }
	  
  /**
   * Write a page in the buffer pool to disk, if dirty.
   * 
   * @throws IllegalArgumentException if the page is not in the buffer pool
   */
  
  
  public void flushPage(PageId pageno, FrameDesc frameinfo) {
	  	//check if the frame is present in the buffer pool
		if(frameinfo != null)
		{
			//if the page is present, check if its dirty.
			//if it is dirty write the page back to disk.
			if (frameinfo.dirty == true) {
				
				Minibase.DiskManager.write_page(pageno, frameinfo);
			}
		}
		else
		{
			//IllegalArgumentException if the page is not in the buffer pool
			 throw new IllegalArgumentException("Page is not in the buffer pool!");
		}
	
  }

   /**
   * Gets the total number of buffer frames.
   */
  public int getNumFrames() {
	  
	  return bufmap.size();

  }

  /**
   * Gets the total number of unpinned buffer frames.
   */
  public int getNumUnpinned() {

      int unpinned = 0;

      // if buffer pool is not empty all the pages in the buffer pool are unpinned.
      if (!bufferpool.isEmpty()) {
          unpinned += bufferpool.size();
      }

      for (Iterator map = bufmap.entrySet().iterator(); map.hasNext(); ) {
    	  Map.Entry it = (Map.Entry) map.next();
          PageId key = (PageId) it.getKey();
          FrameDesc value = (FrameDesc) it.getValue();
          if (value.pin_count == 0) {
              unpinned++;
          }
      }
      return unpinned;
  }

} 
