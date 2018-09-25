package colgatedb.page;

import colgatedb.tuple.Field;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.TupleDesc;
import java.lang.Math;
import java.util.BitSet;

import java.io.*;

/**
 * ColgateDB
 * @author Michael Hay mhay@colgate.edu
 * <p>
 * ColgateDB was developed by Michael Hay but borrows considerably from past
 * efforts including SimpleDB (developed by Sam Madden at MIT) and its predecessor
 * Minibase (developed at U. of Wisconsin by Raghu Ramakrishnan).
 * <p>
 * The contents of this file are either wholly the creation of Michael Hay or are
 * a significant adaptation of code from the SimpleDB project.  A number of
 * substantive changes have been made to meet the pedagogical goals of the cosc460
 * course at Colgate.  If this file contains remnants from SimpleDB, we are
 * grateful for Sam's permission to use and adapt his materials.
 */

/**
 * SlottedPageFormatter is responsible for translating a SlottedPage to and from a
 * byte array representation.
 * <p>
 * A SlottedPage has an array of slots, each of which can hold one tuple and all tuples have the same exact size,
 * that is determined by the TupleDesc for this page.
 * <p>
 * The page format has three components:
 * (a) header
 * (b) payload
 * (c) zeroed out excess bytes
 * The header is a bitmap, with one bit per tuple slot. If the bit corresponding to a particular slot is 1, it
 * indicates that the slot is occupied; if it is 0, the slot is considered empty.
 * <p>
 * The layout of the header requires some explanation.  The first byte of the header represents slots 0..7, the second
 * byte is slots 8..15, and so on.  However, within a byte, the least significant bit of represents the lowest slot
 * value. Thus, suppose the first byte looked like this:
 * bits:  10010110
 * this indicates that slots 1, 2, 4, and 7 are occupied and slots 0, 3, 5, and 6 are empty.  In other words, the bits
 * for the slots are arranged according to following pattern:
 *
 * 7,6,5,4,3,2,1,0  15,14,13,12,11,10,9,8  23,22,21,20,19,18,17,16  and so on.
 *
 * <p>
 * The payload is the data itself.  The tuples of the page are written out in slot order from slot 0 to slot N-1 where
 * N is the number of slots on the page.  If the slot is occupied, the bytes for that slot consist of the data for each
 * field in th tuple, written out in order.  Let k be the number of bytes required to store a tuple.  If the slot is
 * empty slot, then k bytes of zeros are written out.
 * <p>
 * After the last slot is written, there may be excess bytes.  These are just zeroed out.
 */
public class SlottedPageFormatter {

    /**
     * The tuple capacity is computed as follows:
     * - Each tuple has a tuple size (determined by the TupleDesc), which is measured in bytes.
     * - There are 8 bits in a byte.
     * - Additionally, each tuple requires 1 bit in header.
     * - Thus, given the pageSize (measured in bytes) we can store at most.
     *     floor((pageSize *8) / (tuple size * 8 + 1))
     *   tuples on a page.
     * @return number of tuples that this page can hold
     */
    public static int computePageCapacity(int pageSize, TupleDesc td) {
        int capacity = (pageSize * 8)/ (td.getSize() * 8 + 1);
        return (int) Math.floor(capacity);
    }

    /**
     * The size of the header is the number of bytes needed to store the header given that
     * each slot requires one bit.  This is equal to ceiling( numSlots / 8 ).
     *
     * @param numSlots
     * @return the size of the header in bytes.
     */
    public static int getHeaderSize(int numSlots) {
        double size = Math.ceil(numSlots/8.0);
        return (int) size;
    }

    /**
     * Write out the page to bytes.  See the javadoc at the top of file for byte format description.
     * @param page the page to write
     * @param td the TupleDesc that describes the tuples on the page
     * @param pageSize the size of the page
     * @return
     */
    public static byte[] pageToBytes(SlottedPage page, TupleDesc td, int pageSize) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(pageSize);
            DataOutputStream dos = new DataOutputStream(baos);
            // write out the page data to the DataOutputStream dos, starting with the header then the tuples
            // see the Javadocs for DataOutputStream for handy methods
            // to write out the data of a Field use the Field.serialize method
            // also, you may find it the markSlot method very useful here


            int numSlots = page.getNumSlots();
            int headerSize = getHeaderSize(numSlots);
            byte[] header = new byte[headerSize];

            //iterate through the slots and mark the bit in the header depending on weather the slot is used or not
            for (int i = 0; i < numSlots; i++){
                if(page.isSlotUsed(i)) {
                    markSlot(i, header);
                }
            }
            dos.write(header,0,headerSize);

            //iterate through the pages
            //if the slot is used, get the tuple and write the fields to the dos
            // if the pages are empty add 0s

            for (int i = 0; i < numSlots; i++){
                if(page.isSlotUsed(i)) {
                    Tuple t = page.getTuple(i);
                    TupleDesc tdTemp = t.getTupleDesc();
                    for (int j = 0; j < tdTemp.numFields(); j++) {
                        Field field = t.getField(j);
                        field.serialize(dos);
                    }
                }
                else {
                    byte[] b =  new byte[td.getSize()];
                    for (int idx = 0; idx < td.getSize(); idx++){
                        b[idx] = 0;
                    }
                    dos.write(b);
                }
            }
            byte[] excess = new byte[pageSize - dos.size()];
            for(int i = 0; i < pageSize - dos.size(); i++ ){
                excess[i] = 0;
            }
            dos.write(excess);
            return baos.toByteArray();
        } catch (Exception e) {
            throw new PageException(e);
        }
    }

//    // Helper function that will set the bit in the byte array to the desired value
//    private static void setBit(byte[] data, int pos, int val) {
//        int posByte = pos/8;
//        int posBit = (pos%8)%8;
//        byte oldByte = data[posByte];
//        oldByte = (byte) (((0xFF7F>>posBit) & oldByte) & 0x00FF);
//        byte newByte = (byte) ((val<<(8-(posBit+1))) | oldByte);
//        data[posByte] = newByte;
//    }

    /**
     * Populate the empty page with data that is read from the given bytes.  See the javadoc at the top of file
     * for byte format description.
     * @param bytes bytes representing page data
     * @param emptyPage an initially emptyPage to be populated
     * @param td the TupleDesc of tuples on this page
     */
    public static void bytesToPage(byte[] bytes, SlottedPage emptyPage, TupleDesc td) {

        try {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
            // read in the data DataInputStream dis, first the header then the bytes of each tuple
            // see the Javadocs for DataInputStream for handy methods
            // to read the data associated with a Field in a tuple, use the Type.parse method
            // also, you may find it the isSlotUsed method very useful here
            dis.close();


//            byte[] test = new byte[1];
//            test[0] = 1;
//            System.out.println("is slot used: " + isSlotUsed(0, test));

            int numSlots = emptyPage.getNumSlots();
            int headerSize = getHeaderSize(numSlots);
            byte[] header = new byte[headerSize];

            int numFields = td.numFields();

            dis.read(header, 0, headerSize);
//            for(int i = 0; i < header.length; i++){
//                System.out.println(header[i]);
//            }

            for (int i = 0; i < numSlots; i++){
                if (isSlotUsed(i, header)){
                    Tuple tuple = new Tuple(td);
                    for(int j = 0; j < numFields; j++) {
                        Field f = td.getFieldType(j).parse(dis);
                        tuple.setField(j, f);
                    }
                    emptyPage.insertTuple(i, tuple);
                }
                else{
                    dis.skipBytes(td.getSize());
                }
            }

        } catch (IOException e) {
            throw new PageException(e);
        }
    }


    /**
     * Checks whether a slot in the header is used or not.  Optional helper method.
     * @param i slot index to check
     * @param header a byte header, formatted as described in the javadoc at the top.
     * @return
     */
    private static boolean isSlotUsed(int i, byte[] header) {
        int bytePos = i/8;
        int bitPos = (i % 8);
        byte b = header[bytePos];
        b = (byte) (b & (byte) Math.pow(2, bitPos));
        return b >= (byte) 1;
    }

    /**
     * Marks a slot in the header as used or not.  Optional helper method.
     * @param i slot index
     * @param header a byte header, formatted as described in the javadoc at the top.
     * //@param isUsed if true, slot should be set to 1; if false, set to 0
     */
    private static void markSlot(int i, byte[] header) {
        int posByte = i / 8;
        int posBit =  i % 8;

        header[posByte] = (byte) (header[posByte]|(byte) Math.pow(2, posBit));

    }
}
