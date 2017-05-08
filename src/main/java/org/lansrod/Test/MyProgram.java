package org.lansrod.Test;
import java.io.*;
class MyProgram 
{
    public static void main(String args[]) throws IOException
    {
        FileOutputStream out = null;
        try 
        {
            out = new FileOutputStream("test.txt");
            out.write(122);
        }
        catch(IOException io) 
        {
            System.out.println("IO Error.");
        }
      
    }
}
