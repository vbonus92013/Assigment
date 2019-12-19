package local.collectfileimage;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import javax.activation.MimetypesFileTypeMap;
//import java.util.Scanner;

class CollectPathFile{

	public static void main(String[] args) {
	
		//System.out.println(executeCommand("pwd"));
//		Scanner reader = new Scanner(System.in); 
//		System.out.print("Usages: $[path/to/folder/]: ");
//		String path = reader.nextLine();
//		reader.close();
		//String newpath = "../../../../"+path;
		try{
			final File folder = new File(args[0]);
			PrintWriter writer = new PrintWriter(args[1], "UTF-8");
			listFilesForFolder(folder, writer);
			writer.close();
		}
		catch(Exception e){
			System.out.println("Usages: $ java CollectPathFile [path/to/folder/] [output]");
		}
	}
	public static void listFilesForFolder(final File folder, PrintWriter writer) {
    for (final File fileEntry : folder.listFiles()) {
        if (fileEntry.isDirectory()) {
            listFilesForFolder(fileEntry, writer);
        } else {
        	MimetypesFileTypeMap mimetypesFileTypeMap = new MimetypesFileTypeMap();
        	mimetypesFileTypeMap.addMimeTypes("image png tif jpg jpeg bmp webp hdr bat bpg svg");
			
			//String namefile = fileEntry.getName().replaceAll(" ", "\\\\ ");

      		String mimetype= mimetypesFileTypeMap.getContentType(fileEntry.getName());

      		String type = mimetype.split("/")[0];

      		if(type.equals("image")){

	        	//System.out.println(fileEntry.getParent()+"/"+namefile);
	        	//File file = new File(fileEntry.getParent()+"/"+namefile);
	        	//fileEntry.renameTo(file);

	        	//writer.println(fileEntry.getParent()+"/"+namefile);
	        	writer.println(fileEntry.getPath());
	            
      		}
      		else{
      			try{
      				System.out.println(fileEntry.getAbsolutePath()+" is not a image!");
      			}
      			catch(Exception e){}
      		}
        }
    }
	}
	@SuppressWarnings("unused")
	private static String executeCommand(String command) {

		StringBuffer output = new StringBuffer();

		Process p;
		try {
			p = Runtime.getRuntime().exec(command);
			p.waitFor();
			BufferedReader reader = 
                            new BufferedReader(new InputStreamReader(p.getInputStream()));

                        String line = "";			
			while ((line = reader.readLine())!= null) {
				output.append(line + "\n");
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return output.toString();

	}


}