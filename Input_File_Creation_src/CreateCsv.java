package csv;

/*
* csv file generator
* 
* Χριστάκης Γεώργιος 1559
* Γεωργιάδης Χρήστος Ανέστης 2459
* Χαντζής Ιωάννης 1551
*/

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Random;

public class CreateCsv {

public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
		
		String alphabet = "abcdefghijklmnopqrstuvwxyz";
		int N = alphabet.length();
		Random r = new Random();
		//Creates the users.csv file giving a unique id between 1-10000 and a random name per user
		PrintWriter writer1 = new PrintWriter("users.csv", "UTF-8");
		for(int i=1;i<=10000;++i){
			writer1.write(i + "," + alphabet.charAt(r.nextInt(N)) + "\n");
		}
		writer1.close();
		//Creates the transactions.csv file, in the first for loop we gine to each user one transaction so that
		//there won't be any user without one,
		//In the second loop we assign the rest 10000 transactions randomly to the users.
		PrintWriter writer2 = new PrintWriter("transactions.csv", "UTF-8");
		for(int i=1;i<=10000;++i){
			writer2.write(i + "," + (10001-i) + "," + alphabet.charAt(r.nextInt(N)) + "\n");
		}
		for(int i=10001;i<=20000;++i){
			writer2.write(i + "," + r.nextInt(10000) + "," + alphabet.charAt(r.nextInt(N)) + "\n");
		}
		writer2.close();
	}
}
