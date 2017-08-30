import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;


public class main {

    private static final String FILENAME = "correlation.txt";
    private static final String FILEMOVIE = "movies.csv";
    public static void main(String[] args) {

        BufferedReader br = null;
        BufferedReader mr = null;
        FileReader nr = null;
        FileReader fr = null;
        String[] infos;
        String[] infos2;
        String[] id_movie;

        try {

            //br = new BufferedReader(new FileReader(FILENAME));
            fr = new FileReader(FILENAME);
            br = new BufferedReader(fr);
            nr = new FileReader(FILEMOVIE);
            mr = new BufferedReader(nr);

            String sCurrentLine;
            float f;
            int i, j;
            int totalLines;
            int TotalCorreactions = 10987079;
            int cols = 3;
            int totalmovies = 9125;
            int movies_cols = 2;
            double Treshold = 0.99;
            float[][] correlation_matrix = new float[TotalCorreactions][cols];
            String[][] movies_matrix = new String[totalmovies][movies_cols];


            for(i = 0; i < TotalCorreactions; i++){
                sCurrentLine = br.readLine();
                infos = sCurrentLine.split(";");
                infos2 = infos[1].split("\t");
                correlation_matrix[i][0] = Float.parseFloat(infos[0]);
                correlation_matrix[i][1] = Float.parseFloat(infos2[0]);
                correlation_matrix[i][2] = Float.parseFloat(infos2[1]);

            }

            System.out.println("Digita o código de um filme que você gostou");
            Scanner in = new Scanner(System.in);
            int id_search = in.nextInt();
            System.out.println();

            int[] predict_movie = new int[5];
            for (i = 0; i < 5; i++) {
              predict_movie[i] = 0;
            }
            int k = 0;
            for (i = 0; i < TotalCorreactions; i++) {
                if(correlation_matrix[i][0] == id_search && (correlation_matrix[i][2] > Treshold)) {
                    predict_movie[k] = (int)correlation_matrix[i][1];
                    k++;
                    if(k == 5) {
                      break;
                    }

                }
                if(correlation_matrix[i][1] == id_search && (correlation_matrix[i][2] > Treshold)) {
                    predict_movie[k] = (int)correlation_matrix[i][0];
                    k++;
                    if(k == 5) {
                      break;
                    }
                }
            }
            for (i  = 0; i < totalmovies; i++) {
                sCurrentLine = mr.readLine();
                id_movie = sCurrentLine.split(",");
                if(Integer.parseInt(id_movie[0]) == predict_movie[0]) {
                    System.out.println("Filmes Recomendados: " + id_movie[1]);
                }
                if(Integer.parseInt(id_movie[0]) == predict_movie[1]) {
                    System.out.println("Filmes Recomendados: " + id_movie[1]);
                }
                if(Integer.parseInt(id_movie[0]) == predict_movie[2]) {
                    System.out.println("Filmes Recomendados: " + id_movie[1]);
                }
                if(Integer.parseInt(id_movie[0]) == predict_movie[3]) {
                    System.out.println("Filmes Recomendados: " + id_movie[1]);
                }
                if(Integer.parseInt(id_movie[0]) == predict_movie[4]) {
                    System.out.println("Filmes Recomendados: " + id_movie[1]);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null)
                    br.close();
                if (fr != null)
                    fr.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }
}
