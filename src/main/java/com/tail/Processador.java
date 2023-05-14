package com.tail;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class Processador implements Serializable {

  private static final long serialVersionUID = 1L;
  public static final String APP_NAME = "Exercicio 1";
  public static final String MASTER = "local[*]";
  public static final String OUTPUT_FILE = "resultado.txt";
  private static final String INPUT_FILE = "app.log";
  private static final String OUTPUT_DIR = "output";

  /**
   * Executa o processamento realizando as transfomacoes e calculos descritos
   * pelos requisitos R1, R2, R3, R4 e R5.
   */
  public void executar() {
    SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster(MASTER);
    JavaSparkContext context = new JavaSparkContext(conf);

    // realiza os processamentos
    executar(context);

    context.close();
  }

  /**
   * Executa o processamento realizando as transfomacoes e calculos descritos
   * pelos requisitos R1, R2, R3, R4 e R5.
   * 
   * @param context
   * @throws IOException
   */
  public void executar(JavaSparkContext context) {
    // Apaga diretorio e arquivos de output caso existam
    apagarOutputDir();

    // R1. Lê um arquivo de logs que possui duas colunas.
    // Na primeira coluna, os valores podem ser 1 ou 0
    JavaRDD<String> textFile = context.textFile(INPUT_FILE);

    // Mapeia o arquivo com a coluna 1 como chave
    JavaPairRDD<String, String> pairRDD = textFile.flatMapToPair(createPair());

    // R2. Divida o log em duas partes: uma com as linhas começando com 0
    // e outra com as linhas começando com 1
    JavaPairRDD<String, String> logOrdenado = pairRDD.sortByKey();

    // R3. Faça um processador para contar quantas linhas começam com 0
    long countRowsKeyZero = countRowsKeyZero(logOrdenado);

    // R4. Faça um processador para contar as palavras da segunda coluna
    // das linhas que começam com 1
    long countWordsKeyOne = countWordsKeyOne(logOrdenado);

    // R5. Grave o resultado em um arquivo texto

    // Grava os arquivos processados em texto
    logOrdenado.saveAsTextFile(OUTPUT_DIR);

    // Grava dados processados formatados em texto
    saveAsTextFile(countRowsKeyZero, countWordsKeyOne);
  }

  /**
   * Apaga o diretorio de output com os arquivos gerados pelo Spark anteriormente
   */
  private void apagarOutputDir() {
    try {
      FileUtils.deleteDirectory(new File(OUTPUT_DIR));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Quebra as tuplas e separa em pares chave-valor <String, String>
   * 
   * @return Dataset com pares de tuplas
   */
  public PairFlatMapFunction<String, String, String> createPair() {
    return new PairFlatMapFunction<String, String, String>() {
      private static final long serialVersionUID = 1L;

      @Override
      public Iterator<Tuple2<String, String>> call(String t) throws Exception {
        List<Tuple2<String, String>> list = new ArrayList<>();
        int columnIndex = t.indexOf(' ');

        if (columnIndex > -1) {
          list.add(new Tuple2<>(t.substring(0, columnIndex), t.substring(columnIndex + 1)));
        }
        return list.iterator();
      }
    };
  }

  /**
   * Calcula o total de tuplas que iniciam com zero
   * 
   * @param logOrdenado colecao de dados
   * @return total de tuplas
   */
  public long countRowsKeyZero(JavaPairRDD<String, String> logOrdenado) {
    if (logOrdenado == null) {
      return 0;
    }
    return logOrdenado.filter(findByKey("0")).count();
  }

  /**
   * Calcula o total de palavras da segunda coluna que iniciam com 1
   * 
   * @param logOrdenado colecao de dados
   * @return total de palavras
   */
  public long countWordsKeyOne(JavaPairRDD<String, String> logOrdenado) {
    if (logOrdenado == null) {
      return 0;
    }

    return logOrdenado.filter(findByKey("1")).values().mapToDouble(e -> e.split("\\s").length).sum().longValue();
  }

  /**
   * Funcao que busca as tuplas a partir do valor-chave da coluna 1
   * 
   * @param key valor-chave da coluna 1
   * @return funcao de busca
   */
  public Function<Tuple2<String, String>, Boolean> findByKey(String key) {
    return new Function<Tuple2<String, String>, Boolean>() {
      private static final long serialVersionUID = 1L;

      @Override
      public Boolean call(Tuple2<String, String> v1) throws Exception {
        return key != null && key.equals(v1._1);
      }
    };
  }

  /**
   * Salva os dados coletados no processamento em arquivo texto
   * 
   * @param countRowZero     numero de colunas que comecam com zero
   * @param countWordsRowOne numero de palavras da segunda coluna das colunas que
   *                         iniciam com 1
   */
  public void saveAsTextFile(Long countRowZero, Long countWordsRowOne) {
    String texto = String.format("R3=%d%nR4=%d", countRowZero, countWordsRowOne);

    try {
      Files.write(Paths.get(OUTPUT_FILE), texto.getBytes());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
