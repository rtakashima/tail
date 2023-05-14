package com.tail;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import scala.Tuple2;

public class Exercicio1Test {

  private JavaSparkContext context;
  private Processador processador;
  private List<String> inputData = Arrays.asList("0 Linha1", "1 Linha2", "0 Linha3", "1 Linha4");

  @BeforeEach
  public void init() {
    SparkConf conf = new SparkConf().setAppName(Processador.APP_NAME).setMaster(Processador.MASTER);
    context = new JavaSparkContext(conf);
    processador = new Processador();
  }

  @AfterEach
  public void onFinish() {
    if (context != null) {
      context.close();
    }
  }

  @Test
  void testSplitPairFunction() {
    // dados esperados
    List<Tuple2<String, String>> pairData = Arrays.asList(
        new Tuple2<String, String>("0", "Linha1"),
        new Tuple2<String, String>("1", "Linha2"), 
        new Tuple2<String, String>("0", "Linha3"),
        new Tuple2<String, String>("1", "Linha4"));

    // dados de entrada
    JavaRDD<String> data = context.parallelize(inputData);

    // separa os dados usando a funcao pair
    JavaPairRDD<String, String> flatMapToPair = data.flatMapToPair(processador.createPair());

    // verifica o resultado
    assertEquals(pairData, flatMapToPair.collect());
  }

  @Test
  void testCountRowsByKeyZero() {
    // dados de entrada
    JavaRDD<String> data = context.parallelize(inputData);

    // separa os dados usando a funcao pair
    JavaPairRDD<String, String> flatMapToPair = data.flatMapToPair(processador.createPair());

    // verifica o resultado
    assertEquals(2l, processador.countRowsKeyZero(flatMapToPair));
  }

  @Test
  void testCountWordsByKeyOne() {
    // dados de entrada
    JavaRDD<String> data = context.parallelize(inputData);

    // separa os dados usando a funcao pair
    JavaPairRDD<String, String> flatMapToPair = data.flatMapToPair(processador.createPair());

    // verifica o resultado
    assertEquals(2l, processador.countRowsKeyZero(flatMapToPair));
  }

  @Test
  void testSavedFileContent() {
    // executa o processamento dos dados
    processador.executar(context);

    // coleta os dados do arquivo gerado no output
    List<String> dados = context.textFile(Processador.OUTPUT_FILE).collect();

    // verifica o resultado
    assertEquals("R3=6", dados.get(0));
    assertEquals("R4=18", dados.get(1));
  }

}
