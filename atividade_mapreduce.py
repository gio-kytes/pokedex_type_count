import csv
import findspark
from pyspark import SparkContext

# Inicializando o SparkContext
findspark.init()
sc = SparkContext("local", "WordCountExample")


def main():
        # Abertura do arquivo
        with open("./files/Pokedex_Cleaned.csv", 'r') as file:
            csv_reader = csv.reader(file, delimiter=',')
            
            # Realizando a leitura apenas da coluna "Type_Concatenated"
            header = next(csv_reader)
            type_index = header.index("Type_Concatenated")
            
            # Definindo uma lista e atribuindo os valores da coluna
            text = []
            for row in csv_reader:
                text.append(row[type_index])
            
            # Transformando a lista em uma string
            string_text = ",".join(text)
           
            # O restante do código foi reaproveitado do exemplo
            # Criando um RDD a partir do texto
            input_rdd = sc.parallelize([string_text])

            # Passo de mapeamento (alterado o delimitador do split para vírgula)
            mapped_rdd = input_rdd.flatMap(lambda line: line.split(",")).map(lambda word: (word, 1))

            # Passo de redução
            reduced_rdd = mapped_rdd.reduceByKey(lambda a, b: a + b)

            # Exibindo os resultados
            results = reduced_rdd.collect()
            for word, count in results:
                print(f"{word}: {count}")

            # Encerrando o SparkContext
            sc.stop()
        
main()
