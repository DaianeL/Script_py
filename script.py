import zipfile
import pandas as pd

# Especifique o caminho para o arquivo ZIP
caminho_arquivo_zip = "dados.zip"

# Abra o arquivo ZIP
lista_arquivos = []
with zipfile.ZipFile(caminho_arquivo_zip, "r") as arquivo_zip:
    # Extraia todos os arquivos para um diretório específico
    arquivos = arquivo_zip.namelist()

    # Exiba os nomes dos arquivos
    for arquivo in arquivos:
        lista_arquivos.append(arquivo)
    arquivo_zip.extractall("arquivos")

origem_dados = pd.read_csv("arquivos/" + lista_arquivos[0])
origem_dados = origem_dados[origem_dados["status"] == "CRITICO"]
origem_dados.sort_values(by=["created_at"], inplace=True)

tipos = pd.read_csv("arquivos/" + lista_arquivos[1])
origem_dados["id"] = origem_dados["tipo"]
origem_dados = origem_dados.merge(tipos, on="id", how="left")
del origem_dados["id"]

with open("insert-dados.sql", "w") as f:
    insert_statement = "INSERT INTO dados_finais (created_at, product_code, customer_code, status, tipo, nome) VALUES\n"
    for _, row in origem_dados.iterrows():
        insert_statement += "('{}', {}, {}, '{}', {}, '{}'),\n".format(
            row["created_at"],
            row["product_code"],
            row["customer_code"],
            row["status"],
            row["tipo"],
            row["nome"],
        )
    insert_statement = insert_statement.rstrip(",\n") + ";"
    f.write(insert_statement)
    print("arquivo insert_dados.sql gerado com sucesso")


QUERY = "SELECT tipo, COUNT(tipo) from dados_finais GROUP BY tipo, DAY(created_at)"