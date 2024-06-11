import boto3


def processamento_dos_dados():


# definicão do bucket/arquivo e conexão com o s3
    s3_bucket = 'desafio-compass-uol'
    arquivo = 'databases-limpas/salas-de-exibicao-e-complexos-corrigido.csv'
    s3 = boto3.client('s3')


# Script sql
    comandos_sql = open('/home/rafael/Desktop/Comp Sci/CompassUOL/Aulas/Sprint_5/Desafio/S3_select.sql', 'r').read()


# Uso do select_object_content para executar o script sql
    request = s3.select_object_content(
    Bucket=s3_bucket,
    Key=arquivo,
    ExpressionType='SQL',
    Expression=comandos_sql,
    InputSerialization={
        'CSV': {
            'FileHeaderInfo': 'USE',  
            'RecordDelimiter': '\n', 
            'FieldDelimiter': ';'  
        }
    },
    OutputSerialization={
        'JSON': {
            'RecordDelimiter': '\n'
        }
    }
)


# Print dos resultados
    for evento in request['Payload']:
            if 'Records' in evento:
                registros = evento['Records']['Payload'].decode('utf-8')
                print(registros)


if __name__ == '__main__':
    processamento_dos_dados()
