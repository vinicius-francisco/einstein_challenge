# einstein_challenge

## Overview

Este repositório foi criado com o propósito de armazenar o código fonte para as minhas soluções para o Teste para seleção de Engenheiro de Dados do Hospital Albert Einstein

O código foi todo construído usando o [Kedro](https://github.com/quantumblacklabs/kedro), um framework open-source em Python que permite a criação de projetos de dados de maneira muito fácil e que trás inúmeros benefícios. A [documentação](https://kedro.readthedocs.io) é bastante completa e explica detalhadamente as vantagens em usar o Kedro.


## Instruções

É altamente recomendado que você utilize um ambiente virtual.

Para criar um, execute:
```
python3 -m venv env
```
Para ativá-lo, execute:
```
source env/bin/activate
```

Para instalar todas as dependências do projeto, vá ao diretório raiz do repositório e execute:

```
pip install -r src/requirements.txt
```

Para se certificar de que o Kedro está funcionando corretamente, execute:

```
kedro info
```

ATENÇÃO: é presumido que o Spark e todas as suas dependências estejam instalados no seu ambiente para que esse projeto funcione corretamente.


## Como rodar o seu pipeline Kedro

Para rodar os pipelines do Kedro, execute:

```
kedro run
```

Isso fará com que todo o pipeline seja executado. Serão criados dinamicamente todos os arquivos e diretórios intermediários. O único arquivo que é necessário de início é o arquivo .zip inicial, encontrado em data/stage/EINSTEINAgosto.zip

Ao ser executado, o pipeline de dados irá gerar tudo que é pedido nas questões 1, 2 e 3.

Para resolver a questão 4, eu usei um notebook jupyter. Notebooks (encontrados na pasta notebooks) são as únicas exceções num projeto Kedro, no sentido de que não tem dependência com o resto do projeto, não são executados quando o comando 'kedro run' é iniciado. Eles servem justamente para isso, para ser um local de experimentos, uma espécie de sandbox.

## Como trabalhar com notebooks no kedro

> Nota: Usar `kedro jupyter` ou `kedro ipython` para rodar seu notebook te dá as seguintes variáveis: `context`, `catalog`, and `startup_error`. Assim é possível acessar ao data catalog ou aos nós dos pipelines de dentro do seu notebook.

### Jupyter
Para utilizar notebooks Jupyter, é necessário instalar o Jupyter:

```
pip install jupyter
```

Depois de instaldo, você pode iniciar um notebook server:

```
kedro jupyter notebook
```

### JupyterLab
Para utilizar o JupyterLab, você precisa instalá-lo:

```
pip install jupyterlab
```

Iniciando o JupyterLab:

```
kedro jupyter lab
```

### IPython
E se você quiser uma sessão IPython:

```
kedro ipython
```

