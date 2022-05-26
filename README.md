## Instalação

Para instalar este pipeline, garanta que `conda` e `mamba`:
```
[conda aqui]
conda install -n base conda-forge::mamba
```

Agora clone este repositório:
Clone this repository `ncov`
```
git clone https://github.com/andersonbrito/itps_sgtf.git
```

Uma vez instalados `conda` e `mamba`, acesse o diretório `config`, e execute os seguintes comandos:

```
 mamba create -n diag
 mamba env update -n diag --file environment.yaml
 ```

Por fim, ative o ambiente `diag`:
```
conda activate diag
```
