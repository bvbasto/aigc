

-- truncate table `bs-fdld-ai`.`rag_dataset_eu`.`testFomCode` 

--
SELECT *  FROM  `bs-fdld-ai`.`rag_dataset_eu`.`testFomCode` 
where  docPage_ini = 16
LIMIT 1000;

SELECT distinct docname  FROM  `bs-fdld-ai`.`rag_dataset_eu`.`testFomCode` 


SELECT *  FROM  `bs-fdld-ai`.`rag_dataset_eu`.`testFomCode` 


update `bs-fdld-ai`.`rag_dataset_eu`.`testFomCode` 
set docname = 'Seguro automóvel'
where docname not like '%Fidelidade_DL_Final_Hifenizada%'


update `bs-fdld-ai`.`rag_dataset_eu`.`testFomCode` 
set docname = 'Seguro Saude'
where docname not like 'Seguro automóvel'


update `bs-fdld-ai`.`rag_dataset_eu`.`testFomCode`  set
BlocoContent = 
"""1.1. Tabela de Entrada – Classe de Bónus/Malus no início do contrato
N.º DE ANOS COM SEGURO | SINISTROS NOS ÚLTIMOS 5 ANOS: 0 e anos sem sinistro: 0 | SINISTROS NOS ÚLTIMOS 5 ANOS: 1 e anos sem sinistro: 0| SINISTROS NOS ÚLTIMOS 5 ANOS: 1 e anos sem sinistro: 1 a 2 | SINISTROS NOS ÚLTIMOS 5 ANOS: 1 e anos sem sinistro: 3 a 4 | SINISTROS NOS ÚLTIMOS 5 ANOS: 2 e anos sem sinistro: 0| SINISTROS NOS ÚLTIMOS 5 ANOS: 2 e anos sem sinistro: 1 a 2 | SINISTROS NOS ÚLTIMOS 5 ANOS: 2 e anos sem sinistro: 3 a 4 |
-- | -- | -- | -- | -- | -- | -- | -- |
0 | 0% | 40% | | | 90% | | | 
1 | -10% | 20% | 20% | | 60% | 60% | | 
2 | -20% | 10% | 10% | | 60% | 40% | | 
3 | -25% | 0% | 0% | 0% | 40% | 20% | 20% | 
4 | -30% | 0% | -10% | -10% | 20% | 10% | 10% | 
5 | -32,5% | -10% | -20% | -20% | 10% | 0% | 0% | 
6 | -35% | -10% | -20% | -25% | 10% | 0% | -10% | 
7 | -37,5% | -10% | -20% | -25% | 10% | 0% | -10% | 
8 | -37,5% | -20% | -25% | -30% | 10% | 0% | -10% | 
9 | -40% | -20% | -25% | -30% | 0% | 0% | -20% |
10 | -42% | -20% | -25% | -30% | 0% | 0% | -20% | 
11 | -42% | -20% | -25% | -30% | 0% | 0% | -20% | 
12 | -44% | -20% | -25% | -30% | 0% | 0% | -20% | 
13 | -46% | -25% | -30% | -32,5% | 0% | 0% | -20% | 
14 | -48% | -25% | -30% | -32,5% | 0% | -10% | -20% | 
15 | -50% | -30% | -30% | -32,5% | 0% | -10% | -20% | 
16 | -50% | -32,5% | -32,5% | -35% | 0% | -10% | -20% | 
17 | -50% | -32,5% | -32,5% | -35% | 0% | -10% | -20% | 
18 OU + | -50% | -35% | -35% | -35% | 0% | -10% | -20% | """
where  docPage_ini = 16












1.1. Tabela de Entrada – Classe de Bónus/Malus no início do contrato
N.º DE ANOS COM SEGURO | N.º DE SINISTROS NOS ÚLTIMOS 5 ANOS
| 0 | 1 | 2 | 3 OU MAIS
| | N° DE ANOS SEM SINISTROS | CASUÍSTICO
| | 0 | 1 a 2 | 3 a 4 | 0 | 1 a 2 | 3 a 4 |
0 | 0% | 40% | | 90% | | |
1 | -10% | 20% | 20% | | 60% | 60% | |
2 | -20% | 10% | 10% | | 60% | 40% | |
3 | -25% | 0% | 0% | 0% | 40% | 20% | 20% |
4 | -30% | 0% | -10% | -10% | 20% | 10% | 10% |
5 | -32,5% | -10% | -20% | -20% | 10% | 0% | 0% |
6 | -35% | -10% | -20% | -25% | 10% | 0% | -10% |
7 | -37,5% | -10% | -20% | -25% | 10% | 0% | -10% |
8 | -37,5% | -20% | -25% | -30% | 10% | 0% | -10% |
9 | -40% | -20% | -25% | -30% | 0% | 0% | -20% |
10 | -42% | -20% | -25% | -30% | 0% | 0% | -20% |
11 | -42% | -20% | -25% | -30% | 0% | 0% | -20% |
12 | -44% | -20% | -25% | -30% | 0% | 0% | -20% |
13 | -46% | -25% | -30% | -32,5% | 0% | 0% | -20% |
14 | -48% | -25% | -30% | -32,5% | 0% | -10% | -20% |
15 | -50% | -30% | -30% | -32,5% | 0% | -10% | -20% |
16 | -50% | -32,5% | -32,5% | -35% | 0% | -10% | -20% |
17 | -50% | -32,5% | -32,5% | -35% | 0% | -10% | -20% |
18 OU + | -50% | -35% | -35% | -35% | 0% | -10% | -20% |