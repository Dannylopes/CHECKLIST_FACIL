# CHECKLIST_FACIL

Documentação funcional e técnica do pipeline de dados do projeto **CHECKLIST_FACIL**, estruturado no padrão **Medallion (Bronze → Silver → Gold)** em notebooks Databricks.

## 1) Visão geral

O projeto realiza ingestão de dados da API do Checklist Fácil, padroniza e trata os dados em Silver e publica tabelas analíticas na camada Gold.

- **Fonte primária**: API `https://api-analytics.checklistfacil.com.br/v1/`.
- **Padrão de ingestão**: paginação com `hasMore` e controle de carga full/incremental.
- **Persistência**: tabelas Spark/Hive via `saveAsTable`.
- **Schemas principais**:
  - Bronze: `cvc_corp_work`
  - Silver: `cvc_corp_silver`
  - Gold: `cvc_corp`

## 2) Arquitetura de dados

### Bronze
Responsável pela coleta bruta da API, mantendo estrutura próxima da origem e adicionando carimbo de carga (`DT_HR_CARGA`).

### Silver
Responsável por limpeza e padronização (tipos, deduplicação, normalização de texto e valores padrão).

### Gold
Responsável por disponibilizar modelo analítico em:
- **Dimensões** (cadastros/atributos de contexto);
- **Fatos** (eventos de aplicação de checklist e seus resultados).

## 3) Estrutura do repositório

```text
CHECKLIST_FACIL/
├── CHECKLIST_FACIL/
│   ├── 0_UTILS/
│   ├── 1_BRONZE/
│   ├── 2_SILVER/
│   └── 3_GOLD/
└── manifest.mf
```

## 4) Utilitários compartilhados (`0_UTILS`)

O notebook `UTILS_CHECKLIST_FACIL.py` centraliza funções reutilizáveis:

- `determina_tipo_carga(...)`: decide entre FULL/INCREMENTAL com base na existência da tabela e watermark;
- `ingestao_api_paginada(...)`: ingestão paginada da API;
- `ingestao_api_simples(...)`: ingestão de endpoint simples;
- `gera_bronze_schema(...)`: geração de schema para Bronze com `DT_HR_CARGA`;
- `executar_carga(...)`: escrita em tabela com `overwrite` (FULL) ou `append`.

## 5) Inventário de notebooks

### 5.1 Bronze (`1_BRONZE`)

| Notebook | Entidade/API |
|---|---|
| `1_MODELO_BRONZE_CHECKLIST_FACIL_CHECKLISTS.py` | `checklists` |
| `BRONZE_CHECKLIST_FACIL_CATEGORIES.py` | `categories` |
| `BRONZE_CHECKLIST_FACIL_EVALUATIONS.py` | `evaluations` |
| `BRONZE_CHECKLIST_FACIL_EVALUATIONS_RESULTS_V1.py` | `evaluations_results` |
| `BRONZE_CHECKLIST_FACIL_ITEMS.py` | `items` |
| `BRONZE_CHECKLIST_FACIL_UNITS.py` | `units` |
| `BRONZE_CHECKLIST_FACIL_USERS.py` | `users` |
| `BRONZE_CHECKLIST_FACIL_USER_TYPES.py` | `user_types` |

> Observação: a pasta `1_BRONZE/OLD` contém versões anteriores e históricos de evolução.

### 5.2 Silver (`2_SILVER`)

| Notebook | Tabela Silver |
|---|---|
| `1_MODELO_SILVER_CHECKLIST_FACIL_CHECKLISTS.py` | `checklistfacil_checklists` |
| `SILVER_CHECKLIST_FACIL_CATEGORIES.py` | `checklistfacil_categories` |
| `SILVER_CHECKLIST_FACIL_EVALUATIONS.py` | `checklistfacil_evaluations` |
| `SILVER_CHECKLIST_FACIL_EVALUATIONS_RESULTS.py` | `checklistfacil_evaluations_results` |
| `SILVER_CHECKLIST_FACIL_ITEMS.py` | `checklistfacil_items` |
| `SILVER_CHECKLIST_FACIL_UNITS.py` | `checklistfacil_units` |
| `SILVER_CHECKLIST_FACIL_USERS.py` | `checklistfacil_users` |
| `SILVER_CHECKLIST_FACIL_USER_TYPES.py` | `checklistfacil_user_types` |

### 5.3 Gold (`3_GOLD`)

| Notebook | Tabela Gold | Tipo |
|---|---|---|
| `GOLD_CHECKLIST_FACIL_DIM_CHECKLISTS.py` | `checklistfacil_dim_checklists` | Dimensão |
| `GOLD_CHECKLIST_FACIL_DIM_CATEGORIAS.py` | `checklistfacil_dim_categorias` | Dimensão |
| `GOLD_CHECKLIST_FACIL_DIM_ITENS.py` | `checklistfacil_dim_itens` | Dimensão |
| `GOLD_CHECKLIST_FACIL_DIM_PLATAFORMA.py` | `checklistfacil_dim_plataforma` | Dimensão |
| `GOLD_CHECKLIST_FACIL_DIM_STATUS.py` | `checklistfacil_dim_status` | Dimensão |
| `GOLD_CHECKLIST_FACIL_DIM_UNIDADES.py` | `checklistfacil_dim_unidades` | Dimensão |
| `GOLD_CHECKLIST_FACIL_DIM_USUARIO.py` | `checklistfacil_dim_usuario` | Dimensão |
| `GOLD_CHECKLIST_FACIL_FATO_CHECKLISTS_APLICADOS.py` | `checklistfacil_fato_checklists_aplicados` | Fato |
| `GOLD_CHECKLIST_FACIL_FATO_CHECKLISTS_APLICADOS_RESULTADOS.py` | `checklistfacil_fato_checklists_aplicados_resultados` | Fato |

## 6) Modelo analítico Gold (resumo)

### Dimensões

- **`dim_status`**: mapeia códigos de status para nomes de negócio (ex.: Não Iniciado, Em Andamento, Concluído).
- **`dim_plataforma`**: mapeia plataforma de origem (Web, Android, IOS).
- **`dim_checklists`**: cadastro de checklists.
- **`dim_unidades`**: cadastro de unidades.
- **`dim_usuario`**: cadastro de usuários e tipo de usuário.
- **`dim_categorias`**: cadastro/hierarquia de categorias de checklist.
- **`dim_itens`**: cadastro de itens por checklist/categoria.

### Fatos

- **`fato_checklists_aplicados`**
  - Grão: 1 linha por checklist aplicado (`evaluationId`).
  - Relaciona status, plataforma, checklist, unidade e usuário.
  - Métricas/atributos: pontuação, datas de execução/aprovação, geolocalização inicial/final etc.

- **`fato_checklists_aplicados_resultados`**
  - Grão: 1 linha por resultado de item respondido (`resultId`).
  - Relaciona checklist aplicado + item + categoria.
  - Métricas/atributos: pesos, comentários, anexos, assinaturas, ordem de item/categoria.

## 7) Diagrama de tabelas da camada Gold

O diagrama ER está disponível em:

- [`docs/diagrama_gold.md`](docs/diagrama_gold.md)

## 8) Ordem recomendada de execução (Databricks)

1. Executar notebooks de `0_UTILS` (referenciados via `%run`).
2. Executar Bronze (ingestão API).
3. Executar Silver (padronização e deduplicação).
4. Executar Gold na seguinte ordem sugerida:
   - `dim_status`, `dim_plataforma`, `dim_checklists`, `dim_unidades`, `dim_usuario`, `dim_categorias`, `dim_itens`
   - `fato_checklists_aplicados`
   - `fato_checklists_aplicados_resultados`

## 9) Observações operacionais

- A maioria dos notebooks Gold executa carga `FULL`.
- As chaves substitutas (`sk_*`) são geradas via `crc32` dos IDs de origem.
- Há campos de auditoria com `DT_HR_CARGA` em todas as camadas.

